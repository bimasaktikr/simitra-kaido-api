import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

BASE_DIR = "/opt/airflow"
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

def get_pipeline_functions():
    from pipeline.run_ingest import run_ingest
    from pipeline.run_preprocess import run_preprocess
    from pipeline.run_feature_engineering import run_feature_engineering
    from pipeline.run_ranking_mitra import run_fuzzy_cbf
    from pipeline.run_weight_optimizer import weight_optimizer, merge_pso_results
    from pipeline.run_experience_aggregation import aggregate_experience
    
    return {
        'run_ingest': run_ingest,
        'run_preprocess': run_preprocess,
        'run_feature_engineering': run_feature_engineering,
        'run_fuzzy_cbf': run_fuzzy_cbf,
        'weight_optimizer': weight_optimizer,
        'merge_pso_results': merge_pso_results,
        'aggregate_experience': aggregate_experience
    }

def run_ingest_wrapper(**kwargs):
    funcs = get_pipeline_functions()
    
    import os
    import psycopg2
    from dotenv import load_dotenv, find_dotenv
    
    load_dotenv(find_dotenv(), override=True)
    
    raw_dir = os.path.join(BASE_DIR, "data", "raw")
    sql_files = [f for f in os.listdir(raw_dir) if f.endswith(".sql")] if os.path.exists(raw_dir) else []
    
    if not sql_files:
        print("ℹ️  No SQL backup file found in data/raw - SKIPPING ingest task")
        print("   This is normal if you're using live Laravel PostgreSQL connection")
        print("   Preprocess task will read directly from PostgreSQL tables")
        return {"status": "skipped", "reason": "no_sql_file"}
    
    DB_CONFIG = {
        "dbname": os.getenv("DB_NAME", "mitra_kaido"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASS", "postgres"),
        "host": os.getenv("DB_HOST", "postgres"),
        "port": os.getenv("DB_PORT", "5432"),
    }
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM mitras")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        if count > 0:
            print(f"ℹ️  Database already contains {count} mitras - SKIPPING ingest task")
            print("   Ingest only runs on empty database to avoid duplicate data")
            return {"status": "skipped", "reason": "database_not_empty", "mitra_count": count}
    except Exception as e:
        print(f"⚠️  Could not check database status: {e}")
        print("   Proceeding with ingest...")
    
    print(f"📦 Found SQL backup file: {sql_files[0]}")
    print("   Running data ingestion from SQL dump...")
    return funcs['run_ingest'](base_dir=BASE_DIR)

def run_preprocess_wrapper(**kwargs):
    funcs = get_pipeline_functions()
    return funcs['run_preprocess'](base_dir=BASE_DIR, mode="overwrite")

def run_feature_engineering_wrapper(**kwargs):
    funcs = get_pipeline_functions()
    return funcs['run_feature_engineering'](base_dir=BASE_DIR)

def run_fuzzy_cbf_wrapper(**kwargs):
    funcs = get_pipeline_functions()
    return funcs['run_fuzzy_cbf'](base_dir=BASE_DIR)

def cleanup_old_dag_runs(**context):
    from airflow.models import DagRun
    from airflow.utils.session import create_session
    from airflow.utils.state import DagRunState
    
    dag_id = context['dag'].dag_id
    max_total_runs = 5
    keep_completed_runs = max_total_runs - 1
    
    print(f"🧹 Cleaning up old DAG runs for '{dag_id}'...")
    print(f"   Target: Keep max {max_total_runs} runs (including current)")
    print(f"   Action: Keep last {keep_completed_runs} completed runs")
    
    try:
        with create_session() as session:
            successful_runs = session.query(DagRun).filter(
                DagRun.dag_id == dag_id,
                DagRun.state == DagRunState.SUCCESS
            ).order_by(DagRun.execution_date.desc()).all()
            
            total_runs = len(successful_runs)
            print(f"   📊 Found {total_runs} completed successful runs")
            
            if total_runs <= keep_completed_runs:
                print(f"   ✅ Within limit, no cleanup needed")
                return
            
            runs_to_delete = successful_runs[keep_completed_runs:]
            deleted_count = 0
            
            print(f"   🗑️  Deleting {len(runs_to_delete)} old runs...")
            for run in runs_to_delete:
                print(f"      • {run.run_id} (executed: {run.execution_date})")
                session.delete(run)
                deleted_count += 1
            
            session.commit()
            print(f"   ✅ Cleanup complete: Deleted {deleted_count} runs, kept {keep_completed_runs} recent")
            print(f"   📊 After current run completes: {keep_completed_runs + 1} total runs")
    
    except Exception as e:
        print(f"   ⚠️ Cleanup failed: {str(e)}")
        pass

def optimize_weight_rumah_tangga():
    print("🏠 Starting PSO optimization for Rumah Tangga...")
    funcs = get_pipeline_functions()
    return funcs['weight_optimizer'](BASE_DIR, survey_type="rumah_tangga")

def optimize_weight_perusahaan():
    print("🏢 Starting PSO optimization for Perusahaan...")
    funcs = get_pipeline_functions()
    return funcs['weight_optimizer'](BASE_DIR, survey_type="perusahaan")

def merge_pso_outputs():
    print("🔗 Merging PSO results...")
    funcs = get_pipeline_functions()
    return funcs['merge_pso_results'](BASE_DIR)

def aggregate_experience_recommendations():
    print("📊 Starting experience aggregation...")
    funcs = get_pipeline_functions()
    return funcs['aggregate_experience'](BASE_DIR)

def refresh_mysql_cache(**context):
    import requests
    import os
    
    print("🔄 Triggering MySQL cache refresh via Laravel webhook...")
    
    try:
        laravel_url = os.getenv("LARAVEL_API_URL", "http://host.docker.internal:8000")
        webhook_url = f"{laravel_url}/api/webhooks/ml-training-complete"
        
        dag_run_id = context.get('dag_run').run_id if context.get('dag_run') else 'unknown'
        
        payload = {
            'dag_run_id': dag_run_id,
            'status': 'success',
            'timestamp': datetime.now().isoformat(),
            'message': 'ML training completed successfully'
        }
        
        response = requests.post(webhook_url, json=payload, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ MySQL cache refreshed successfully")
            print(f"   📊 Rumah Tangga: {result.get('rumah_tangga', 0)} records")
            print(f"   📊 Perusahaan: {result.get('perusahaan', 0)} records")
        else:
            print(f"⚠️ Cache refresh failed: HTTP {response.status_code}")
            print(f"   Response: {response.text}")
    
    except Exception as e:
        print(f"❌ Failed to refresh MySQL cache: {str(e)}")
        pass

with DAG(
    dag_id="master_mitra_survey",
    start_date=datetime(2025, 10, 14),
    schedule_interval=None,
    catchup=False,
    tags=["ETL", "ML-Training", "Linguistic", "CBF", "PSO", "Dual-Model", "Experience"],
    description="ML Training Pipeline - Full ETL from SQL dump to ML recommendations. Note: 'ingest_data' task only needed for initial setup from SQL backup file.",
) as dag:

    cleanup_task = PythonOperator(
        task_id="cleanup_old_runs",
        python_callable=cleanup_old_dag_runs,
        provide_context=True,
    )

    ingest = PythonOperator(
        task_id="ingest_data",
        python_callable=run_ingest_wrapper,
    )

    preprocess = PythonOperator(
        task_id="preprocess_data",
        python_callable=run_preprocess_wrapper,
    )

    feature_engineering = PythonOperator(
        task_id="feature_engineering",
        python_callable=run_feature_engineering_wrapper,
    )

    fuzzy_cbf = PythonOperator(
        task_id="fuzzy_cbf_model",
        python_callable=run_fuzzy_cbf_wrapper,
    )

    optimize_weight_rt = PythonOperator(
        task_id="optimize_weight_rumah_tangga",
        python_callable=optimize_weight_rumah_tangga,
    )

    optimize_weight_pr = PythonOperator(
        task_id="optimize_weight_perusahaan",
        python_callable=optimize_weight_perusahaan,
    )

    merge_pso_task = PythonOperator(
        task_id="merge_pso_results",
        python_callable=merge_pso_outputs,
    )

    aggregate_experience_task = PythonOperator(
        task_id="aggregate_experience",
        python_callable=aggregate_experience_recommendations,
    )

    refresh_cache_task = PythonOperator(
        task_id="refresh_cache_mysql",
        python_callable=refresh_mysql_cache,
        provide_context=True,
    )

    cleanup_task >> ingest >> preprocess >> feature_engineering >> fuzzy_cbf >> [optimize_weight_rt, optimize_weight_pr] >> merge_pso_task >> aggregate_experience_task >> refresh_cache_task
