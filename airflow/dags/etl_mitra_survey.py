import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

BASE_DIR = "/opt/airflow/project"
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from pipeline.run_ingest import run_ingest
from pipeline.run_preprocess import run_preprocess
from pipeline.run_feature_engineering import run_feature_engineering
from pipeline.run_ranking_mitra import run_fuzzy_cbf
from pipeline.run_weight_optimizer import weight_optimizer, merge_pso_results
from pipeline.run_experience_aggregation import aggregate_experience

def cleanup_old_dag_runs(**context):
    """
    Auto-cleanup old DAG runs to prevent log accumulation.
    Keeps only the last 4 completed successful runs (+ current run = 5 total).
    Runs at the start of each DAG execution.
    """
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
    """
    Run PSO optimization for Rumah Tangga survey type only
    """
    print("🏠 Starting PSO optimization for Rumah Tangga...")
    return weight_optimizer(BASE_DIR, survey_type="rumah_tangga")

def optimize_weight_perusahaan():
    """
    Run PSO optimization for Perusahaan survey type only
    """
    print("🏢 Starting PSO optimization for Perusahaan...")
    return weight_optimizer(BASE_DIR, survey_type="perusahaan")

def merge_pso_outputs():
    """
    Merge individual PSO result files into combined file and upload to PostgreSQL
    """
    print("🔗 Merging PSO results...")
    return merge_pso_results(BASE_DIR)

def aggregate_experience_recommendations():
    """
    Aggregate ML ratings with historical performance & experience
    Generate final recommendations with combined scoring
    """
    print("📊 Starting experience aggregation...")
    return aggregate_experience(BASE_DIR)

def refresh_mysql_cache(**context):
    """
    Trigger Laravel to refresh MySQL cache after training completion.
    
    This task sends webhook to Laravel API which will:
    1. Fetch fresh recommendations from ML API (PostgreSQL)
    2. Clear old cache in MySQL
    3. Store new recommendations in MySQL cache tables:
       - ml_cache_rumah_tangga
       - ml_cache_perusahaan
    """
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
    description="ML Training Pipeline - Generates 2 models + Experience-based recommendations",
) as dag:

    cleanup_task = PythonOperator(
        task_id="cleanup_old_runs",
        python_callable=cleanup_old_dag_runs,
        provide_context=True,
    )

    preprocess = PythonOperator(
        task_id="preprocess_data",
        python_callable=run_preprocess,
        op_kwargs={"base_dir": BASE_DIR, "mode": "append"},
    )

    feature_engineering = PythonOperator(
        task_id="feature_engineering",
        python_callable=run_feature_engineering,
        op_kwargs={"base_dir": BASE_DIR},
    )

    fuzzy_cbf = PythonOperator(
        task_id="fuzzy_cbf_model",
        python_callable=run_fuzzy_cbf,
        op_kwargs={"base_dir": BASE_DIR},
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

    cleanup_task >> preprocess >> feature_engineering >> fuzzy_cbf >> [optimize_weight_rt, optimize_weight_pr] >> merge_pso_task >> aggregate_experience_task >> refresh_cache_task
