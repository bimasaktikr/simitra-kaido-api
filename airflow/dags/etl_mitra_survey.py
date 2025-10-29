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

def notify_laravel_completion(**context):
    """
    Notify Laravel after DAG completion to refresh MySQL cache
    """
    import requests
    import os
    
    print("🔔 Notifying Laravel to refresh cache...")
    
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
            print(f"✅ Laravel notified successfully")
            print(f"   Response: {response.json()}")
        else:
            print(f"⚠️ Laravel notification failed: HTTP {response.status_code}")
            print(f"   Response: {response.text}")
    
    except Exception as e:
        print(f"❌ Failed to notify Laravel: {str(e)}")
        pass

with DAG(
    dag_id="master_mitra_survey",
    start_date=datetime(2025, 10, 14),
    schedule_interval=None,
    catchup=False,
    tags=["ETL", "ML-Training", "Linguistic", "CBF", "PSO", "Dual-Model", "Experience"],
    description="ML Training Pipeline - Generates 2 models + Experience-based recommendations",
) as dag:

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

    notify_laravel_task = PythonOperator(
        task_id="notify_laravel",
        python_callable=notify_laravel_completion,
        provide_context=True,
    )

    preprocess >> feature_engineering >> fuzzy_cbf >> [optimize_weight_rt, optimize_weight_pr] >> merge_pso_task >> aggregate_experience_task >> notify_laravel_task
