from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
import requests
import logging
import os

router = APIRouter(prefix="/webhooks", tags=["Webhooks"])

logger = logging.getLogger(__name__)

class TrainingCompletePayload(BaseModel):
    """Payload dari Airflow setelah training selesai"""
    dag_run_id: str
    status: str
    survey_type: Optional[str] = None
    records_processed: Optional[int] = None
    timestamp: str

@router.post("/training-complete")
async def handle_training_complete(
    payload: TrainingCompletePayload,
    background_tasks: BackgroundTasks
):
    """
    Webhook endpoint yang dipanggil oleh Airflow setelah training selesai.
    
    Flow:
    1. Airflow selesai training → simpan ke PostgreSQL
    2. Airflow POST ke endpoint ini
    3. Endpoint ini trigger Laravel untuk refresh MySQL cache
    
    Args:
        payload: Data dari Airflow (dag_run_id, status, survey_type, dll)
        
    Returns:
        Success response
    """
    try:
        logger.info(f"🔔 Received training complete webhook from Airflow")
        logger.info(f"   DAG Run ID: {payload.dag_run_id}")
        logger.info(f"   Status: {payload.status}")
        logger.info(f"   Survey Type: {payload.survey_type}")
        logger.info(f"   Records: {payload.records_processed}")
        
        if payload.status != "success":
            logger.warning(f"⚠️  Training status is not success: {payload.status}")
            return {
                "status": "acknowledged",
                "message": "Training not successful, skipping cache refresh"
            }
        
        background_tasks.add_task(
            notify_laravel_to_refresh_cache,
            payload.survey_type
        )
        
        return {
            "status": "success",
            "message": "Webhook received, triggering Laravel cache refresh",
            "dag_run_id": payload.dag_run_id
        }
        
    except Exception as e:
        logger.error(f"❌ Error processing webhook: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


async def notify_laravel_to_refresh_cache(survey_type: Optional[str] = None):
    """
    Notify Laravel untuk refresh MySQL cache setelah training selesai.
    
    Args:
        survey_type: Tipe survey yang di-training (None = refresh semua)
    """
    try:
        laravel_urls = [
            os.getenv("LARAVEL_WEBHOOK_URL", "http://localhost:8000/api/webhooks/training-complete"),
            "http://host.docker.internal:8000/api/webhooks/training-complete",
            "http://172.17.0.1:8000/api/webhooks/training-complete",  
            "http://192.168.65.1:8000/api/webhooks/training-complete",  
        ]
        
        for url in laravel_urls:
            try:
                logger.info(f"📤 Trying to notify Laravel...")
                logger.info(f"   URL: {url}")
                logger.info(f"   Survey Type: {survey_type or 'all'}")
                
                response = requests.post(
                    url,
                    json={
                        "survey_type": survey_type,
                        "source": "airflow_training"
                    },
                    timeout=5  
                )
                
                if response.status_code == 200:
                    result = response.json()
                    logger.info(f"✅ Laravel cache refresh triggered successfully")
                    logger.info(f"   Response: {result}")
                    return  
                else:
                    logger.warning(f"⚠️ Laravel returned error: {response.status_code}")
                    continue  
                    
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                logger.debug(f"   ⏭️ Failed, trying next URL...")
                continue
        
        logger.warning(f"⚠️ Could not reach Laravel on any URL")
        logger.warning(f"   This is OK if Laravel is not running")
        logger.warning(f"   Laravel will fetch from PostgreSQL on next request")
            
    except Exception as e:
        logger.error(f"❌ Error notifying Laravel: {str(e)}")


@router.get("/health")
def webhook_health():
    """Health check untuk webhook endpoint"""
    return {
        "status": "healthy",
        "service": "Webhook Service",
        "endpoints": ["/training-complete"]
    }


@router.post("/airflow-notify")
async def airflow_notify_simple(background_tasks: BackgroundTasks):
    """
    Simple endpoint untuk Airflow call setelah training selesai.
    Tidak perlu payload kompleks - cukup POST ke endpoint ini.
    
    Gunakan di Airflow DAG dengan:
    ```python
    from airflow.providers.http.operators.http import SimpleHttpOperator
    
    notify_task = SimpleHttpOperator(
        task_id='notify_mysql_sync',
        http_conn_id='fastapi_default',
        endpoint='/webhooks/airflow-notify',
        method='POST',
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
        dag=dag
    )
    
    # Letakkan setelah save to PostgreSQL
    save_to_postgres >> notify_task
    ```
    
    Returns:
        Success response
    """
    try:
        logger.info("🔔 Received simple notification from Airflow")
        logger.info("   Triggering Laravel MySQL cache refresh...")
        
        # Trigger Laravel untuk refresh cache (all survey types)
        background_tasks.add_task(
            notify_laravel_to_refresh_cache,
            None  # None = refresh both Rumah Tangga & Perusahaan
        )
        
        return {
            "status": "success",
            "message": "MySQL cache refresh triggered successfully"
        }
        
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
