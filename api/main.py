from fastapi import FastAPI
from routers import mitra_router, recommendation_router, sync_router, webhook_router, master_survey_router
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:     %(message)s'
)

app = FastAPI(
    title="Mitra Recommendation API",
    description="API untuk menampilkan hasil rekomendasi mitra, fitur, dan data PSO",
    version="1.0",
)

@app.get("/")
def root():
    return {"message": "🚀 API Mitra Recommendation ready in use!, please go to /docs for API documentation."}

@app.get("/health")
def health_check():
    """
    Health check endpoint for monitoring and container orchestration.
    Returns service status and timestamp.
    """
    return {
        "status": "healthy",
        "service": "Mitra Recommendation API",
        "version": "1.0",
        "timestamp": datetime.utcnow().isoformat(),
        "uptime": "running"
    }

app.include_router(mitra_router.router)
app.include_router(recommendation_router.router)
app.include_router(sync_router.router)
app.include_router(webhook_router.router)
app.include_router(master_survey_router.router)

