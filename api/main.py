from fastapi import FastAPI
from routers import mitra_router, recommendation_router, sync_router
import logging

# Configure logging
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

app.include_router(mitra_router.router)
app.include_router(recommendation_router.router)
app.include_router(sync_router.router)

