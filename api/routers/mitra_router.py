from fastapi import APIRouter
from services.database_service import fetch_table

router = APIRouter(prefix="/mitra", tags=["Mitra"])

@router.get("/")
def get_mitra_data():
    data = fetch_table("mitras_cleaned", "id")
    if isinstance(data, dict) and "error" in data:
        return {"status": "error", "detail": data["error"]}
    return {"count": len(data), "data": data}

@router.get("/features_engineering")
def get_feature_data():
    data = fetch_table("features_mitra_survey", "mitra_ID")
    if isinstance(data, dict) and "error" in data:
        return {"status": "error", "detail": data["error"]}
    return {"count": len(data), "data": data}

@router.get("/optimized_mitra")
def get_pso_data():
    data = fetch_table("pso_optimized_mitra", "optimized_score")
    if isinstance(data, dict) and "error" in data:
        return {"status": "error", "detail": data["error"]}
    return {"count": len(data), "data": data}
