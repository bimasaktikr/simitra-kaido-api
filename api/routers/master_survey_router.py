from fastapi import APIRouter, HTTPException
from typing import List, Optional
import logging
from services.database_service import DatabaseService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["Master Surveys"])

@router.get("/master-surveys")
async def get_master_surveys():
    """
    Get all master surveys from PostgreSQL with their types.
    Used for syncing master survey types to Laravel MySQL.
    """
    try:
        db_service = DatabaseService()
        
        query = """
            SELECT id, name, code, type, created_at, updated_at
            FROM master_surveys_enriched
            ORDER BY id
        """
        
        master_surveys = db_service.fetch_from_postgres(query)
        
        if not master_surveys:
            return {
                "success": False,
                "message": "No master surveys found",
                "data": []
            }
        
        logger.info(f"✅ Fetched {len(master_surveys)} master surveys")
        
        return {
            "success": True,
            "message": f"Found {len(master_surveys)} master surveys",
            "data": master_surveys,
            "total": len(master_surveys)
        }
        
    except Exception as e:
        logger.error(f"❌ Error fetching master surveys: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch master surveys: {str(e)}"
        )

@router.get("/master-surveys/{master_survey_id}")
async def get_master_survey_by_id(master_survey_id: int):
    """
    Get a specific master survey by ID
    """
    try:
        db_service = DatabaseService()
        
        query = """
            SELECT id, name, code, type, created_at, updated_at
            FROM master_surveys_enriched
            WHERE id = %s
        """
        
        result = db_service.fetch_from_postgres(query, (master_survey_id,))
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Master survey {master_survey_id} not found"
            )
        
        return {
            "success": True,
            "data": result[0]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error fetching master survey: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch master survey: {str(e)}"
        )
