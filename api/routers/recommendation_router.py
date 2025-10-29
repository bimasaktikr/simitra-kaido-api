from fastapi import APIRouter, Query
from typing import Optional
from services.database_service import fetch_table
import psycopg2
import psycopg2.extras
import os

router = APIRouter(prefix="/rekomendasi", tags=["Rekomendasi"])

@router.get("/pso-optimized")
def get_pso_optimized_mitra(
    model_type: Optional[str] = Query(None, description="Filter by model type: 'rumah_tangga' or 'perusahaan'"),
    limit: Optional[int] = Query(None, description="Limit hasil (default: semua)"),
    min_score: Optional[float] = Query(0.0, description="Minimum optimized_score")
):
    """
    Get PSO-optimized mitra recommendations (ML-only, no experience aggregation)
    
    Returns raw ML model output from PSO optimization combining:
    - Fuzzy Logic scoring
    - Content-Based Filtering (CBF) similarity
    
    Args:
        model_type: Filter by 'rumah_tangga' or 'perusahaan' (optional)
        limit: Jumlah maksimal mitra (optional)
        min_score: Minimum optimized_score untuk filter (default: 0.0)
        
    Returns:
        JSON dengan data PSO-optimized mitra
        - mitra_ID: ID mitra
        - mitra_name: Nama mitra
        - model_type: Type of survey (rumah_tangga/perusahaan)
        - fuzzy_score: Fuzzy logic score
        - cbf_avg_similarity: CBF similarity score
        - optimized_score: Final PSO-optimized score (weighted combination)
        - weight_fuzzy: Weight applied to fuzzy score
        - weight_cbf: Weight applied to CBF score
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "postgres"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME", "mitra_kaido"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASS", "postgres")
        )
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        query = """
            SELECT 
                mitra_ID,
                mitra_name,
                mitra_gender,
                mitra_age,
                survey_type,
                model_type,
                fuzzy_score,
                cbf_avg_similarity,
                optimized_score,
                weight_fuzzy,
                weight_cbf,
                created_at
            FROM pso_optimized_mitra
            WHERE optimized_score >= %s
        """
        
        params = [min_score]
        
        if model_type:
            query += " AND model_type = %s"
            params.append(model_type.lower())
        
        query += " ORDER BY optimized_score DESC"
        
        if limit:
            query += " LIMIT %s"
            params.append(limit)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        data = []
        for row in results:
            row_dict = dict(row)
            for key in row_dict:
                if hasattr(row_dict[key], 'real'):  
                    row_dict[key] = float(row_dict[key])
            data.append(row_dict)
        
        stats = {
            "total_count": len(data)
        }
        
        if model_type:
            stats["model_type"] = model_type
        else:
            rt_count = sum(1 for d in data if d['model_type'] == 'rumah_tangga')
            pr_count = sum(1 for d in data if d['model_type'] == 'perusahaan')
            stats["rumah_tangga_count"] = rt_count
            stats["perusahaan_count"] = pr_count
        
        return {
            "status": "success",
            "description": "PSO-optimized ML recommendations (Fuzzy + CBF weighted combination)",
            "statistics": stats,
            "data": data
        }
    
    except Exception as e:
        return {
            "status": "error",
            "detail": str(e)
        }


@router.get("/pso-weights")
def get_pso_weights_info():
    """
    Get PSO optimization weights information
    
    Returns the optimized weights for each model type showing how
    fuzzy scores and CBF similarities are weighted in the final score.
    
    Returns:
        JSON dengan informasi bobot PSO per model type
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "postgres"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME", "mitra_kaido"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASS", "postgres")
        )
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        query = """
            SELECT 
                model_type,
                AVG(weight_fuzzy) as avg_weight_fuzzy,
                AVG(weight_cbf) as avg_weight_cbf,
                COUNT(*) as mitra_count,
                AVG(optimized_score) as avg_optimized_score,
                MAX(optimized_score) as max_optimized_score,
                MIN(optimized_score) as min_optimized_score
            FROM pso_optimized_mitra
            GROUP BY model_type
            ORDER BY model_type
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        data = []
        for row in results:
            row_dict = dict(row)
            for key in row_dict:
                if hasattr(row_dict[key], 'real'):
                    row_dict[key] = float(row_dict[key])
            data.append(row_dict)
        
        return {
            "status": "success",
            "description": "PSO optimization weights per model type",
            "data": data
        }
    
    except Exception as e:
        return {
            "status": "error",
            "detail": str(e)
        }


@router.get("/rumah_tangga")
def rekom_rumah_tangga(
    min_score: Optional[float] = Query(0.0, description="Minimum final_rank_score")
):
    """
    Get ALL recommendations for Rumah Tangga with experience aggregation
    
    Returns ALL mitra sorted by final_rank_score (ML rating + historical performance + experience)
    NO LIMIT - fetches all available recommendations for complete caching
    
    Args:
        min_score: Minimum final_rank_score untuk filter (default: 0.0)
        
    Returns:
        JSON dengan data rekomendasi mitra Rumah Tangga
        - mitra_id: ID mitra
        - mitra_name: Nama mitra
        - survey_type: Survey type
        - survey_score: Avg performance dari historical survey
        - jumlah_survey: Total experience count
        - exp_norm: Normalized experience score
        - weighted_score: Weighted combination score
        - optimized_score: ML rating (dari PSO optimization)
        - final_rank_score: Combined score (50% ML + 50% experience)
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "postgres"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME", "mitra_kaido"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASS", "postgres")
        )
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        query = """
            SELECT 
                mitra_id,
                mitra_name,
                survey_type,
                survey_score,
                jumlah_survey,
                exp_norm,
                weighted_score,
                optimized_score,
                final_rank_score,
                created_at
            FROM recommendation_rumah_tangga
            WHERE final_rank_score >= %s
            ORDER BY final_rank_score DESC
        """
        
        params = [min_score]
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        data = []
        for row in results:
            row_dict = dict(row)
            for key in row_dict:
                if hasattr(row_dict[key], 'real'):
                    row_dict[key] = float(row_dict[key])
            data.append(row_dict)
        
        return {
            "status": "success",
            "survey_type": "Rumah Tangga",
            "count": len(data),
            "description": "ML rating + Historical performance + Experience combined",
            "data": data
        }
    
    except Exception as e:
        return {
            "status": "error",
            "detail": str(e)
        }

@router.get("/perusahaan")
def rekom_perusahaan(
    min_score: Optional[float] = Query(0.0, description="Minimum final_rank_score")
):
    """
    Get ALL recommendations for Perusahaan with experience aggregation
    
    Returns ALL mitra sorted by final_rank_score (ML rating + historical performance + experience)
    NO LIMIT - fetches all available recommendations for complete caching
    
    Args:
        min_score: Minimum final_rank_score untuk filter (default: 0.0)
        
    Returns:
        JSON dengan data rekomendasi mitra Perusahaan
        - mitra_id: ID mitra
        - mitra_name: Nama mitra
        - survey_type: Survey type
        - survey_score: Avg performance dari historical survey
        - jumlah_survey: Total experience count
        - exp_norm: Normalized experience score
        - weighted_score: Weighted combination score
        - optimized_score: ML rating (dari PSO optimization)
        - final_rank_score: Combined score (60% performance + 40% experience)
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "postgres"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME", "mitra_kaido"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASS", "postgres")
        )
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        query = """
            SELECT 
                mitra_id,
                mitra_name,
                survey_type,
                survey_score,
                jumlah_survey,
                exp_norm,
                weighted_score,
                optimized_score,
                final_rank_score,
                created_at
            FROM recommendation_perusahaan
            WHERE final_rank_score >= %s
            ORDER BY final_rank_score DESC
        """
        
        params = [min_score]
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        data = []
        for row in results:
            row_dict = dict(row)
            for key in row_dict:
                if hasattr(row_dict[key], 'real'):
                    row_dict[key] = float(row_dict[key])
            data.append(row_dict)
        
        return {
            "status": "success",
            "survey_type": "Perusahaan",
            "count": len(data),
            "description": "ML rating + Historical performance + Experience combined",
            "data": data
        }
    
    except Exception as e:
        return {
            "status": "error",
            "detail": str(e)
        }
