from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import logging
from datetime import datetime

from services.database_service import DatabaseService

router = APIRouter(prefix="/sync", tags=["Data Synchronization"])
logger = logging.getLogger(__name__)

class SurveySync(BaseModel):
    survey_id: int
    source: str = "laravel_finalization"

class SyncResponse(BaseModel):
    success: bool
    message: str
    synced_at: Optional[str] = None
    records_synced: Optional[int] = None

@router.post("/survey", response_model=SyncResponse)
async def sync_survey_data(sync_request: SurveySync):
    """
    Sync finalized survey data from Laravel MySQL to PostgreSQL
    This triggers ML retraining with new survey data
    """
    try:
        logger.info(f"📥 Syncing survey {sync_request.survey_id} from {sync_request.source}")
        
        db_service = DatabaseService()
        
        # Fetch survey with related data from MySQL
        survey_query = """
            SELECT 
                s.id, s.master_survey_id, s.triwulan, s.year, 
                s.payment_month, s.payment_id, s.team_id, s.rate, s.status,
                s.is_scored, s.is_synced, s.file,
                s.created_at, s.updated_at,
                ms.name as survey_name, ms.type as survey_type
            FROM surveys s
            LEFT JOIN master_surveys ms ON s.master_survey_id = ms.id
            WHERE s.id = %s
        """
        
        surveys = db_service.fetch_from_mysql(survey_query, (sync_request.survey_id,))
        
        if not surveys:
            raise HTTPException(
                status_code=404,
                detail=f"Survey {sync_request.survey_id} not found"
            )
        
        survey = surveys[0]
        
        # Fetch transactions for this survey
        transactions_query = """
            SELECT 
                t.id, t.mitra_id, t.survey_id, t.target, t.rate,
                t.created_at, t.updated_at,
                m.name as mitra_name, m.email as mitra_email
            FROM transactions t
            LEFT JOIN mitras m ON t.mitra_id = m.id
            WHERE t.survey_id = %s
        """
        
        transactions = db_service.fetch_from_mysql(transactions_query, (sync_request.survey_id,))
        
        # Fetch nilai1s (ratings) for these transactions
        transaction_ids = [t['id'] for t in transactions]
        nilai1s = []
        
        if transaction_ids:
            placeholders = ','.join(['%s'] * len(transaction_ids))
            nilai1s_query = f"""
                SELECT 
                    transaction_id, aspek1, aspek2, aspek3, rerata,
                    created_at, updated_at
                FROM nilai1s
                WHERE transaction_id IN ({placeholders})
            """
            
            nilai1s = db_service.fetch_from_mysql(nilai1s_query, tuple(transaction_ids))
        
        # Prepare data for PostgreSQL upsert
        records_synced = 0
        
        # Upsert survey to PostgreSQL
        survey_data = {
            'id': survey['id'],
            'master_survey_id': survey['master_survey_id'],
            'triwulan': survey['triwulan'],
            'year': survey['year'],
            'payment_month': survey.get('payment_month'),
            'payment_id': survey.get('payment_id'),
            'team_id': survey.get('team_id'),
            'rate': survey.get('rate'),
            'status': survey.get('status'),
            'is_scored': survey.get('is_scored', 0),
            'is_synced': survey.get('is_synced', 0),
            'file': survey.get('file'),
            'created_at': survey.get('created_at'),
            'updated_at': survey.get('updated_at')
        }
        
        # Execute upsert for survey
        pg_conn = db_service.get_postgres_connection()
        pg_conn.autocommit = False  # Explicit transaction mode
        pg_cursor = pg_conn.cursor()
        
        try:
            logger.info(f"🔄 About to sync survey {survey_data['id']}")
            logger.info(f"   - Transactions to sync: {len(transactions)}")
            logger.info(f"   - Nilai to sync: {len(nilai1s)}")
            
            # Upsert survey to PostgreSQL - simple version without ON CONFLICT for debugging
            # First try to update
            pg_cursor.execute("""
                UPDATE surveys_cleaned
                SET status = %s, rate = %s, is_scored = %s, is_synced = %s, updated_at = %s
                WHERE id = %s
            """, (
                survey_data['status'], survey_data['rate'], survey_data['is_scored'],
                survey_data['is_synced'], survey_data['updated_at'], survey_data['id']
            ))
            
            logger.info(f"   - Survey UPDATE affected {pg_cursor.rowcount} rows")
            
            # If no rows updated, insert
            if pg_cursor.rowcount == 0:
                logger.info(f"   - Inserting survey {survey_data['id']} (new record)")
                pg_cursor.execute("""
                    INSERT INTO surveys_cleaned (
                        id, master_survey_id, triwulan, year, payment_month, payment_id, team_id,
                        rate, file, is_scored, is_synced, status, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    survey_data['id'], survey_data['master_survey_id'], survey_data['triwulan'],
                    survey_data['year'], survey_data['payment_month'], survey_data['payment_id'], 
                    survey_data['team_id'], survey_data['rate'], survey_data.get('file'),
                    survey_data['is_scored'], survey_data['is_synced'], survey_data['status'], 
                    survey_data['created_at'], survey_data['updated_at']
                ))
                logger.info(f"   - Survey INSERT completed")
            records_synced += 1
            
            # Upsert transactions
            for trans in transactions:
                # Try update first
                pg_cursor.execute("""
                    UPDATE transactions_cleaned
                    SET target = %s, rate = %s, updated_at = %s
                    WHERE id = %s
                """, (
                    trans.get('target'), trans.get('rate'), 
                    trans.get('updated_at'), trans['id']
                ))
                
                # If no rows updated, insert
                if pg_cursor.rowcount == 0:
                    pg_cursor.execute("""
                        INSERT INTO transactions_cleaned (
                            id, mitra_id, survey_id, target, rate, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        trans['id'], trans['mitra_id'], trans['survey_id'],
                        trans.get('target'), trans.get('rate'), 
                        trans.get('created_at'), trans.get('updated_at')
                    ))
                records_synced += 1
            
            # Upsert nilai1s
            for nilai in nilai1s:
                # Try update first
                pg_cursor.execute("""
                    UPDATE nilai1s_cleaned
                    SET aspek1 = %s, aspek2 = %s, aspek3 = %s, rerata = %s, updated_at = %s
                    WHERE transaction_id = %s
                """, (
                    nilai.get('aspek1'), nilai.get('aspek2'), nilai.get('aspek3'),
                    nilai.get('rerata'), nilai.get('updated_at'), nilai['transaction_id']
                ))
                
                # If no rows updated, insert
                if pg_cursor.rowcount == 0:
                    pg_cursor.execute("""
                        INSERT INTO nilai1s_cleaned (
                            transaction_id, aspek1, aspek2, aspek3, rerata, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        nilai['transaction_id'], nilai.get('aspek1'), nilai.get('aspek2'),
                        nilai.get('aspek3'), nilai.get('rerata'), 
                        nilai.get('created_at'), nilai.get('updated_at')
                    ))
                records_synced += 1
            
            logger.info(f"🔄 About to COMMIT transaction...")
            pg_conn.commit()
            logger.info(f"✅ pg_conn.commit() executed successfully!")
            logger.info(f"   📊 Synced {records_synced} records total")
            logger.info(f"   - Survey: {survey_data['id']}")
            logger.info(f"   - Transactions: {len(transactions)}")
            logger.info(f"   - Nilai: {len(nilai1s)}")
            
        except Exception as sync_ex:
            logger.error(f"❌ Error during sync operation: {sync_ex}")
            pg_conn.rollback()
            logger.info(f"🔄 ROLLBACK executed")
            raise
        finally:
            logger.info(f"🔒 Closing cursor and connection...")
            pg_cursor.close()
            pg_conn.close()
            logger.info(f"✅ Cursor and connection closed successfully")
        
        # Verify data with NEW connection after commit
        try:
            verify_conn = db_service.get_postgres_connection()
            verify_cursor = verify_conn.cursor()
            
            verify_cursor.execute("SELECT COUNT(*) FROM surveys_cleaned WHERE id = %s", (sync_request.survey_id,))
            survey_check = verify_cursor.fetchone()[0]
            verify_cursor.execute("SELECT COUNT(*) FROM transactions_cleaned WHERE survey_id = %s", (sync_request.survey_id,))
            trans_check = verify_cursor.fetchone()[0]
            
            logger.info(f"   🔍 Post-commit verification (new connection):")
            logger.info(f"      - Survey {sync_request.survey_id}: {survey_check} record(s)")
            logger.info(f"      - Transactions: {trans_check} record(s)")
            
            verify_cursor.close()
            verify_conn.close()
        except Exception as ve:
            logger.warning(f"   ⚠️  Verification failed: {ve}")
            
        return SyncResponse(
            success=True,
            message=f"Successfully synced survey {sync_request.survey_id}",
            synced_at=datetime.now().isoformat(),
            records_synced=records_synced
        )
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"❌ Sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Sync failed: {str(e)}")
