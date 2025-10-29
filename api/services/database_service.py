import psycopg2
import mysql.connector
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
from typing import Optional, Dict, List, Any

load_dotenv()

POSTGRES_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "host": os.getenv("DB_HOST", "postgres"),
    "port": os.getenv("DB_PORT", "5432")
}

MYSQL_CONFIG = {
    "host": os.getenv("LARAVEL_DB_HOST", "host.docker.internal"),
    "port": int(os.getenv("LARAVEL_DB_PORT", 3306)),
    "database": os.getenv("LARAVEL_DB_NAME", "kaido_kit"),
    "user": os.getenv("LARAVEL_DB_USER", "root"),
    "password": os.getenv("LARAVEL_DB_PASS", "")
}

DB_CONFIG = POSTGRES_CONFIG

class DatabaseService:
    """Service for handling database connections and operations"""
    
    def __init__(self):
        self.postgres_config = POSTGRES_CONFIG
        self.mysql_config = MYSQL_CONFIG
    
    def get_postgres_connection(self):
        """Get PostgreSQL connection"""
        return psycopg2.connect(**self.postgres_config)
    
    def get_mysql_connection(self):
        """Get MySQL connection"""
        return mysql.connector.connect(**self.mysql_config)
    
    def fetch_from_postgres(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Execute SELECT query on PostgreSQL"""
        conn = self.get_postgres_connection()
        try:
            df = pd.read_sql(query, conn, params=params)
            df = df.replace([np.nan, np.inf, -np.inf], None)
            return df.to_dict(orient="records")
        finally:
            conn.close()
    
    def fetch_from_mysql(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Execute SELECT query on MySQL"""
        conn = self.get_mysql_connection()
        try:
            df = pd.read_sql(query, conn, params=params)
            df = df.replace([np.nan, np.inf, -np.inf], None)
            return df.to_dict(orient="records")
        finally:
            conn.close()
    
    def execute_postgres(self, query: str, params: tuple = None) -> int:
        """Execute INSERT/UPDATE/DELETE on PostgreSQL"""
        conn = self.get_postgres_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount
        finally:
            cursor.close()
            conn.close()
    
    def upsert_to_postgres(self, table: str, data: List[Dict], conflict_columns: List[str]) -> int:
        """Bulk upsert data to PostgreSQL table"""
        if not data:
            return 0
        
        conn = self.get_postgres_connection()
        cursor = conn.cursor()
        
        try:
            columns = list(data[0].keys())
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join(columns)
            
            conflict_str = ', '.join(conflict_columns)
            update_columns = [col for col in columns if col not in conflict_columns]
            update_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
            
            query = f"""
                INSERT INTO {table} ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_str})
                DO UPDATE SET {update_str}
            """
            
            rows_affected = 0
            for record in data:
                values = tuple(record[col] for col in columns)
                cursor.execute(query, values)
                rows_affected += cursor.rowcount
            
            conn.commit()
            return rows_affected
            
        finally:
            cursor.close()
            conn.close()

def fetch_table(table_name: str, order_by: str = None):
    """Ambil data dari PostgreSQL dan pastikan aman dari NaN/inf"""
    conn = psycopg2.connect(**DB_CONFIG)
    query = f"SELECT * FROM {table_name}"
    if order_by:
        query += f" ORDER BY {order_by} DESC"

    df = pd.read_sql(query, conn)
    conn.close()

    df = df.replace([np.nan, np.inf, -np.inf], None)

    return df.to_dict(orient="records")
