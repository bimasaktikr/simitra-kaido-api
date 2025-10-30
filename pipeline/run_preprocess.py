import os, re, platform
import pandas as pd
import psycopg2
from dotenv import load_dotenv, find_dotenv

def run_preprocess(base_dir: str, mode: str = "overwrite"):
    """
    Preprocess data by reading DIRECTLY from PostgreSQL (synced from Laravel).
    No longer depends on SQL dump files - uses live data from surveys_cleaned, 
    transactions_cleaned, nilai1s_cleaned tables.
    """
    load_dotenv(find_dotenv(), override=True)

    processed_dir = os.path.join(base_dir, "data", "processed")
    os.makedirs(processed_dir, exist_ok=True)

    processed_dir = os.path.join(base_dir, "data", "processed")
    os.makedirs(processed_dir, exist_ok=True)

    mitra_out_csv = os.path.join(processed_dir, "cleaned_mitras.csv")
    master_out_csv = os.path.join(processed_dir, "cleaned_master_surveys.csv")
    survey_out_csv = os.path.join(processed_dir, "cleaned_surveys.csv")
    trans_out_csv = os.path.join(processed_dir, "cleaned_transactions.csv")
    nilai_out_csv = os.path.join(processed_dir, "cleaned_nilai1s.csv")

    if os.environ.get("AIRFLOW_HOME"):
        detected_host = os.getenv("DB_HOST", "postgres")
    else:
        detected_host = os.getenv("DB_HOST", "127.0.0.1")

    DB_CONFIG = {
        "dbname": os.getenv("DB_NAME", "mitra_kaido"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASS", "postgres"),
        "host": detected_host,
        "port": os.getenv("DB_PORT", "5432"),
    }

    print(f"\n{'='*70}")
    print(f"🔄 READING DATA FROM POSTGRESQL (LIVE DATA SYNCED FROM LARAVEL)")
    print(f"{'='*70}")
    print(f"📍 Host: {detected_host}")
    print(f"📍 Database: {DB_CONFIG['dbname']}\n")

    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        print(f"📥 Reading surveys from PostgreSQL...")
        df_s = pd.read_sql("SELECT * FROM surveys_cleaned WHERE is_scored = 1", conn)
        print(f"   ✅ Found {len(df_s)} scored surveys")
        if len(df_s) > 0:
            latest_ids = sorted(df_s['id'].astype(int).unique())[-5:]
            print(f"   📊 Latest 5 survey IDs: {latest_ids}")
        
        print(f"\n📥 Reading transactions from PostgreSQL...")
        df_t = pd.read_sql("SELECT * FROM transactions_cleaned", conn)
        print(f"   ✅ Found {len(df_t)} transactions")
        
        print(f"\n📥 Reading nilai from PostgreSQL...")
        df_n = pd.read_sql("SELECT * FROM nilai1s_cleaned", conn)
        print(f"   ✅ Found {len(df_n)} nilai records")
        
        print(f"\n📥 Reading master surveys from PostgreSQL...")
        df_ms = pd.read_sql("SELECT * FROM master_surveys_enriched", conn)
        print(f"   ✅ Found {len(df_ms)} master surveys")
        
        print(f"\n📥 Reading mitras data...")
        try:
            df_m = pd.read_sql("SELECT * FROM mitras_enriched", conn)
            print(f"   ✅ Found {len(df_m)} mitras from PostgreSQL")
        except Exception as e:
            print(f"   ⚠️  Table mitras_enriched not found, reading from CSV...")
            raw_dir = os.path.join(base_dir, "data", "raw")
            mitra_csv = os.path.join(raw_dir, "raw_mitras.csv")
            
            if not os.path.exists(mitra_csv):
                raise FileNotFoundError(f"Mitras data not found in PostgreSQL or CSV: {mitra_csv}")
            
            import csv
            rows = []
            with open(mitra_csv, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                for row in reader:
                    if len(row) == len(headers):
                        rows.append(row)
            
            df_m = pd.DataFrame(rows, columns=headers)
            print(f"   ✅ Found {len(df_m)} mitras from CSV")
        
    finally:
        conn.close()
    
    print(f"\n{'='*70}")
    print(f"🧹 CLEANING MITRA NAMES")
    print(f"{'='*70}")
    
    def clean_mitra_name(name):
        """
        Clean mitra name by removing problematic symbols:
        - Backslash (\)
        - Forward slash (/)
        - Single quotes (')
        - Multiple spaces
        """
        if pd.isna(name):
            return name
        name = str(name)
        name = name.replace('\\', '')
        name = name.replace('/', '')
        name = name.replace("'", '')
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name
    
    if 'name' in df_m.columns:
        print(f"\n📊 Sample BEFORE cleaning:")
        sample_before = df_m[df_m['name'].str.contains(r"[\\/'']", na=False, regex=True)].head(3)
        if len(sample_before) > 0:
            for idx, row in sample_before.iterrows():
                print(f"   ❌ ID {row.get('id', 'N/A')}: {row['name']}")
        else:
            print(f"   ✅ No problematic symbols found")
        
        df_m['name'] = df_m['name'].apply(clean_mitra_name)
        
        print(f"\n📊 Sample AFTER cleaning:")
        if len(sample_before) > 0:
            for idx, row in sample_before.iterrows():
                mitra_id = row.get('id', 'N/A')
                cleaned_name = df_m[df_m.get('id', pd.Series()) == row.get('id')]['name'].iloc[0] if 'id' in df_m.columns else 'N/A'
                print(f"   ✅ ID {mitra_id}: {cleaned_name}")
        
        print(f"\n✅ Cleaned {len(df_m)} mitra names")
    
    print(f"\n{'='*70}")
    print(f"💾 SAVING CLEANED DATA TO CSV (FOR DOWNSTREAM TASKS)")
    print(f"{'='*70}")
    
    df_m.to_csv(mitra_out_csv, index=False)
    print(f"   ✅ Mitras: {len(df_m)} rows → {mitra_out_csv}")
    
    df_ms.to_csv(master_out_csv, index=False)
    print(f"   ✅ Master Surveys: {len(df_ms)} rows → {master_out_csv}")
    
    df_s.to_csv(survey_out_csv, index=False)
    print(f"   ✅ Surveys: {len(df_s)} rows → {survey_out_csv}")
    
    df_t.to_csv(trans_out_csv, index=False)
    print(f"   ✅ Transactions: {len(df_t)} rows → {trans_out_csv}")
    
    df_n.to_csv(nilai_out_csv, index=False)
    print(f"   ✅ Nilai: {len(df_n)} rows → {nilai_out_csv}")
    
    print(f"\n{'='*70}")
    print(f"✅ PREPROCESS COMPLETED - DATA READY FOR FEATURE ENGINEERING")
    print(f"{'='*70}\n")

    return {
        "mitra_csv": mitra_out_csv,
        "master_csv": master_out_csv,
        "survey_csv": survey_out_csv,
        "transaction_csv": trans_out_csv,
        "nilai_csv": nilai_out_csv,
    }


if __name__ == "__main__":
    import sys
    base = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
    run_preprocess(base)
