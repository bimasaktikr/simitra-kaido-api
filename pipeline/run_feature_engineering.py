import os
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv, find_dotenv

def run_feature_engineering(base_dir: str):
    load_dotenv(find_dotenv(), override=True)

    print(f"\n{'='*70}")
    print(f"🔧 FEATURE ENGINEERING - CREATING ML FEATURES")
    print(f"{'='*70}\n")

    processed_dir = os.path.join(base_dir, "data", "processed")
    os.makedirs(processed_dir, exist_ok=True)
    output_csv = os.path.join(processed_dir, "features_mitra_survey.csv")

    print(f"📥 Reading cleaned data from PostgreSQL with CSV fallback...")
    
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "simitra_postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "mitra123"),
        database=os.getenv("POSTGRES_DB", "mitra_kaido")
    )
    
    try:
        mitras = pd.read_sql("SELECT * FROM mitras_cleaned", conn)
        if len(mitras) == 0:
            raise ValueError("mitras_cleaned is empty")
        print(f"   ✅ Mitras (PostgreSQL): {len(mitras)} rows")
    except Exception:
        mitras = pd.read_csv(os.path.join(processed_dir, "cleaned_mitras.csv"))
        print(f"   ✅ Mitras (CSV fallback): {len(mitras)} rows")
    
    try:
        master_surveys = pd.read_sql("SELECT * FROM master_surveys_enriched", conn)
        if len(master_surveys) == 0:
            raise ValueError("master_surveys_enriched is empty")
        print(f"   ✅ Master Surveys (PostgreSQL): {len(master_surveys)} rows")
    except Exception:
        master_surveys = pd.read_csv(os.path.join(processed_dir, "cleaned_master_surveys.csv"))
        print(f"   ✅ Master Surveys (CSV fallback): {len(master_surveys)} rows")
    
    try:
        surveys = pd.read_sql("SELECT * FROM surveys_cleaned", conn)
        if len(surveys) == 0:
            raise ValueError("surveys_cleaned is empty")
        print(f"   ✅ Surveys (PostgreSQL): {len(surveys)} rows")
    except Exception:
        surveys = pd.read_csv(os.path.join(processed_dir, "cleaned_surveys.csv"))
        print(f"   ✅ Surveys (CSV fallback): {len(surveys)} rows")
    
    try:
        transactions = pd.read_sql("SELECT * FROM transactions_cleaned", conn)
        if len(transactions) == 0:
            raise ValueError("transactions_cleaned is empty")
        print(f"   ✅ Transactions (PostgreSQL): {len(transactions)} rows")
    except Exception:
        transactions = pd.read_csv(os.path.join(processed_dir, "cleaned_transactions.csv"))
        print(f"   ✅ Transactions (CSV fallback): {len(transactions)} rows")
    
    conn.close()
    print(f"   PostgreSQL connection closed")

    print(f"\n🔍 Data loaded: mitras={len(mitras)}, master_surveys={len(master_surveys)}, surveys={len(surveys)}, transactions={len(transactions)}")

    for df in [mitras, master_surveys, surveys, transactions]:
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    print(f"\n🔗 Merging dataframes...")
    df = transactions.merge(
        mitras[["id", "name", "jenis_kelamin", "tanggal_lahir"]],
        left_on="mitra_id", right_on="id", how="left", suffixes=("", "_mitra")
    )
    print(f"   ✅ After merging with mitras: {len(df)} rows")

    def calc_age(date_str):
        try:
            if len(str(date_str)) < 4:
                return None
            year = int(str(date_str).split("-")[0])
            return datetime.now().year - year
        except:
            return None

    df["mitra_age"] = df["tanggal_lahir"].apply(calc_age)

    df = df.rename(columns={
        "mitra_id": "mitra_ID",
        "name": "mitra_name",
        "jenis_kelamin": "mitra_gender",
        "survey_id": "survey_ID"
    })

    df = df.merge(
        surveys[["id", "master_survey_id"]],
        left_on="survey_ID", right_on="id", how="left"
    )
    print(f"   ✅ After merging with surveys: {len(df)} rows")

    df = df.merge(
        master_surveys[["id", "type"]],
        left_on="master_survey_id", right_on="id", how="left", suffixes=("", "_master")
    )
    print(f"   ✅ After merging with master_surveys: {len(df)} rows")

    df = df.rename(columns={"type": "survey_type"})

    print(f"\n🧮 Calculating features...")
    df_final = df[[
        "mitra_ID", "mitra_name", "mitra_gender", "mitra_age", "survey_ID", "survey_type"
    ]].copy()
    print(f"   ✅ Initial feature dataset: {len(df_final)} rows")

    df_final = df_final[~df_final["mitra_ID"].isin(["", "nan", "None"])]
    print(f"   ✅ After removing invalid mitra_ID: {len(df_final)} rows")
    
    df_final = df_final[~df_final["survey_ID"].isin(["", "nan", "None"])]
    print(f"   ✅ After removing invalid survey_ID: {len(df_final)} rows")

    df_final["mitra_gender"] = df_final["mitra_gender"].replace(["", "nan", "None"], "Unknown")
    df_final["mitra_age"] = df_final["mitra_age"].fillna(0).astype(int)

    df_final["survey_type"] = df_final["survey_type"].replace(["", "nan", "None"], "Unknown")

    df_final = df_final.drop_duplicates().reset_index(drop=True)
    print(f"   ✅ After removing duplicates: {len(df_final)} rows")

    print(f"\n✅ Feature dataset created: {len(df_final)} rows")
    if len(df_final) > 0:
        print("📊 Sample data (first 5 rows):")
        print(df_final.head().to_string(index=False))
    else:
        print("   ⚠️  WARNING: Feature dataset is empty!")

    print(f"\n💾 Saving to CSV: {output_csv}")
    df_final.to_csv(output_csv, index=False)
    print(f"   ✅ Saved successfully")

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

    print(f"\n📤 Uploading to PostgreSQL...")
    print(f"   📍 Host: {detected_host}")
    print(f"   📍 Database: {DB_CONFIG['dbname']}")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print(f"   🗑️  Dropping existing table (if exists)...")
        cur.execute("DROP TABLE IF EXISTS features_mitra_survey CASCADE;")
        
        print(f"   📋 Creating table features_mitra_survey...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS features_mitra_survey (
                id SERIAL PRIMARY KEY,
                mitra_ID VARCHAR(100),
                mitra_name VARCHAR(255),
                mitra_gender VARCHAR(10),
                mitra_age INT,
                survey_ID VARCHAR(100),
                survey_type VARCHAR(100)
            );
        """)

        print(f"   📥 Inserting {len(df_final)} rows...")
        for idx, r in df_final.iterrows():
            cur.execute("""
                INSERT INTO features_mitra_survey (
                    mitra_ID, mitra_name, mitra_gender, mitra_age, survey_ID, survey_type
                ) VALUES (%s, %s, %s, %s, %s, %s);
            """, (
                r["mitra_ID"], r["mitra_name"], r["mitra_gender"],
                int(r["mitra_age"]), r["survey_ID"], r["survey_type"]
            ))

        conn.commit()
        print(f"   ✅ Data committed successfully")
        
        cur.close()
        conn.close()
        print(f"   ✅ Database connection closed")

        print(f"\n{'='*70}")
        print(f"✅ FEATURE ENGINEERING COMPLETED")
        print(f"{'='*70}\n")

    except Exception as e:
        print(f"\n❌ ERROR during database upload: {e}")
        raise

    return {
        "feature_csv": output_csv,
        "table": "features_mitra_survey",
        "rows": len(df_final)
    }
