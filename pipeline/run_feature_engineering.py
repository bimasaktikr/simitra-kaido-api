import os
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv, find_dotenv

def run_feature_engineering(base_dir: str):
    load_dotenv(find_dotenv(), override=True)

    processed_dir = os.path.join(base_dir, "data", "processed")
    
    # Create processed directory if it doesn't exist
    os.makedirs(processed_dir, exist_ok=True)
    
    output_csv = os.path.join(processed_dir, "features_mitra_survey.csv")

    mitras = pd.read_csv(os.path.join(processed_dir, "cleaned_mitras.csv"))
    master_surveys = pd.read_csv(os.path.join(processed_dir, "cleaned_master_surveys.csv"))
    surveys = pd.read_csv(os.path.join(processed_dir, "cleaned_surveys.csv"))
    transactions = pd.read_csv(os.path.join(processed_dir, "cleaned_transactions.csv"))

    print(f"🔍 mitras: {len(mitras)}, master_surveys: {len(master_surveys)}, surveys: {len(surveys)}, transactions: {len(transactions)}")

    for df in [mitras, master_surveys, surveys, transactions]:
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    df = transactions.merge(
        mitras[["id", "name", "jenis_kelamin", "tanggal_lahir"]],
        left_on="mitra_id", right_on="id", how="left", suffixes=("", "_mitra")
    )

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

    df = df.merge(
        master_surveys[["id", "type"]],
        left_on="master_survey_id", right_on="id", how="left", suffixes=("", "_master")
    )

    df = df.rename(columns={"type": "survey_type"})

    df_final = df[[
        "mitra_ID", "mitra_name", "mitra_gender", "mitra_age", "survey_ID", "survey_type"
    ]].copy()

    df_final = df_final[~df_final["mitra_ID"].isin(["", "nan", "None"])]
    df_final = df_final[~df_final["survey_ID"].isin(["", "nan", "None"])]

    df_final["mitra_gender"] = df_final["mitra_gender"].replace(["", "nan", "None"], "Unknown")
    df_final["mitra_age"] = df_final["mitra_age"].fillna(0).astype(int)

    df_final["survey_type"] = df_final["survey_type"].replace(["", "nan", "None"], "Unknown")

    df_final = df_final.drop_duplicates().reset_index(drop=True)

    print(f"✅ Feature dataset created: {len(df_final)} rows")
    if len(df_final) > 0:
        print("📊 Contoh data:\n", df_final.head().to_string(index=False))

    df_final.to_csv(output_csv, index=False)

    DB_CONFIG = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "host": os.getenv("DB_HOST", "postgres"),
        "port": os.getenv("DB_PORT", "5432"),
    }

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS features_mitra_survey CASCADE;")
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

    for _, r in df_final.iterrows():
        cur.execute("""
            INSERT INTO features_mitra_survey (
                mitra_ID, mitra_name, mitra_gender, mitra_age, survey_ID, survey_type
            ) VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            r["mitra_ID"], r["mitra_name"], r["mitra_gender"],
            int(r["mitra_age"]), r["survey_ID"], r["survey_type"]
        ))

    conn.commit()
    cur.close()
    conn.close()

    print("📤 Uploaded feature dataset to PostgreSQL → table: features_mitra_survey")

    return {
        "feature_csv": output_csv,
        "table": "features_mitra_survey",
        "rows": len(df_final)
    }
