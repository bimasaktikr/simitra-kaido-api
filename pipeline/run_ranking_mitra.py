import os
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime
from sklearn.metrics.pairwise import cosine_similarity
from dotenv import load_dotenv, find_dotenv

def fuzzy_membership_age(age):
    if age <= 25:
        return {"young": 1.0, "mid": 0.0, "senior": 0.0}
    elif 25 < age <= 35:
        return {"young": (35 - age) / 10, "mid": (age - 25) / 10, "senior": 0.0}
    elif 35 < age <= 50:
        return {"young": 0.0, "mid": (50 - age) / 15, "senior": (age - 35) / 15}
    else:
        return {"young": 0.0, "mid": 0.0, "senior": 1.0}

def fuzzy_similarity(row, survey_type):
    gender = str(row["mitra_gender"]).lower()
    age = float(row["mitra_age"])

    if survey_type.lower() == "rumah tangga":
        gender_score = 1.0 if gender in ["perempuan", "female", "p"] else 0.7
    elif survey_type.lower() == "perusahaan":
        gender_score = 1.0 if gender in ["laki-laki", "male", "l"] else 0.6
    else:
        gender_score = 0.8

    if 21 <= age <= 30:
        age_score = 0.6
    elif 31 <= age <= 40:
        age_score = 0.9
    elif 41 <= age <= 50:
        age_score = 1.0
    elif age >= 51:
        age_score = 0.8
    else:
        age_score = 0.5

    if survey_type.lower() == "rumah tangga":
        score = 0.6 * gender_score + 0.4 * age_score
    elif survey_type.lower() == "perusahaan":
        score = 0.5 * gender_score + 0.5 * age_score
    else:
        score = 0.5
    return round(score, 3)

def run_fuzzy_cbf(base_dir: str):
    load_dotenv(find_dotenv(), override=True)
    
    processed_dir = os.path.join(base_dir, "data", "processed")
    report_dir = os.path.join(base_dir, "data", "reports")
    os.makedirs(report_dir, exist_ok=True)
    
    print(f"\n📥 Reading data from PostgreSQL with CSV fallback...")
    
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "simitra_postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "mitra123"),
        database=os.getenv("POSTGRES_DB", "mitra_kaido")
    )
    
    feature_path = os.path.join(processed_dir, "features_mitra_survey.csv")
    try:
        df = pd.read_sql("SELECT * FROM features_mitra_survey", conn)
        if len(df) == 0:
            raise ValueError("features_mitra_survey table is empty")
        column_mapping = {
            'mitra_id': 'mitra_ID',
            'survey_id': 'survey_ID'
        }
        df.rename(columns=column_mapping, inplace=True)
        print(f"   ✅ Features (PostgreSQL): {len(df)} rows")
    except Exception:
        if not os.path.exists(feature_path):
            raise FileNotFoundError("❌ File fitur tidak ditemukan. Jalankan feature engineering terlebih dahulu.")
        df = pd.read_csv(feature_path)
        print(f"   ✅ Features (CSV fallback): {len(df)} rows")
    
    try:
        df_nilai = pd.read_sql("SELECT * FROM nilai1s_cleaned", conn)
        if len(df_nilai) == 0:
            raise ValueError("nilai1s_cleaned is empty")
        print(f"   ✅ Nilai (PostgreSQL): {len(df_nilai)} rows")
    except Exception:
        nilai_path = os.path.join(processed_dir, "cleaned_nilai1s.csv")
        df_nilai = pd.read_csv(nilai_path)
        print(f"   ✅ Nilai (CSV fallback): {len(df_nilai)} rows")
    
    try:
        df_trans = pd.read_sql("SELECT * FROM transactions_cleaned", conn)
        if len(df_trans) == 0:
            raise ValueError("transactions_cleaned is empty")
        print(f"   ✅ Transactions (PostgreSQL): {len(df_trans)} rows")
    except Exception:
        trans_path = os.path.join(processed_dir, "cleaned_transactions.csv")
        if os.path.exists(trans_path):
            df_trans = pd.read_csv(trans_path)
            print(f"   ✅ Transactions (CSV fallback): {len(df_trans)} rows")
        else:
            df_trans = pd.DataFrame()
    
    conn.close()
    print(f"   PostgreSQL connection closed\n")

    df["mitra_ID"] = pd.to_numeric(df["mitra_ID"], errors="coerce")
    df_nilai["transaction_id"] = pd.to_numeric(df_nilai["transaction_id"], errors="coerce")

    if len(df_trans) > 0:
        df_trans["id"] = pd.to_numeric(df_trans["id"], errors="coerce")
        df_trans["mitra_id"] = pd.to_numeric(df_trans["mitra_id"], errors="coerce")
        df_map = df_trans[["id", "mitra_id"]].rename(columns={"id": "transaction_id"})
        df_nilai = df_nilai.merge(df_map, on="transaction_id", how="left")
    else:
        df_nilai["mitra_id"] = np.nan

    df_avg = df_nilai.groupby("mitra_id")["rerata"].mean().reset_index(name="avg_score")

    df = df.merge(df_avg, how="left", left_on="mitra_ID", right_on="mitra_id")
    df["avg_score"] = df["avg_score"].fillna(0)

    survey_pref = (
        df.groupby(["mitra_ID", "survey_type"])["avg_score"]
        .mean()
        .reset_index()
        .sort_values(["mitra_ID", "avg_score"], ascending=[True, False])
    )

    dominant = survey_pref.groupby("mitra_ID").first().reset_index()
    dominant = dominant.rename(columns={"survey_type": "survey_type_dominant"})
    df = df.merge(dominant[["mitra_ID", "survey_type_dominant"]], on="mitra_ID", how="left")
    df["survey_type_dominant"] = df["survey_type_dominant"].fillna("Perusahaan")
    df.loc[df["survey_type_dominant"] == "Tidak Ada", "survey_type_dominant"] = "Perusahaan"

    df_unique = df.groupby(
        ["mitra_ID", "mitra_name", "mitra_gender", "mitra_age", "survey_type_dominant"]
    ).first().reset_index()

    fuzzy_scores = []
    for idx, row in df_unique.iterrows():
        score = fuzzy_similarity(row, row["survey_type_dominant"])
        fuzzy_scores.append(float(score))
    
    df_unique["fuzzy_score"] = fuzzy_scores  

    X = pd.get_dummies(df_unique[["mitra_gender", "survey_type_dominant"]])
    X["mitra_age"] = df_unique["mitra_age"].astype(float)
    X["fuzzy_score"] = df_unique["fuzzy_score"]

    similarity_matrix = cosine_similarity(X)
    df_unique["cbf_avg_similarity"] = similarity_matrix.mean(axis=1)
    df_unique["final_recommendation_score"] = (
        0.7 * df_unique["fuzzy_score"] + 0.3 * df_unique["cbf_avg_similarity"]
    )

    df_unique = df_unique.sort_values(by="final_recommendation_score", ascending=False).reset_index(drop=True)

    df_final = df_unique.rename(columns={"survey_type_dominant": "survey_type"})[
        ["mitra_ID", "mitra_name", "mitra_gender", "mitra_age",
        "survey_type", "fuzzy_score", "cbf_avg_similarity", "final_recommendation_score"]
    ]

    df_final = df_final.loc[:, ~df_final.columns.duplicated()]
    
    print(f"\n🔍 Checking for mitras without transactions...")
    try:
        df_all_mitras = pd.read_sql("SELECT * FROM mitras", 
            psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "simitra_postgres"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "mitra123"),
                database=os.getenv("POSTGRES_DB", "mitra_kaido")
            ))
        print(f"   ✅ Total mitras in database: {len(df_all_mitras)}")
    except Exception:
        mitras_path = os.path.join(processed_dir, "cleaned_mitras.csv")
        df_all_mitras = pd.read_csv(mitras_path)
        print(f"   ✅ Total mitras from CSV: {len(df_all_mitras)}")
    
    existing_mitra_ids = df_final['mitra_ID'].unique()
    missing_mitras = df_all_mitras[~df_all_mitras['id'].isin(existing_mitra_ids)]
    
    if len(missing_mitras) > 0:
        print(f"   ⚠️  Found {len(missing_mitras)} mitras without transactions")
        
        def calc_age(date_str):
            try:
                if pd.isna(date_str) or len(str(date_str)) < 4:
                    return 30
                year = int(str(date_str).split("-")[0])
                return datetime.now().year - year
            except:
                return 30
        
        df_missing = pd.DataFrame({
            'mitra_ID': missing_mitras['id'].values,
            'mitra_name': missing_mitras['name'].values,
            'mitra_gender': missing_mitras['jenis_kelamin'].fillna('Laki-laki').values,
            'mitra_age': missing_mitras['tanggal_lahir'].apply(calc_age).values,
            'survey_type': 'Perusahaan',  
            'fuzzy_score': 0.5,  
            'cbf_avg_similarity': 0.5,  
            'final_recommendation_score': 0.5  
        })
        
        df_final = pd.concat([df_final, df_missing], ignore_index=True)
        print(f"   ✅ Added {len(missing_mitras)} missing mitras")
        print(f"   ✅ Final total: {len(df_final)} mitras")
    else:
        print(f"   ✅ All mitras are included")
    
    print(f"\n🔍 Final validation...")
    nan_survey_count = df_final['survey_type'].isna().sum()
    if nan_survey_count > 0:
        print(f"   ⚠️  Found {nan_survey_count} rows with NaN survey_type, filling with 'Perusahaan'")
        df_final['survey_type'] = df_final['survey_type'].fillna('Perusahaan')
    
    print(f"\n📊 Final survey_type distribution:")
    for stype, count in df_final['survey_type'].value_counts().items():
        print(f"   {stype}: {count} mitras")
    
    print(f"   Total unique mitras: {df_final['mitra_ID'].nunique()}")
    print(f"   Total rows: {len(df_final)}")

    out_path_csv = os.path.join(report_dir, "cbf_ranked_mitra.csv")

    df_final.to_csv(out_path_csv, index=False)

    print(f"✅ Fuzzy + CBF berhasil dijalankan → {len(df_final)} mitra total (termasuk yang belum survei).")
    print(f"📊 Output disimpan di: {out_path_csv}")
    return df_final

if __name__ == "__main__":
    BASE_DIR = os.getcwd()
    run_fuzzy_cbf(BASE_DIR)
