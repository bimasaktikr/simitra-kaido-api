"""
Experience Aggregation Pipeline
Combines ML ratings (from PSO) with historical survey performance & experience
Output: Recommendation tables with final_rank_score (rating + experience)
"""
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv, find_dotenv


def aggregate_experience(base_dir: str, survey_type: str = None):
    """
    Aggregate mitra experience & performance with ML ratings
    
    Args:
        base_dir: Base directory path
        survey_type: 'rumah_tangga', 'perusahaan', or None (both)
    
    Returns:
        Dict with aggregation results
    """
    load_dotenv(find_dotenv(), override=True)

    processed_dir = os.path.join(base_dir, "data", "processed")
    report_dir = os.path.join(base_dir, "data", "reports")

    print(f"\n{'='*70}")
    print(f"📊 EXPERIENCE AGGREGATION")
    print(f"{'='*70}")

    pg_conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "simitra_postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        database=os.getenv("POSTGRES_DB", "mitra_kaido")
    )
    
    print(f"\n📥 Reading data from PostgreSQL...")
    df_nilai = pd.read_sql("SELECT * FROM nilai1s_cleaned", pg_conn)
    df_trans = pd.read_sql("SELECT * FROM transactions_cleaned", pg_conn)
    df_survey = pd.read_sql("SELECT * FROM surveys_cleaned WHERE is_scored = 1", pg_conn)
    df_master = pd.read_sql("SELECT * FROM master_surveys_enriched", pg_conn)
    
    df_mitra = None
    for table_name in ["mitras_enriched", "mitras_cleaned", "mitras"]:
        try:
            df_mitra = pd.read_sql(f"SELECT id, name FROM {table_name}", pg_conn)
            print(f"   - Mitras (from {table_name}): {len(df_mitra)} records")
            break
        except Exception as e:
            continue
    
    if df_mitra is None:
        print(f"   ⚠️  WARNING: No mitra table found! Mitra names will be NULL for those not in PSO data")
        df_mitra = pd.DataFrame(columns=["id", "name"])  
    
    print(f"   - Nilai: {len(df_nilai)} records")
    print(f"   - Transactions: {len(df_trans)} records")
    print(f"   - Surveys (scored only): {len(df_survey)} records")
    print(f"   - Master surveys: {len(df_master)} records")
    
    df_pso = pd.read_csv(os.path.join(report_dir, "pso_optimized_mitra.csv"))
    
    pg_conn.close()
    print(f"   PostgreSQL connection closed\n")

    df_survey = df_survey.merge(
        df_master[["id", "type"]],
        left_on="master_survey_id",
        right_on="id",
        how="left",
        suffixes=("", "_master")
    ).rename(columns={"type": "survey_type", "id": "survey_id_final"}).drop(columns=["id_master"])

    df_join = df_trans.merge(df_nilai, left_on="id", right_on="transaction_id", how="left")
    df_join = df_join.merge(
        df_survey[["survey_id_final", "survey_type"]],
        left_on="survey_id",
        right_on="survey_id_final",
        how="left"
    )

    df_join = df_join[["mitra_id", "survey_type", "survey_id", "transaction_id", "rerata"]].dropna()
    df_join["rerata"] = df_join["rerata"].astype(float)

    print(f"\n📈 Aggregating historical performance...")
    df_agg = (
        df_join.groupby(["mitra_id", "survey_type"], as_index=False)
        .agg(
            survey_score=("rerata", "mean"),      
            jumlah_survey=("survey_id", "nunique")  
        )
    )
    
    if df_mitra is not None and len(df_mitra) > 0:
        df_agg = df_agg.merge(
            df_mitra[["id", "name"]],
            left_on="mitra_id",
            right_on="id",
            how="left"
        ).rename(columns={"name": "mitra_name"}).drop(columns=["id"])
        
        nan_count = df_agg["mitra_name"].isna().sum()
        if nan_count > 0:
            print(f"   ⚠️  {nan_count} mitra without names in mitra table")
    else:
        df_agg["mitra_name"] = None

    print(f"   Total mitra with experience: {len(df_agg)}")
    print(f"   Survey types in aggregation: {sorted(df_agg['survey_type'].unique())}")
    print(f"   Rumah Tangga count: {len(df_agg[df_agg['survey_type'].str.contains('Rumah', case=False, na=False)])}")
    print(f"   Perusahaan count: {len(df_agg[df_agg['survey_type'].str.contains('Perusahaan', case=False, na=False)])}")

    df_pso["model_type_clean"] = df_pso["model_type"].str.strip().str.lower().str.replace(" ", "_")
    df_agg["survey_type_clean"] = df_agg["survey_type"].str.strip().str.lower().str.replace(" ", "_")
    
    df_final = df_agg.merge(
        df_pso[["mitra_ID", "mitra_name", "model_type", "model_type_clean", "optimized_score"]],
        left_on=["mitra_id", "survey_type_clean"],
        right_on=["mitra_ID", "model_type_clean"],
        how="left",  
        suffixes=("", "_pso")
    )
    
    if "mitra_name_pso" in df_final.columns:
        df_final["mitra_name"] = df_final["mitra_name"].fillna(df_final["mitra_name_pso"])
        df_final.loc[df_final["mitra_name"] == "NaN", "mitra_name"] = df_final.loc[df_final["mitra_name"] == "NaN", "mitra_name_pso"]
        df_final = df_final.drop(columns=["mitra_name_pso"])
    
    df_final.loc[df_final["mitra_name"] == "NaN", "mitra_name"] = None
    
    missing_names = df_final[df_final["mitra_name"].isna()]
    if len(missing_names) > 0:
        print(f"\n   ⚠️  WARNING: {len(missing_names)} mitra still without names after merge!")
        print(f"   Missing name mitra_ids: {missing_names['mitra_id'].unique().tolist()[:10]}...")
    
    print(f"\n   After merge with PSO (matching by mitra_id AND survey_type): {len(df_final)} rows")
    print(f"   - Unique mitra: {df_final['mitra_id'].nunique()}")
    print(f"   - Rumah Tangga: {len(df_final[df_final['survey_type'].str.contains('Rumah', case=False, na=False)])}")
    print(f"   - Perusahaan: {len(df_final[df_final['survey_type'].str.contains('Perusahaan', case=False, na=False)])}")
    print(f"   - With PSO scores: {len(df_final[df_final['optimized_score'].notna()])}")
    print(f"   - Without PSO (experience only): {len(df_final[df_final['optimized_score'].isna()])}")
    
    df_final = df_final.drop(columns=["mitra_ID", "model_type", "model_type_clean", "survey_type_clean"])

    for survey_type_val in df_final["survey_type"].unique():
        mask = df_final["survey_type"] == survey_type_val
        median_score = df_final.loc[mask & df_final["optimized_score"].notna(), "optimized_score"].median()
        
        if pd.isna(median_score):
            median_score = 0.5  
        
        missing_count = df_final.loc[mask & df_final["optimized_score"].isna()].shape[0]
        if missing_count > 0:
            print(f"   ℹ️  Filling {missing_count} missing PSO scores for {survey_type_val} with median: {median_score:.4f}")
            df_final.loc[mask & df_final["optimized_score"].isna(), "optimized_score"] = median_score

    df_final = df_final.fillna({"optimized_score": 0.5})

    max_exp = df_final["jumlah_survey"].max() if len(df_final) > 0 else 1
    df_final["exp_norm"] = df_final["jumlah_survey"] / max(max_exp, 1)

    def weighted_score(row):
        """
        Combine survey performance with experience
        Different weights for different survey types
        """
        survey_type_lower = str(row["survey_type"]).strip().lower()
        
        if "rumah" in survey_type_lower or survey_type_lower == "rumah tangga":
            alpha, beta = 0.8, 0.2  
        elif "perusahaan" in survey_type_lower:
            alpha, beta = 0.6, 0.4  
        else:
            alpha, beta = 0.7, 0.3  
        
        return (alpha * row["survey_score"]) + (beta * row["exp_norm"] * 5)

    df_final["weighted_score"] = df_final.apply(weighted_score, axis=1)

    max_weighted = df_final["weighted_score"].max() if len(df_final) > 0 else 1
    df_final["final_rank_score"] = (
        0.5 * df_final["optimized_score"] + 
        0.5 * (df_final["weighted_score"] / max(max_weighted, 1))
    )

    df_rt = df_final[df_final["survey_type"].str.strip().str.lower().str.contains("rumah", na=False)].copy()
    df_pr = df_final[df_final["survey_type"].str.strip().str.lower().str.contains("perusahaan", na=False)].copy()

    df_rt = df_rt.sort_values("final_rank_score", ascending=False).reset_index(drop=True)
    df_pr = df_pr.sort_values("final_rank_score", ascending=False).reset_index(drop=True)

    df_rt = df_rt.drop(columns=["mitra_ID", "model_type"], errors="ignore")
    df_pr = df_pr.drop(columns=["mitra_ID", "model_type"], errors="ignore")

    desired_cols = [
        "mitra_id", "mitra_name", "survey_type",
        "survey_score", "jumlah_survey", "exp_norm",
        "weighted_score", "optimized_score", "final_rank_score"
    ]

    df_rt = df_rt.reindex(columns=[c for c in desired_cols if c in df_rt.columns])
    df_pr = df_pr.reindex(columns=[c for c in desired_cols if c in df_pr.columns])

    results = {}
    
    if len(df_rt) > 0:
        out_rt = os.path.join(report_dir, "recommendation_rumah_tangga.csv")
        df_rt.to_csv(out_rt, index=False)
        print(f"\n✅ Rumah Tangga: {len(df_rt)} mitra")
        print(f"   Saved to: {out_rt}")
        results['rumah_tangga'] = len(df_rt)
    
    if len(df_pr) > 0:
        out_pr = os.path.join(report_dir, "recommendation_perusahaan.csv")
        df_pr.to_csv(out_pr, index=False)
        print(f"\n✅ Perusahaan: {len(df_pr)} mitra")
        print(f"   Saved to: {out_pr}")
        results['perusahaan'] = len(df_pr)

    save_to_postgres(df_rt, df_pr)

    print(f"\n{'='*70}")
    print(f"✅ EXPERIENCE AGGREGATION COMPLETED")
    print(f"{'='*70}\n")

    return results


def save_to_postgres(df_rt, df_pr):
    """Save aggregated recommendations to PostgreSQL"""
    
    db_cfg = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "host": os.getenv("DB_HOST", "postgres"),
        "port": os.getenv("DB_PORT", 5432)
    }

    conn = psycopg2.connect(**db_cfg)
    cur = conn.cursor()

    for table in ["recommendation_rumah_tangga", "recommendation_perusahaan"]:
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        cur.execute(f"""
            CREATE TABLE {table} (
                mitra_id BIGINT,
                mitra_name VARCHAR(255),
                survey_type VARCHAR(100),
                survey_score FLOAT,
                jumlah_survey INT,
                exp_norm FLOAT,
                weighted_score FLOAT,
                optimized_score FLOAT,
                final_rank_score FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        cur.execute(f"""
            CREATE INDEX idx_{table}_final_rank_score 
            ON {table}(final_rank_score DESC);
        """)
    
    def insert_df(df, table_name):
        if len(df) == 0:
            return
        
        cols = ",".join(df.columns)
        placeholders = ",".join(["%s"] * len(df.columns))
        query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders});"
        
        for _, row in df.iterrows():
            cur.execute(query, tuple(row.values))

    insert_df(df_rt, "recommendation_rumah_tangga")
    insert_df(df_pr, "recommendation_perusahaan")

    conn.commit()
    
    print(f"\n🔄 Fixing mitra names from mitra_cleaned table...")
    for table in ["recommendation_rumah_tangga", "recommendation_perusahaan"]:
        cur.execute(f"""
            UPDATE {table} r
            SET mitra_name = m.name
            FROM mitra_cleaned m
            WHERE r.mitra_id = m.id
            AND (r.mitra_name IS NULL OR r.mitra_name = 'NaN')
        """)
        fixed_count = cur.rowcount
        if fixed_count > 0:
            print(f"   ✅ Fixed {fixed_count} mitra names in {table}")
    
    conn.commit()
    cur.close()
    conn.close()

    print(f"\n✅ Data saved to PostgreSQL:")
    print(f"   - recommendation_rumah_tangga: {len(df_rt)} rows")
    print(f"   - recommendation_perusahaan: {len(df_pr)} rows")


if __name__ == "__main__":
    import sys
    BASE_DIR = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
    aggregate_experience(BASE_DIR)
