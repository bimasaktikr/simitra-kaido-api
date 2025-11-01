import os
import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv, find_dotenv

def apply_age_gender_priority(df, survey_type):
    """
    Apply age and gender priority based on survey type
    
    Rumah Tangga:
    - Gender: Female > Male (Female = 1.2x, Male = 1.0x)
    - Age Priority: 41-50 > 31-40 > 51+ > 21-30
      (x3 > x2 > x4 > x1)
      
    Perusahaan:
    - Gender: Female > Male (Female = 1.2x, Male = 1.0x)
    - Age Priority: 51+ > 41-50 > 31-40 > 21-30
      (x4 > x3 > x2 > x1)
    """
    df = df.copy()
    df['gender_weight'] = df['mitra_gender'].apply(
        lambda x: 1.2 if str(x).lower() in ['female', 'perempuan', 'p'] else 1.0
    )
    
    def get_age_weight(age, survey_type):
        try:
            age = float(age)
        except:
            return 1.0
            
        if survey_type.lower() == 'rumah_tangga':
            if 41 <= age <= 50:
                return 1.5
            elif 31 <= age <= 40:
                return 1.3
            elif age >= 51:
                return 1.2
            elif 21 <= age <= 30:
                return 1.0
            else:
                return 1.0
        else:
            if age >= 51:
                return 1.5
            elif 41 <= age <= 50:
                return 1.3
            elif 31 <= age <= 40:
                return 1.2
            elif 21 <= age <= 30:
                return 1.0 
            else:
                return 1.0
    
    df['age_weight'] = df.apply(
        lambda row: get_age_weight(row.get('mitra_age', 0), survey_type),
        axis=1
    )
    
    df['demographic_weight'] = df['gender_weight'] * df['age_weight']
    
    return df

def fitness_function(weights, df):
    """
    Fitness function for PSO optimization
    Optimizes 2 weights: fuzzy + cbf
    
    BALANCED APPROACH: Multi-objective with natural balance incentive
    """
    w_fuzzy, w_cbf = weights
    
    if w_fuzzy <= 0 or w_cbf <= 0:
        return -999
    
    weight_sum = w_fuzzy + w_cbf
    w_fuzzy_norm = w_fuzzy / weight_sum
    w_cbf_norm = w_cbf / weight_sum
    
    df["pso_score"] = w_fuzzy_norm * df["fuzzy_score"] + w_cbf_norm * df["cbf_avg_similarity"]

    std_dev = np.std(df["pso_score"])
    mean_score = np.mean(df["pso_score"])
    cv = std_dev / (mean_score + 1e-10)
    
    score_range = df["pso_score"].max() - df["pso_score"].min()
    max_possible_range = 1.0  
    normalized_range = score_range / max_possible_range
    
    epsilon = 1e-10
    entropy = -(w_fuzzy_norm * np.log(w_fuzzy_norm + epsilon) + 
                w_cbf_norm * np.log(w_cbf_norm + epsilon))
    max_entropy = np.log(2)  
    normalized_entropy = entropy / max_entropy
    
    fitness = (
        0.6 * cv +                      
        0.3 * normalized_entropy +      
        0.1 * normalized_range          
    )
    
    return fitness

def run_pso(df, survey_type, n_particles=40, n_iterations=100, inertia=0.6, c1=1.8, c2=1.8):
    """
    Run PSO optimization for specific survey type
    Optimizes 2 weights: fuzzy + cbf
    
    Updated strategy:
    - 40 particles for better exploration
    - Lower inertia (0.6) for faster convergence
    - Soft constraints via penalty instead of hard bounds
    """
    print(f"\n{'='*60}")
    print(f"🎯 Running PSO Optimization for: {survey_type.upper()}")
    print(f"{'='*60}")
    
    np.random.seed(42 if survey_type == 'rumah_tangga' else 84)
    min_bound, max_bound = 0.01, 2.0

    particles = np.random.uniform(min_bound, max_bound, (n_particles, 2))
    velocities = np.random.uniform(-0.2, 0.2, (n_particles, 2))

    personal_best = particles.copy()
    personal_best_scores = np.array([fitness_function(p, df.copy()) for p in particles])
    global_best_idx = np.argmax(personal_best_scores)
    global_best = personal_best[global_best_idx].copy()
    global_best_score = personal_best_scores[global_best_idx]

    print(f"📊 Initial data stats:")
    print(f"   Fuzzy: min={df['fuzzy_score'].min():.3f}, max={df['fuzzy_score'].max():.3f}, mean={df['fuzzy_score'].mean():.3f}, std={df['fuzzy_score'].std():.3f}")
    print(f"   CBF:   min={df['cbf_avg_similarity'].min():.3f}, max={df['cbf_avg_similarity'].max():.3f}, mean={df['cbf_avg_similarity'].mean():.3f}, std={df['cbf_avg_similarity'].std():.3f}")

    for i in range(n_iterations):
        for j in range(n_particles):
            r1, r2 = np.random.rand(2)
            velocities[j] = (
                inertia * velocities[j]
                + c1 * r1 * (personal_best[j] - particles[j])
                + c2 * r2 * (global_best - particles[j])
            )

            particles[j] = np.clip(particles[j] + velocities[j], min_bound, max_bound)
            score = fitness_function(particles[j], df.copy())

            if score > personal_best_scores[j]:
                personal_best[j] = particles[j].copy()
                personal_best_scores[j] = score

        best_idx = np.argmax(personal_best_scores)
        if personal_best_scores[best_idx] > global_best_score:
            global_best = personal_best[best_idx].copy()
            global_best_score = personal_best_scores[best_idx]

        if (i + 1) % 20 == 0:
            w_sum = global_best[0] + global_best[1]
            w_f_norm = global_best[0] / w_sum
            w_c_norm = global_best[1] / w_sum
            print(f"🌀 Iterasi {i+1}/{n_iterations} | Score = {global_best_score:.5f} | Weights = [{w_f_norm:.3f}, {w_c_norm:.3f}]")

    weight_sum = global_best[0] + global_best[1]
    normalized_weights = global_best / weight_sum
    
    print(f"\n✅ Bobot optimal untuk {survey_type}:")
    print(f"   w_fuzzy = {normalized_weights[0]:.4f}")
    print(f"   w_cbf   = {normalized_weights[1]:.4f}")
    print(f"   (sum = {normalized_weights.sum():.4f})")
    
    return normalized_weights

def weight_optimizer(base_dir: str, survey_type: str = None):
    """
    Main weight optimizer function
    Processes specific survey type or BOTH if survey_type is None
    
    Args:
        base_dir: Base directory path
        survey_type: 'rumah_tangga', 'perusahaan', or None (both)
    """
    load_dotenv(find_dotenv(), override=True)

    report_dir = os.path.join(base_dir, "data", "reports")
    
    # Create reports directory if it doesn't exist
    os.makedirs(report_dir, exist_ok=True)
    
    ranked_path = os.path.join(report_dir, "cbf_ranked_mitra.csv")

    if not os.path.exists(ranked_path):
        raise FileNotFoundError("❌ File cbf_ranked_mitra.csv tidak ditemukan.")

    df_all = pd.read_csv(ranked_path)
    df_all = df_all.dropna(subset=["fuzzy_score", "cbf_avg_similarity"])
    
    print(f"\n📊 Total data: {len(df_all)} mitra")
    print(f"   Rumah Tangga: {len(df_all[df_all['survey_type'] == 'Rumah Tangga'])} mitra")
    print(f"   Perusahaan: {len(df_all[df_all['survey_type'] == 'Perusahaan'])} mitra")
    
    df_rt = df_all[df_all['survey_type'] == 'Rumah Tangga'].copy()
    df_pr = df_all[df_all['survey_type'] == 'Perusahaan'].copy()
    
    results = []
    weights_rt = None
    weights_pr = None
    
    process_rt = (survey_type is None or survey_type.lower() == 'rumah_tangga') and len(df_rt) > 0
    process_pr = (survey_type is None or survey_type.lower() == 'perusahaan') and len(df_pr) > 0
    
    if process_rt:
        print(f"\n{'='*70}")
        print(f"🏠 PROCESSING: RUMAH TANGGA ({len(df_rt)} mitra)")
        print(f"{'='*70}")
        
        weights_rt = run_pso(df_rt, 'rumah_tangga')
        
        df_rt["optimized_score"] = (
            weights_rt[0] * df_rt["fuzzy_score"] + 
            weights_rt[1] * df_rt["cbf_avg_similarity"]
        )
        
        if df_rt["optimized_score"].max() > 0:
            df_rt["optimized_score"] = df_rt["optimized_score"] / df_rt["optimized_score"].max()
        
        df_rt = df_rt.sort_values(by="optimized_score", ascending=False).reset_index(drop=True)
        df_rt['model_type'] = 'rumah_tangga'
        df_rt['weight_fuzzy'] = weights_rt[0]
        df_rt['weight_cbf'] = weights_rt[1]
        
        results.append(df_rt)
        
        out_rt = os.path.join(report_dir, "pso_optimized_rumah_tangga.csv")
        df_rt.to_csv(out_rt, index=False)
        print(f"📤 Saved to: {out_rt}")
    
    if process_pr:
        print(f"\n{'='*70}")
        print(f"🏢 PROCESSING: PERUSAHAAN ({len(df_pr)} mitra)")
        print(f"{'='*70}")
        
        weights_pr = run_pso(df_pr, 'perusahaan')
        
        df_pr["optimized_score"] = (
            weights_pr[0] * df_pr["fuzzy_score"] + 
            weights_pr[1] * df_pr["cbf_avg_similarity"]
        )
        
        if df_pr["optimized_score"].max() > 0:
            df_pr["optimized_score"] = df_pr["optimized_score"] / df_pr["optimized_score"].max()
        
        df_pr = df_pr.sort_values(by="optimized_score", ascending=False).reset_index(drop=True)
        df_pr['model_type'] = 'perusahaan'
        df_pr['weight_fuzzy'] = weights_pr[0]
        df_pr['weight_cbf'] = weights_pr[1]
        
        results.append(df_pr)
        
        out_pr = os.path.join(report_dir, "pso_optimized_perusahaan.csv")
        df_pr.to_csv(out_pr, index=False)
        print(f"📤 Saved to: {out_pr}")
    
    if len(results) == 0:
        print("⚠️ No data to process!")
        return {"rumah_tangga": 0, "perusahaan": 0}
    
    print(f"\n🔍 DEBUG - Results list before concat:")
    print(f"   Number of DataFrames in results: {len(results)}")
    for i, df in enumerate(results):
        model_type_val = df['model_type'].iloc[0] if len(df) > 0 and 'model_type' in df.columns else 'UNKNOWN'
        print(f"   DataFrame {i}: {len(df)} rows, model_type='{model_type_val}'")
    
    df_combined = pd.concat(results, ignore_index=True)
    
    print(f"\n🔍 DEBUG - Combined DataFrame after concat:")
    print(f"   Total rows: {len(df_combined)}")
    print(f"   Unique model_types: {df_combined['model_type'].unique().tolist()}")
    print(f"   Model type counts: {df_combined['model_type'].value_counts().to_dict()}")
    
    if survey_type is None:
        out_combined = os.path.join(report_dir, "pso_optimized_mitra.csv")
        df_combined.to_csv(out_combined, index=False)
        print(f"\n📤 Combined file saved to: {out_combined}")
    else:
        print(f"\n⏭️  Skipping combined file (processing single type: {survey_type})")
        print(f"   Combined file will be created by separate merge task")
    
    print(f"\n📊 OPTIMIZATION SUMMARY:")
    print(f"   Total optimized mitra: {len(df_combined)}")
    if weights_rt is not None:
        rt_count = len([r for r in results if r['model_type'].iloc[0] == 'rumah_tangga'][0]) if any(r['model_type'].iloc[0] == 'rumah_tangga' for r in results) else 0
        print(f"   Rumah Tangga: {rt_count} mitra | Weights: F={weights_rt[0]:.3f}, C={weights_rt[1]:.3f}")
    if weights_pr is not None:
        pr_count = len([r for r in results if r['model_type'].iloc[0] == 'perusahaan'][0]) if any(r['model_type'].iloc[0] == 'perusahaan' for r in results) else 0
        print(f"   Perusahaan: {pr_count} mitra | Weights: F={weights_pr[0]:.3f}, C={weights_pr[1]:.3f}")

    db_cfg = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "host": os.getenv("DB_HOST", "postgres"),
        "port": os.getenv("DB_PORT", 5432)
    }
    
    if survey_type is None:
        print(f"\n📤 Uploading to PostgreSQL...")
        
        conn = psycopg2.connect(**db_cfg)
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS pso_optimized_mitra CASCADE;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pso_optimized_mitra (
                mitra_ID BIGINT,
                mitra_name VARCHAR(255),
                mitra_gender VARCHAR(50),
                mitra_age FLOAT,
                survey_type VARCHAR(100),
                model_type VARCHAR(50),
                fuzzy_score FLOAT,
                cbf_avg_similarity FLOAT,
                optimized_score FLOAT,
                weight_fuzzy FLOAT,
                weight_cbf FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_pso_model_type ON pso_optimized_mitra(model_type);
            CREATE INDEX IF NOT EXISTS idx_pso_optimized_score ON pso_optimized_mitra(optimized_score DESC);
        """)

        for _, r in df_combined.iterrows():
            cur.execute("""
                INSERT INTO pso_optimized_mitra (
                    mitra_ID, mitra_name, mitra_gender, mitra_age,
                    survey_type, model_type, fuzzy_score, cbf_avg_similarity,
                    optimized_score,
                    weight_fuzzy, weight_cbf
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """, (
                r.get("mitra_ID"), r.get("mitra_name"), r.get("mitra_gender"),
                r.get("mitra_age"), r.get("survey_type"), r.get("model_type"),
                r.get("fuzzy_score"), r.get("cbf_avg_similarity"),
                r.get("optimized_score"),
                r.get("weight_fuzzy"), r.get("weight_cbf")
            ))

        conn.commit()
        cur.close()
        conn.close()

        print(f"\n✅ Data PSO berhasil diunggah ke PostgreSQL")
        print(f"   Table: pso_optimized_mitra")
        print(f"   Total rows: {len(df_combined)}")
        print(f"   Mode: DUAL MODEL (Rumah Tangga & Perusahaan)")
    else:
        print(f"\n⏭️  Skipping PostgreSQL upload (processing single type: {survey_type})")
        print(f"   Data will be uploaded by merge task")
    
    return {
        "rumah_tangga": len(df_rt) if weights_rt is not None else 0,
        "perusahaan": len(df_pr) if weights_pr is not None else 0,
        "total": len(df_combined)
    }


def merge_pso_results(base_dir: str):
    """
    Merge individual PSO result files (rumah_tangga and perusahaan) into a combined file.
    This task should run after both optimization tasks complete.
    
    Args:
        base_dir: Base directory path
    
    Returns:
        dict: Summary of merged results
    """
    report_dir = os.path.join(base_dir, "data", "reports")
    rt_file = os.path.join(report_dir, "pso_optimized_rumah_tangga.csv")
    pr_file = os.path.join(report_dir, "pso_optimized_perusahaan.csv")
    combined_file = os.path.join(report_dir, "pso_optimized_mitra.csv")
    
    print(f"\n{'='*70}")
    print(f"🔗 MERGING PSO RESULTS")
    print(f"{'='*70}")
    
    results = []
    
    if os.path.exists(rt_file):
        df_rt = pd.read_csv(rt_file)
        print(f"✅ Loaded Rumah Tangga: {len(df_rt)} rows from {rt_file}")
        results.append(df_rt)
    else:
        print(f"⚠️  Rumah Tangga file not found: {rt_file}")
    
    if os.path.exists(pr_file):
        df_pr = pd.read_csv(pr_file)
        print(f"✅ Loaded Perusahaan: {len(df_pr)} rows from {pr_file}")
        results.append(df_pr)
    else:
        print(f"⚠️  Perusahaan file not found: {pr_file}")
    
    if len(results) == 0:
        raise FileNotFoundError("❌ No PSO result files found to merge!")
    
    df_combined = pd.concat(results, ignore_index=True)
    
    print(f"\n📊 MERGE SUMMARY:")
    print(f"   Total rows: {len(df_combined)}")
    print(f"   Model types: {df_combined['model_type'].value_counts().to_dict()}")
    
    df_combined.to_csv(combined_file, index=False)
    print(f"\n📤 Combined file saved to: {combined_file}")
    
    load_dotenv(find_dotenv(), override=True)
    
    db_cfg = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "host": os.getenv("DB_HOST", "postgres"),
        "port": os.getenv("DB_PORT", 5432)
    }
    
    conn = psycopg2.connect(**db_cfg)
    cur = conn.cursor()
    
    cur.execute("DROP TABLE IF EXISTS pso_optimized_mitra CASCADE;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pso_optimized_mitra (
            mitra_ID BIGINT,
            mitra_name VARCHAR(255),
            mitra_gender VARCHAR(50),
            mitra_age FLOAT,
            survey_type VARCHAR(100),
            model_type VARCHAR(50),
            fuzzy_score FLOAT,
            cbf_avg_similarity FLOAT,
            optimized_score FLOAT,
            weight_fuzzy FLOAT,
            weight_cbf FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_pso_model_type ON pso_optimized_mitra(model_type);
        CREATE INDEX IF NOT EXISTS idx_pso_optimized_score ON pso_optimized_mitra(optimized_score DESC);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_pso_model_type ON pso_optimized_mitra(model_type);
        CREATE INDEX IF NOT EXISTS idx_pso_optimized_score ON pso_optimized_mitra(optimized_score DESC);
    """)

    for _, r in df_combined.iterrows():
        cur.execute(
            """
            INSERT INTO pso_optimized_mitra 
            (mitra_ID, mitra_name, mitra_gender, mitra_age, survey_type, model_type,
             fuzzy_score, cbf_avg_similarity, optimized_score, weight_fuzzy, weight_cbf)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                r.get("mitra_ID"), r.get("mitra_name"), r.get("mitra_gender"),
                r.get("mitra_age"), r.get("survey_type"), r.get("model_type"),
                r.get("fuzzy_score"), r.get("cbf_avg_similarity"),
                r.get("optimized_score"),
                r.get("weight_fuzzy"), r.get("weight_cbf")
            )
        )

    conn.commit()
    cur.close()
    conn.close()
    
    print(f"\n✅ Combined data uploaded to PostgreSQL")
    print(f"   Table: pso_optimized_mitra")
    print(f"   Total rows: {len(df_combined)}")
    
    return {
        "rumah_tangga": len(df_combined[df_combined['model_type'] == 'rumah_tangga']),
        "perusahaan": len(df_combined[df_combined['model_type'] == 'perusahaan']),
        "total": len(df_combined)
    }

if __name__ == "__main__":
    BASE_DIR = os.getcwd()
    weight_optimizer(BASE_DIR)
