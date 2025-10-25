-- ========================================
-- ML Training Tables Initialization
-- Simitra Kaido - PostgreSQL Setup
-- ========================================
-- This script creates all necessary tables for ML training pipeline
-- Run automatically on first PostgreSQL container startup

\c mitra_kaido;

-- ========================================
-- 1. MITRA DATA TABLE
-- ========================================
CREATE TABLE IF NOT EXISTS mitra_cleaned (
    id BIGINT PRIMARY KEY,
    sobat_id BIGINT,
    name VARCHAR(200),
    user_id BIGINT,
    email VARCHAR(200),
    pendidikan VARCHAR(50),
    jenis_kelamin VARCHAR(50),
    tanggal_lahir DATE,
    photo VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_mitra_cleaned_name ON mitra_cleaned(name);
CREATE INDEX IF NOT EXISTS idx_mitra_cleaned_email ON mitra_cleaned(email);

-- ========================================
-- 2. MASTER SURVEYS TABLE
-- ========================================
CREATE TABLE IF NOT EXISTS master_surveys_enriched (
    id BIGINT PRIMARY KEY,
    name VARCHAR(100),
    code VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    type VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_master_surveys_type ON master_surveys_enriched(type);

-- ========================================
-- 3. SURVEYS TABLE (Main Survey Records)
-- ========================================
CREATE TABLE IF NOT EXISTS surveys_cleaned (
    id BIGINT PRIMARY KEY,
    master_survey_id BIGINT,
    triwulan SMALLINT,
    year INT,
    payment_month INT,
    payment_id BIGINT,
    team_id BIGINT,
    rate INT,
    file VARCHAR(255),
    is_scored SMALLINT DEFAULT 0,
    is_synced SMALLINT DEFAULT 0,
    status VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_surveys_master_survey ON surveys_cleaned(master_survey_id);
CREATE INDEX IF NOT EXISTS idx_surveys_status ON surveys_cleaned(status);
CREATE INDEX IF NOT EXISTS idx_surveys_is_scored ON surveys_cleaned(is_scored);

-- ========================================
-- 4. TRANSACTIONS TABLE (Survey Assignments)
-- ========================================
CREATE TABLE IF NOT EXISTS transactions_cleaned (
    id BIGINT PRIMARY KEY,
    mitra_id BIGINT,
    survey_id BIGINT,
    target INT,
    rate INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transactions_mitra ON transactions_cleaned(mitra_id);
CREATE INDEX IF NOT EXISTS idx_transactions_survey ON transactions_cleaned(survey_id);
CREATE INDEX IF NOT EXISTS idx_transactions_mitra_survey ON transactions_cleaned(mitra_id, survey_id);

-- ========================================
-- 5. NILAI TABLE (Survey Scores/Ratings)
-- ========================================
CREATE TABLE IF NOT EXISTS nilai1s_cleaned (
    transaction_id BIGINT PRIMARY KEY,
    aspek1 SMALLINT,
    aspek2 SMALLINT,
    aspek3 SMALLINT,
    rerata DECIMAL(5,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_nilai1s_rerata ON nilai1s_cleaned(rerata);

-- ========================================
-- 6. FEATURE ENGINEERING TABLE
-- ========================================
CREATE TABLE IF NOT EXISTS features_mitra_survey (
    mitra_id BIGINT,
    survey_type VARCHAR(100),
    fuzzy_score FLOAT,
    cbf_avg_similarity FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (mitra_id, survey_type)
);

CREATE INDEX IF NOT EXISTS idx_features_survey_type ON features_mitra_survey(survey_type);
CREATE INDEX IF NOT EXISTS idx_features_fuzzy_score ON features_mitra_survey(fuzzy_score DESC);
CREATE INDEX IF NOT EXISTS idx_features_cbf_similarity ON features_mitra_survey(cbf_avg_similarity DESC);

-- ========================================
-- 7. PSO OPTIMIZATION RESULTS TABLE
-- ========================================
CREATE TABLE IF NOT EXISTS pso_optimized_mitra (
    mitra_id BIGINT,
    mitra_name VARCHAR(200),
    model_type VARCHAR(50),
    fuzzy_score FLOAT,
    cbf_avg_similarity FLOAT,
    optimized_score FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (mitra_id, model_type)
);

CREATE INDEX IF NOT EXISTS idx_pso_model_type ON pso_optimized_mitra(model_type);
CREATE INDEX IF NOT EXISTS idx_pso_optimized_score ON pso_optimized_mitra(optimized_score DESC);

-- ========================================
-- 8. FINAL RECOMMENDATIONS TABLE - RUMAH TANGGA
-- ========================================
CREATE TABLE IF NOT EXISTS recommendation_rumah_tangga (
    mitra_id BIGINT PRIMARY KEY,
    mitra_name VARCHAR(200),
    survey_type VARCHAR(100),
    survey_score FLOAT,
    jumlah_survey INT,
    exp_norm FLOAT,
    weighted_score FLOAT,
    optimized_score FLOAT,
    final_rank_score FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_recommendation_rt_final_rank ON recommendation_rumah_tangga(final_rank_score DESC);
CREATE INDEX IF NOT EXISTS idx_recommendation_rt_survey_score ON recommendation_rumah_tangga(survey_score DESC);

-- ========================================
-- 9. FINAL RECOMMENDATIONS TABLE - PERUSAHAAN
-- ========================================
CREATE TABLE IF NOT EXISTS recommendation_perusahaan (
    mitra_id BIGINT PRIMARY KEY,
    mitra_name VARCHAR(200),
    survey_type VARCHAR(100),
    survey_score FLOAT,
    jumlah_survey INT,
    exp_norm FLOAT,
    weighted_score FLOAT,
    optimized_score FLOAT,
    final_rank_score FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_recommendation_pr_final_rank ON recommendation_perusahaan(final_rank_score DESC);
CREATE INDEX IF NOT EXISTS idx_recommendation_pr_survey_score ON recommendation_perusahaan(survey_score DESC);

-- ========================================
-- GRANT PERMISSIONS
-- ========================================
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;

-- ========================================
-- VERIFICATION
-- ========================================
\echo '========================================';
\echo 'PostgreSQL ML Tables Initialized';
\echo '========================================';
\echo 'Tables created:';
SELECT schemaname, tablename 
FROM pg_tables 
WHERE schemaname = 'public' 
  AND tablename LIKE '%cleaned%' 
   OR tablename LIKE 'recommendation%'
   OR tablename LIKE 'features_%'
   OR tablename LIKE 'pso_%'
ORDER BY tablename;

\echo '';
\echo '✅ Setup complete! Ready for ML training pipeline.';
