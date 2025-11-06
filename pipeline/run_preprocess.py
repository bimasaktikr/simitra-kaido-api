import os, re, platform
import pandas as pd
import psycopg2
from dotenv import load_dotenv, find_dotenv

def run_preprocess(base_dir: str, mode: str = "overwrite"):
    load_dotenv(find_dotenv(), override=True)
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
        try:
            df_s = pd.read_sql("SELECT * FROM surveys_cleaned WHERE is_scored = 1", conn)
            if len(df_s) == 0:
                raise ValueError("surveys_cleaned is empty, trying fallback")
            print(f"   ✅ Found {len(df_s)} scored surveys from surveys_cleaned")
        except Exception as e:
            print(f"   ⚠️  surveys_cleaned issue: {str(e)[:50]}")
            print(f"   🔄 Trying fallback to surveys (raw)...")
            df_s = pd.read_sql("SELECT * FROM surveys WHERE is_scored = 1", conn)
            print(f"   ✅ Found {len(df_s)} scored surveys from surveys (raw)")
        
        if len(df_s) > 0:
            latest_ids = sorted(df_s['id'].astype(int).unique())[-5:]
            print(f"   📊 Latest 5 survey IDs: {latest_ids}")
        else:
            print(f"   ⚠️  WARNING: surveys table is empty!")
        
        print(f"\n📥 Reading transactions from PostgreSQL...")
        try:
            df_t = pd.read_sql("SELECT * FROM transactions_cleaned", conn)
            if len(df_t) == 0:
                raise ValueError("transactions_cleaned is empty, trying fallback")
            print(f"   ✅ Found {len(df_t)} transactions from transactions_cleaned")
        except Exception as e:
            print(f"   ⚠️  transactions_cleaned issue: {str(e)[:50]}")
            print(f"   🔄 Trying fallback to transactions (raw)...")
            df_t = pd.read_sql("SELECT * FROM transactions", conn)
            print(f"   ✅ Found {len(df_t)} transactions from transactions (raw)")
        
        print(f"\n📥 Reading nilai from PostgreSQL...")
        try:
            df_n = pd.read_sql("SELECT * FROM nilai1s_cleaned", conn)
            if len(df_n) == 0:
                raise ValueError("nilai1s_cleaned is empty, trying fallback")
            print(f"   ✅ Found {len(df_n)} nilai records from nilai1s_cleaned")
        except Exception as e:
            print(f"   ⚠️  nilai1s_cleaned issue: {str(e)[:50]}")
            print(f"   🔄 Trying fallback to nilai1s (raw)...")
            df_n = pd.read_sql("SELECT * FROM nilai1s", conn)
            print(f"   ✅ Found {len(df_n)} nilai records from nilai1s (raw)")
        
        print(f"\n📥 Reading master surveys from PostgreSQL...")
        try:
            df_ms = pd.read_sql("SELECT * FROM master_surveys_enriched", conn)
            if len(df_ms) == 0:
                raise ValueError("master_surveys_enriched is empty, trying fallback")
            print(f"   ✅ Found {len(df_ms)} master surveys from master_surveys_enriched")
        except Exception as e:
            print(f"   ⚠️  master_surveys_enriched issue: {str(e)[:50]}")
            print(f"   🔄 Trying fallback to master_surveys (raw)...")
            df_ms = pd.read_sql("SELECT * FROM master_surveys", conn)
            print(f"   ✅ Found {len(df_ms)} master surveys from master_surveys (raw)")
            
            print(f"\n{'='*70}")
            print(f"🏷️  ENRICHING MASTER SURVEYS WITH TYPE")
            print(f"{'='*70}")
            
            def classify_survey_type(name):
                name_lower = str(name).lower()
                
                household_keywords = [
                    'rumah tangga', 'rumah_tangga', 'seruti', 'susenas', 'ssn',
                    'sosial ekonomi nasional', 'ekonomi rumah tangga',
                    'tenaga kerja', 'ketenagakerjaan', 'sakernas', 'sak',
                    'literasi', 'inklusi keuangan', 'snlik',
                    'penduduk', 'supas', 'sensus',
                    'makanan bergizi', 'mbg', 'podes', 'potensi desa',
                    'konsumen', 'shk' 
                ]
                
                company_keywords = [
                    'perusahaan', 'industri', 'ibs', 'imk', 'konstruksi',
                    'perdagangan', 'penjualan', 'eceran', 'perdagangan besar',
                    'pergudangan', 'angkutan', 'transportasi',
                    'hotel', 'akomodasi', 'penghunian kamar',
                    'restoran', 'makanan minuman',
                    'lembaga keuangan', 'slk', 'non profit',
                    'hortikultura', 'sph', 'kehutanan', 'spk', 'peternakan', 'spp',
                    'ternak', 'lptb', 'air bersih', 'spab',
                    'captive power', 'scp', 'non migas', 'snm',
                    'e-commerce', 'pola distribusi', 'spd',
                    'produsen', 'shp', 'shpb'  
                ]
                
                for keyword in household_keywords:
                    if keyword in name_lower:
                        return "Rumah Tangga"
                
                for keyword in company_keywords:
                    if keyword in name_lower:
                        return "Perusahaan"
                
                if any(word in name_lower for word in ['updating', 'wilkerstat', 'ksa', 'kerangka', 'ubinan', 'konversi', 'harga']):
                    return "Perusahaan"
                
                return "Perusahaan"
            
            print(f"\n📋 Classifying {len(df_ms)} master surveys...")
            df_ms['type'] = df_ms['name'].apply(classify_survey_type)
            
            type_counts = df_ms['type'].value_counts()
            print(f"\n📊 Classification results:")
            for survey_type, count in type_counts.items():
                print(f"   - {survey_type}: {count} surveys")
            
            print(f"\n📝 Sample classifications:")
            for _, row in df_ms.head(5).iterrows():
                print(f"   {row['type']:15} | {row['name'][:50]}")
            
            print(f"\n{'='*70}\n")
        
        print(f"\n📥 Reading mitras data...")
        try:
            df_m = pd.read_sql("SELECT * FROM mitra_cleaned", conn)
            if len(df_m) == 0:
                raise ValueError("mitra_cleaned is empty, trying fallback")
            print(f"   ✅ Found {len(df_m)} mitras from mitra_cleaned")
        except Exception as e:
            print(f"   ⚠️  mitra_cleaned issue: {str(e)[:50]}")
            print(f"   🔄 Trying fallback to mitras (raw)...")
            try:
                df_m = pd.read_sql("SELECT * FROM mitras", conn)
                print(f"   ✅ Found {len(df_m)} mitras from mitras (raw)")
            except Exception as e2:
                print(f"   ❌ ERROR: Cannot read mitras table: {str(e2)[:100]}")
                print(f"   🔄 Last resort: trying to read from CSV...")
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
        
        print(f"\n{'='*70}")
        print(f"🧹 CLEANING MITRA NAMES")
        print(f"{'='*70}")
        
        def clean_mitra_name(name):
            r"""
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
        print(f"💾 SAVING CLEANED DATA TO CSV & POSTGRESQL")
        print(f"{'='*70}")
        
        df_m.to_csv(mitra_out_csv, index=False)
        print(f"   ✅ Mitras CSV: {len(df_m)} rows → {mitra_out_csv}")
        
        df_ms.to_csv(master_out_csv, index=False)
        print(f"   ✅ Master Surveys CSV: {len(df_ms)} rows → {master_out_csv}")
        
        df_s.to_csv(survey_out_csv, index=False)
        print(f"   ✅ Surveys CSV: {len(df_s)} rows → {survey_out_csv}")
        
        df_t.to_csv(trans_out_csv, index=False)
        print(f"   ✅ Transactions CSV: {len(df_t)} rows → {trans_out_csv}")
        
        df_n.to_csv(nilai_out_csv, index=False)
        print(f"   ✅ Nilai CSV: {len(df_n)} rows → {nilai_out_csv}")
        
        print(f"\n📤 Inserting cleaned data to PostgreSQL...")

        cursor = conn.cursor()
        
        def bulk_insert_df(df, table_name, cursor):
            import io
            cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            
            columns = []
            for col, dtype in df.dtypes.items():
                if 'int' in str(dtype):
                    col_type = 'BIGINT'  
                elif 'float' in str(dtype):
                    col_type = 'DOUBLE PRECISION'
                elif 'datetime' in str(dtype):
                    col_type = 'TIMESTAMP'
                else:
                    col_type = 'TEXT'
                columns.append(f'"{col}" {col_type}')
            
            create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)});"
            cursor.execute(create_sql)
            
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
            buffer.seek(0)
            
            cursor.copy_from(buffer, table_name, sep='\t', null='\\N')
            print(f"   💾 Inserted {len(df)} rows to {table_name}")
        
        bulk_insert_df(df_m, 'mitras_cleaned', cursor)
        bulk_insert_df(df_ms, 'master_surveys_enriched', cursor)
        bulk_insert_df(df_s, 'surveys_cleaned', cursor)
        bulk_insert_df(df_t, 'transactions_cleaned', cursor)
        bulk_insert_df(df_n, 'nilai1s_cleaned', cursor)
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"   ✅ All cleaned data inserted to PostgreSQL!")

        print(f"\n{'='*70}")
        print(f"✅ PREPROCESS COMPLETED - DATA READY FOR FEATURE ENGINEERING")
        print(f"{'='*70}\n")

    except Exception as e:
        print(f"\n❌ ERROR during preprocessing: {e}")
        raise
    finally:
        try:
            if conn and not conn.closed:
                conn.close()
        except:
            pass

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
