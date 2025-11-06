import os, re, csv
import psycopg2
from dotenv import load_dotenv, find_dotenv

def run_ingest(base_dir: str):
    load_dotenv(find_dotenv(), override=True)
    
    raw_dir = os.path.join(base_dir, "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)

    sql_candidates = [f for f in os.listdir(raw_dir) if f.endswith(".sql")]
    if not sql_candidates:
        raise FileNotFoundError(f"Tidak ada file .sql di {raw_dir}")
    sql_file = os.path.join(raw_dir, sql_candidates[0])

    with open(sql_file, "r", encoding="utf-8") as f:
        content = f.read()

    def extract(table, headers, out_name):
        pattern = re.compile(rf"INSERT INTO `{table}` .*?VALUES\s*(.*?);", re.DOTALL)
        matches = pattern.findall(content)
        rows = []
        for match in matches:
            tuples = re.findall(r"\((.*?)\)(?=\s*,|\s*;|$)", match, re.DOTALL)
            for t in tuples:
                vals = []
                current = []
                in_quote = False
                for char in t + ',':
                    if char == "'" and (not current or current[-1] != '\\'):
                        in_quote = not in_quote
                    elif char == ',' and not in_quote:
                        val = ''.join(current).strip().strip("'")
                        vals.append(val if val != 'NULL' else '')
                        current = []
                    else:
                        current.append(char)
                
                if len(vals) == len(headers):
                    rows.append(vals)

        out_csv = os.path.join(raw_dir, out_name)
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, quoting=csv.QUOTE_ALL)
            w.writerow(headers)
            w.writerows(rows)

        return out_csv, len(rows)

    mitra_headers = [
        "id","sobat_id","name","user_id","email","pendidikan",
        "jenis_kelamin","tanggal_lahir","photo","created_at","updated_at"
    ]
    master_survey_headers = ["id","name","code","created_at","updated_at"]
    survey_headers = [
        "id","master_survey_id","triwulan","year","payment_month","payment_id",
        "team_id","rate","file","is_scored","is_synced","status","created_at","updated_at"
    ]
    transaction_headers = [
        "id","mitra_id","survey_id","target","rate","created_at","updated_at"
    ]
    nilai_headers = [
        "transaction_id","aspek1","aspek2","aspek3","rerata","created_at","updated_at"
    ]

    mitra_csv, n_mitra = extract("mitras", mitra_headers, "raw_mitras.csv")
    master_csv, n_master = extract("master_surveys", master_survey_headers, "raw_master_surveys.csv")
    survey_csv, n_survey = extract("surveys", survey_headers, "raw_surveys.csv")
    trans_csv, n_trans = extract("transactions", transaction_headers, "raw_transactions.csv")
    nilai_csv, n_nilai = extract("nilai1s", nilai_headers, "raw_nilai1s.csv")

    print(f"✅ Ingested {n_mitra} mitra, {n_master} master surveys, {n_survey} surveys, {n_trans} transactions, {n_nilai} nilai1s.")

    print(f"\n{'='*70}")
    print(f"📤 INSERTING DATA TO POSTGRESQL")
    print(f"{'='*70}")
    
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
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    def clean_value(val):
        """Convert empty string to None for SQL NULL"""
        return None if val == '' or val == 'NULL' else val
    
    try:
        print(f"\n📋 Creating tables if not exist...")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mitras (
                id INTEGER PRIMARY KEY,
                sobat_id VARCHAR(255),
                name VARCHAR(255),
                user_id INTEGER,
                email VARCHAR(255),
                pendidikan VARCHAR(255),
                jenis_kelamin VARCHAR(50),
                tanggal_lahir DATE,
                photo TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS master_surveys (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255),
                code VARCHAR(50),
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS surveys (
                id INTEGER PRIMARY KEY,
                master_survey_id INTEGER,
                triwulan INTEGER,
                year INTEGER,
                payment_month INTEGER,
                payment_id INTEGER,
                team_id INTEGER,
                rate DECIMAL,
                file TEXT,
                is_scored INTEGER,
                is_synced INTEGER,
                status VARCHAR(50),
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY,
                mitra_id INTEGER,
                survey_id INTEGER,
                target INTEGER,
                rate DECIMAL,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nilai1s (
                transaction_id INTEGER PRIMARY KEY,
                aspek1 DECIMAL,
                aspek2 DECIMAL,
                aspek3 DECIMAL,
                rerata DECIMAL,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)
        
        print(f"   ✅ Tables created successfully")
        
        print(f"\n📥 Inserting {n_mitra} mitras...")
        cursor.execute("DELETE FROM mitras")
        with open(mitra_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute("""
                    INSERT INTO mitras (id, sobat_id, name, user_id, email, pendidikan, 
                                       jenis_kelamin, tanggal_lahir, photo, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (clean_value(row['id']), clean_value(row['sobat_id']), clean_value(row['name']), 
                      clean_value(row['user_id']), clean_value(row['email']), clean_value(row['pendidikan']), 
                      clean_value(row['jenis_kelamin']), clean_value(row['tanggal_lahir']), 
                      clean_value(row['photo']), clean_value(row['created_at']), clean_value(row['updated_at'])))
        print(f"   ✅ Inserted {n_mitra} mitras")
        
        print(f"\n📥 Inserting {n_master} master surveys...")
        cursor.execute("DELETE FROM master_surveys")
        with open(master_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute("""
                    INSERT INTO master_surveys (id, name, code, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (clean_value(row['id']), clean_value(row['name']), clean_value(row['code']), 
                      clean_value(row['created_at']), clean_value(row['updated_at'])))
        print(f"   ✅ Inserted {n_master} master surveys")
        
        print(f"\n📥 Inserting {n_survey} surveys...")
        cursor.execute("DELETE FROM surveys")
        with open(survey_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute("""
                    INSERT INTO surveys (id, master_survey_id, triwulan, year, payment_month, 
                                        payment_id, team_id, rate, file, is_scored, is_synced, 
                                        status, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (clean_value(row['id']), clean_value(row['master_survey_id']), 
                      clean_value(row['triwulan']), clean_value(row['year']), 
                      clean_value(row['payment_month']), clean_value(row['payment_id']), 
                      clean_value(row['team_id']), clean_value(row['rate']), 
                      clean_value(row['file']), clean_value(row['is_scored']), 
                      clean_value(row['is_synced']), clean_value(row['status']), 
                      clean_value(row['created_at']), clean_value(row['updated_at'])))
        print(f"   ✅ Inserted {n_survey} surveys")
        
        print(f"\n📥 Inserting {n_trans} transactions...")
        cursor.execute("DELETE FROM transactions")
        with open(trans_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute("""
                    INSERT INTO transactions (id, mitra_id, survey_id, target, rate, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (clean_value(row['id']), clean_value(row['mitra_id']), clean_value(row['survey_id']), 
                      clean_value(row['target']), clean_value(row['rate']), 
                      clean_value(row['created_at']), clean_value(row['updated_at'])))
        print(f"   ✅ Inserted {n_trans} transactions")
        
        print(f"\n📥 Inserting {n_nilai} nilai1s...")
        cursor.execute("DELETE FROM nilai1s")
        with open(nilai_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute("""
                    INSERT INTO nilai1s (transaction_id, aspek1, aspek2, aspek3, rerata, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (clean_value(row['transaction_id']), clean_value(row['aspek1']), 
                      clean_value(row['aspek2']), clean_value(row['aspek3']), 
                      clean_value(row['rerata']), clean_value(row['created_at']), 
                      clean_value(row['updated_at'])))
        print(f"   ✅ Inserted {n_nilai} nilai1s")
        
        conn.commit()
        print(f"\n✅ ALL DATA COMMITTED TO POSTGRESQL")
        
    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error inserting to PostgreSQL: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

    return {
        "mitra_csv": mitra_csv,
        "master_csv": master_csv,
        "survey_csv": survey_csv,
        "transaction_csv": trans_csv,
        "nilai_csv": nilai_csv,
        "db_inserted": True
    }
