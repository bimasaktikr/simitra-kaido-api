# 🚀 SIMITRA KAIDO - ML Backend API

Machine Learning Backend Service untuk Mitra Ranking & Survey Aggregation dengan Apache Airflow dan FastAPI. This ML backend is responsible for automatically managing, processing, and calculating partner ranking results and survey performance. The system is designed with a separate architecture for partner profiles (ML pipeline) and survey results (aggregator pipeline) to make the process more modular, efficient, and easy to maintain.

## 🧱 **Folder Structure**

    simitra-kaido-api/
    ├── airflow/
    │   ├── dags/
    │   │   └── etl_mitra_survey.py
    │   │
    │   ├── operators/
    │   │   ├── featiure_engineering_operator.py
    │   │   ├── ingest_operator.py
    │   │   ├── preprocess_operator.py
    │   │   ├── ranking_mitra_operator.py
    │   │   └── weight_optimizer_operator.py
    │   │
    │   ├── Dockerfile
    │   ├── init-airflow.sh
    │   ├── requirements.txt
    │   └── webserver_config.py
    │
    ├── api/
    │   ├── routers/
    │   │   ├── __init__.py
    │   │   ├── master_survey_router.py
    │   │   ├── mitra_router.py
    │   │   ├── recommendation_router.py
    │   │   ├── sync_router.py
    │   │   └── webhook_router.py
    │   │
    │   ├── services/
    │   │   ├── __init__.py
    │   │   └── database_service.py
    │   │
    │   ├── Dockerfile
    │   ├── main.py
    │   └── requirements.txt
    │
    ├── data/
    │   ├── processed/
    │   │   ├── cleaned_master_surveys.csv
    │   │   ├── cleaned_mitras.csv
    │   │   ├── cleaned_nilai1s.csv
    │   │   ├── cleaned_surveys.csv
    │   │   ├── cleaned_transactions.csv
    │   │   └── features_mitra_survey.csv
    │   ├── raw/
    │   │   ├── raw_master_surveys.csv
    │   │   ├── raw_mitras.csv
    │   │   ├── raw_nilai1s.csv
    │   │   ├── raw_surveys.csv
    │   │   └── raw_transactions.csv
    │   └── reports/
    │       ├── cbf_ranked_mitra.csv
    │       ├── pso_optimized_mitra.csv
    │       ├── pso_optimized_perusahaan.csv
    │       ├── pso_optimized_rumah_tangga.csv
    │       ├── recommendation_perusahaan.csv
    │       └── recommendations_rumah_tangga.csv
    │
    ├── pipeline/
    │   ├── __init__.py
    │   ├── run_feature_engineering.py
    │   ├── run_ingest.py
    │   ├── run_preprocess.py
    │   ├── run_ranking_mitra.py
    │   ├── run_experience_aggregation.py
    │   └── run_weight_optimizer.py
    │
    ├── sql/
    │   ├── init-databases.sql
    │   └── init-ml-tables.sql
    │
    ├── .env.example
    ├── .gitattributes
    ├── .gitignore
    ├── docker-compose.db.yml      # PostgreSQL database stack
    ├── docker-compose.ml.yml      # Airflow + FastAPI stack
    ├── docker-compose.yml         # Legacy single-stack compose
    ├── README.md
    └── requirements.txt

## **Prerequisites:**

- Docker Desktop running
- Portainer installed (http://localhost:9443)
- Git (untuk clone repository)

## 🚀 **Quick Start**

1. **Clone Repository**

   ```bash
   git clone https://github.com/bimasaktikr/simitra-kaido-api.git
   cd simitra-kaido-api
   ```

2. **Copy Environment File**

   ```bash
   cp .env.example .env
   ```

3. **Start Docker Containers (Isolated Stacks)**

   **⚡ Recommended Approach - Separate Stacks for Fault Isolation:**

   ```bash
   # Step 1: Start Database Stack (Critical Infrastructure)
   docker compose -f docker-compose.db.yml -p simitra-db up -d
   
   # Step 2: Wait for database to be ready (~10 seconds)
   docker logs simitra_postgres -f  # Press Ctrl+C after "ready to accept connections"
   
   # Step 3: Start ML Stack (Application Layer)
   docker compose -f docker-compose.ml.yml -p simitra-ml up -d
   ```

   **📊 Database akan otomatis dibuat:**

   - `airflow_metadata` - Airflow internal database (WAJIB!)
   - `mitra_kaido` - ML results database

   **🎯 Why Separate Stacks?**
   - ✅ **Fault Isolation**: Database remains accessible if ML stack crashes
   - ✅ **Independent Restart**: Update/restart ML services without database downtime
   - ✅ **Data Safety**: Database volume protected during ML troubleshooting
   - ✅ **Resource Management**: Scale ML resources independently

4. **Tunggu ~30 detik untuk initialization**

5. **Verifikasi services running:**

   ```bash
   docker ps --filter "name=simitra"
   ```

   Expected:

   - ✅ `simitra_postgres` - Up (healthy) - **Stack: simitra-db**
   - ✅ `simitra_airflow` - Up (port 8080) - **Stack: simitra-ml**
   - ✅ `simitra_api` - Up (port 8001) - **Stack: simitra-ml**

6. **Access services:**
   - Airflow UI: http://localhost:8080
   - API Docs: http://localhost:8001/docs
   - PostgreSQL: localhost:5432

7. **Managing Stacks:**

   ```bash
   # Restart only ML stack (database stays up)
   docker compose -f docker-compose.ml.yml -p simitra-ml restart
   
   # Stop ML stack (database stays up)
   docker compose -f docker-compose.ml.yml -p simitra-ml down
   
   # Stop all stacks
   docker compose -f docker-compose.db.yml -p simitra-db down
   docker compose -f docker-compose.ml.yml -p simitra-ml down
   ```

---

## **🎯 Deploy to Portainer (Alternative Method)**

> **Note:** Command-line deployment with `-p` flag (explained above) is **recommended** for better control and isolation. Use Portainer UI only if you don't have SSH access to the server.

### **Method 1: Via Portainer UI (2 Separate Stacks)**

**Stack 1 - Database (Critical Infrastructure):**
1. **Login to Portainer:** http://localhost:9443
2. **Go to:** Stacks → Add stack
3. **Stack name:** `simitra-db`
4. **Build method:** Web editor
5. **Copy & Paste** content dari `docker-compose.db.yml`
   - ⚠️ **Important:** Comment out or remove local file volume mounts (lines with `./sql/...`)
   - Use named volumes or manual database initialization instead
6. **Deploy** and wait for database to be healthy

**Stack 2 - ML Services (Application Layer):**
1. **Go to:** Stacks → Add stack
2. **Stack name:** `simitra-ml`
3. **Build method:** Web editor
4. **Copy & Paste** content dari `docker-compose.ml.yml`
   - ⚠️ **Important:** Comment out `./airflow/dags` and `./data/raw` volume mounts
   - DAG changes require image rebuild or use git pull + image update
5. **Environment variables** (optional):
   ```env
   DB_NAME=mitra_kaido
   DB_USER=postgres
   DB_PASS=mitra123
   LARAVEL_API_URL=http://host.docker.internal:8000
   AIRFLOW_PORT=8080
   API_PORT=8001
   ```
6. **Deploy** and wait for services to be healthy

### **Method 2: Via Command Line (Recommended)**

If you have SSH access to the server:

```bash
# Pull latest code
cd /path/to/simitra-kaido-api
git pull origin main

# Deploy with separate stacks
docker compose -f docker-compose.db.yml -p simitra-db up -d
docker compose -f docker-compose.ml.yml -p simitra-ml up -d
```

**Advantages:**
- ✅ No volume mounting issues
- ✅ Easier code updates (just `git pull` + restart)
- ✅ Full control over stack lifecycle
- ✅ Better for development workflow

### **Verify Deployment**

**Access Services:**

```
✅ Airflow UI:  http://localhost:8080
✅ API Docs:    http://localhost:8001/docs
✅ PostgreSQL:  localhost:5432
```

**Test API:**

```bash
curl http://localhost:8001/docs
curl http://localhost:8001/health
```

**Test Airflow:**

- Open http://localhost:8080 (no login required)
- DAG `etl_mitra_survey` should appear
- Click DAG → Trigger DAG to run pipeline

---

## 📦 **Docker Images**

Pre-built images available on Docker Hub:

```
🐳 qwact/simitra-airflow:latest   (Apache Airflow + DAGs + Pipeline)
🐳 qwact/simitra-api:latest       (FastAPI + Routers + Services)
🐳 postgres:17                    (PostgreSQL Database)
```

---

## 🔄 **Update Code (For Developers)**

When you make changes to DAGs or API code:

```bash
# 1. Edit your code
# 2. Rebuild images
docker build -f airflow/Dockerfile -t qwact/simitra-airflow:latest .
docker build -f api/Dockerfile -t qwact/simitra-api:latest .

# 3. Push to Docker Hub
docker push qwact/simitra-airflow:latest
docker push qwact/simitra-api:latest

# 4. In Portainer: Pull and redeploy stack
```

---

## 🔧 **Troubleshooting**

### **Common Issues:**

#### **1. Database "airflow_metadata" does not exist**

**Quick Fix:**

```bash
# Option 1: Clean installation (removes all data)
docker-compose -f docker-compose.portainer.yml down
docker volume rm simitra-kaido-api_postgres_data
docker-compose -f docker-compose.portainer.yml up -d

# Option 2: Manual database creation (keeps data)
docker exec -it simitra_postgres psql -U postgres -c "CREATE DATABASE airflow_metadata;"
docker exec -it simitra_postgres psql -U postgres -c "CREATE DATABASE mitra_kaido;"
```

**Solution:** Versi terbaru sudah include automatic database creation. Update dengan:

```bash
docker-compose -f docker-compose.portainer.yml pull
docker-compose -f docker-compose.portainer.yml up -d
```

#### **2. Container Unhealthy**

```bash
# Check logs
docker logs simitra_airflow --tail 50
docker logs simitra_api --tail 50

# Restart container
docker restart simitra_airflow
docker restart simitra_api
```

#### **3. DAG Import Errors**

- **Error:** `ModuleNotFoundError: No module named 'pipeline'`
- **Solution:** Pastikan menggunakan image `qwact/simitra-airflow:latest` yang sudah include PYTHONPATH fix

#### **4. Port Already in Use**

```bash
# Check what's using the port
netstat -ano | findstr :8080  # Windows
lsof -i :8080                  # macOS/Linux

# Change port in .env
AIRFLOW_PORT=8081
API_PORT=8002
```

#### **5. Database Connection Failed / MD5 Authentication Error**

- **Check:** Password di `.env` harus sama dengan yang di volume PostgreSQL

---

## � **Security Notes**

- **Default Airflow:** No authentication (disable for development only)
- **Production:** Enable authentication in `docker-compose.portainer.yml`
- **Database:** Change default passwords in `.env`
- **API:** Add authentication middleware for production

---

Jika Anda tidak dapat mengakses PostgreSQL karena port tertahan oleh service lokal atau tidak bisa menjalankan `docker compose down -v`, coba langkah berikut di Windows (jalankan terminal sebagai Administrator):

1. Hentikan service PostgreSQL lokal (contoh nama service: postgresql-x64-17)

```powershell
net stop postgresql-x64-17
# atau di PowerShell sebagai alternatif:
Stop-Service -Name postgresql-x64-17 -Force

# gunakan Linux environtment untuk mematikan mendalam
sudo service postgresql stop

sudo systemctl disable postgres
```

2. Periksa apakah port 5432 masih digunakan oleh proses lain

```powershell
netstat -ano | findstr 5432
# hasil menampilkan PID proses yang memakai port 5432; gunakan Task Manager atau
# `taskkill /PID <pid> /F` untuk menghentikannya bila perlu
```

3. Lakukan clean up docker postgres volume

```
docker-compose down

docker volume rm simitra-kaido-api_postgres_data
```

4. Setelah memastikan port 5432 bebas, jalankan kembali Docker Compose

```powershell
docker-compose up -d
```

Catatan: perintah `net stop`/`Stop-Service` akan menghentikan service PostgreSQL yang di-install secara native pada Windows. Gunakan pendekatan ini hanya jika Anda memang menjalankan instance Postgres lokal yang mengganggu kontainer Docker.
