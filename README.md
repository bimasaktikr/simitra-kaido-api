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
    ├── docker-compose.yml
    ├── README.md
    └── requirements.txt

## **Prerequisites:**

- Docker Desktop running
- Portainer installed (http://localhost:9443)
- Git (untuk clone repository)

## 🚀 **Quick Start**

1. **Clone Repository**

   ```bash
   git clone https://github.com/QwAct225/simitra-kaido-api.git
   cd simitra-kaido-api
   ```

2. **Copy Environment File**

   ```bash
   cp .env.example .env
   ```

3. **Start Docker Containers**

   ```bash
   docker-compose up -d --build
   ```

   **📊 Database akan otomatis dibuat:**

   - `airflow_metadata` - Airflow internal database (WAJIB!)
   - `mitra_kaido` - ML results database

4. **Tunggu ~30 detik untuk initialization**

5. **Verifikasi services running:**

   ```bash
   docker-compose ps
   ```

   Expected:

   - ✅ `simitra_postgres` - Up (healthy)
   - ✅ `simitra_airflow` - Up (port 8080)
   - ✅ `simitra_api` - Up (port 8001)

6. **Access services:**
   - Airflow UI: http://localhost:8080
   - API Docs: http://localhost:8001/docs

---

## **🎯 Deploy to Portainer**

1. **Login to Portainer:** http://localhost:9443
2. **Go to:** Stacks → Add stack
3. **Stack name:** `simitra-kaido-api`
4. **Build method:** Web editor
5. **Copy & Paste** entire content dari file `docker-compose.portainer.yml`
6. **Environment variables** (optional - gunakan default jika tidak diisi):
   ```env
   DB_NAME=mitra_kaido
   DB_USER=postgres
   DB_PASS=mitra123
   POSTGRES_PORT=5432
   AIRFLOW_PORT=8080
   API_PORT=8001
   ```
7. **Click:** "Deploy the stack"
8. **Wait:** 3-5 minutes untuk initialization

### **Verify Deployment**

**Check Containers Status:**

- Portainer → Containers → All should be "healthy" (green)

**Verify DAG Status (No Import Errors):**

```bash
docker exec simitra_airflow airflow dags list
# Expected: master_mitra_survey | /opt/airflow/dags/etl_mitra_survey.py | airflow | False

docker exec simitra_airflow airflow dags list-import-errors
# Expected: No data found (✅ means no errors)
```

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
🐳 qwact/simitra-api:latest        (FastAPI + Routers + Services)
🐳 postgres:17                      (PostgreSQL Database)
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

#### **1. Container Unhealthy**

```bash
# Check logs
docker logs simitra_airflow --tail 50
docker logs simitra_api --tail 50

# Restart container
docker restart simitra_airflow
docker restart simitra_api
```

#### **2. DAG Import Errors**

- **Error:** `ModuleNotFoundError: No module named 'pipeline'`
- **Solution:** Pastikan menggunakan image `qwact/simitra-airflow:latest` yang sudah include PYTHONPATH fix

#### **3. Port Already in Use**

```bash
# Check what's using the port
netstat -ano | findstr :8080  # Windows
lsof -i :8080                  # macOS/Linux

# Change port in .env
AIRFLOW_PORT=8081
API_PORT=8002
```

#### **4. Database Connection Failed**

- **Check:** Password di `.env` harus sama dengan yang di volume PostgreSQL
- **Solution:** Jika password berubah, hapus volume dan recreate:
  ```bash
  docker volume rm simitra-kaido-api_postgres_data
  docker-compose up -d
  ```

---

## 📊 **API Endpoints**

### **FastAPI Documentation:**

- Swagger UI: http://localhost:8001/docs
- ReDoc: http://localhost:8001/redoc

### **Available Endpoints:**

**Health Check:**

```bash
GET /health
```

**Mitra Recommendations:**

```bash
GET /recommendations/mitra?limit=10
```

**Survey Master Data:**

```bash
GET /master-survey
```

**Webhook (Trigger Airflow DAG):**

```bash
POST /webhook/trigger-dag
```

---

## 🛠️ **Local Development**

### **Option 1: Docker Compose (Recommended)**

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f airflow

# Stop services
docker-compose down
```

### **Option 2: Local Python (Advanced)**

```bash
# Setup virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r airflow/requirements.txt
pip install -r api/requirements.txt

# Run API locally
cd api
uvicorn main:app --reload --port 8001
```

---

## 📝 **Environment Variables**

Create `.env` file (copy from `.env.example`):

```env
# PostgreSQL
DB_NAME=mitra_kaido
DB_USER=postgres
DB_PASS=mitra123
POSTGRES_PORT=5432

# Laravel MySQL (External)
LARAVEL_DB_HOST=host.docker.internal
LARAVEL_DB_PORT=3306
LARAVEL_DB_NAME=kaido_kit
LARAVEL_DB_USER=root
LARAVEL_DB_PASS=
LARAVEL_API_URL=http://host.docker.internal:8000

# Airflow
AIRFLOW_PORT=8080
AIRFLOW_SECRET_KEY=your-secret-key

# API
API_PORT=8001
```

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
