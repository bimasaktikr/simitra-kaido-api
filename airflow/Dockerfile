FROM apache/airflow:2.10.2

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    dos2unix \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY airflow/init-airflow.sh /opt/airflow/init-airflow.sh
COPY sql/init-ml-tables.sql /opt/airflow/sql/init-ml-tables.sql
RUN dos2unix /opt/airflow/init-airflow.sh && \
    chmod +x /opt/airflow/init-airflow.sh && \
    chown airflow:root /opt/airflow/init-airflow.sh

USER airflow

COPY airflow/requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

COPY airflow/dags /opt/airflow/dags
COPY pipeline /opt/airflow/pipeline

COPY airflow/webserver_config.py /opt/airflow/webserver_config.py

USER root
RUN mkdir -p /opt/airflow/data/raw /opt/airflow/data/processed /opt/airflow/data/reports && \
    chown -R airflow:root /opt/airflow/data && \
    chmod -R 777 /opt/airflow/data

# Create entrypoint script to fix permissions on startup
RUN cat > /entrypoint-fix-permissions.sh << 'EOF'
#!/bin/bash
set -e

echo "🔧 Fixing data directory permissions on startup..."

# Fix permissions for all data directories
chmod -R 777 /opt/airflow/data 2>/dev/null || true
chown -R airflow:root /opt/airflow/data 2>/dev/null || true

# Ensure raw directory is writable
if [ -d "/opt/airflow/data/raw" ]; then
    chmod 777 /opt/airflow/data/raw 2>/dev/null || true
    echo "   ✅ Permissions fixed"
else
    echo "   ⚠️  /opt/airflow/data/raw not found"
fi

# Execute the original command as airflow user
exec gosu airflow "$@"
EOF

RUN chmod +x /entrypoint-fix-permissions.sh && \
    chown root:root /entrypoint-fix-permissions.sh && \
    apt-get update && \
    apt-get install -y --no-install-recommends gosu && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

WORKDIR /opt/airflow

# Use custom entrypoint
ENTRYPOINT ["/entrypoint-fix-permissions.sh"]
CMD ["airflow", "webserver"]
