FROM apache/airflow:2.10.2

USER root

# Install system dependencies including dos2unix for line ending conversion
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    dos2unix \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy initialization script and fix line endings FIRST
COPY airflow/init-airflow.sh /opt/airflow/init-airflow.sh
RUN dos2unix /opt/airflow/init-airflow.sh && \
    chmod +x /opt/airflow/init-airflow.sh && \
    chown airflow:root /opt/airflow/init-airflow.sh

USER airflow

# Copy and install Python requirements
COPY airflow/requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy DAGs and pipeline code
COPY airflow/dags /opt/airflow/dags
COPY pipeline /opt/airflow/pipeline

# Copy webserver config for no-authentication
COPY airflow/webserver_config.py /opt/airflow/webserver_config.py

# Create data directories for ML pipeline outputs
USER root
RUN mkdir -p /opt/airflow/data/raw /opt/airflow/data/processed /opt/airflow/data/reports && \
    chown -R airflow:root /opt/airflow/data
USER airflow

# Set PYTHONPATH so pipeline module can be imported
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

# Set working directory
WORKDIR /opt/airflow
