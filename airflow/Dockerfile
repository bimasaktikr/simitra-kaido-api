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
    chown -R airflow:root /opt/airflow/data
USER airflow

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

WORKDIR /opt/airflow
