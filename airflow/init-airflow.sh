#!/bin/bash
set -e

echo " Initializing Airflow..."

echo " Waiting for PostgreSQL to be ready..."
max_attempts=30
attempt=0
until PGPASSWORD=$DB_PASS psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" -d postgres -c '\q' 2>/dev/null; do
    attempt=$((attempt + 1))
    if [ $attempt -ge $max_attempts ]; then
        echo " ERROR: PostgreSQL is not available after $max_attempts attempts"
        exit 1
    fi
    echo " PostgreSQL is unavailable - sleeping (attempt $attempt/$max_attempts)"
    sleep 2
done

echo " PostgreSQL is ready!"

echo " Checking if airflow_metadata database exists..."
if ! PGPASSWORD=$DB_PASS psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" -lqt | cut -d \| -f 1 | grep -qw airflow_metadata; then
    echo " Creating airflow_metadata database..."
    PGPASSWORD=$DB_PASS psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" -d postgres -c "CREATE DATABASE airflow_metadata;"
    echo " airflow_metadata database created successfully!"
else
    echo " airflow_metadata database already exists."
fi

echo " Checking if mitra_kaido database exists..."
if ! PGPASSWORD=$DB_PASS psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" -lqt | cut -d \| -f 1 | grep -qw mitra_kaido; then
    echo " Creating mitra_kaido database..."
    PGPASSWORD=$DB_PASS psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" -d postgres -c "CREATE DATABASE mitra_kaido;"
    echo " mitra_kaido database created successfully!"
else
    echo " mitra_kaido database already exists."
fi

echo " Checking if ML tables exist..."
TABLE_COUNT=$(PGPASSWORD=$DB_PASS psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" -d mitra_kaido -tAc "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public';")
if [ "$TABLE_COUNT" -eq "0" ]; then
    echo " Creating ML tables from init-ml-tables.sql..."
    PGPASSWORD=$DB_PASS psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" -d mitra_kaido -f /opt/airflow/sql/init-ml-tables.sql
    echo " ML tables created successfully!"
else
    echo " ML tables already exist (found $TABLE_COUNT tables)."
fi

echo " Running database initialization/migration..."
airflow db init || airflow db migrate

echo " Creating default admin user..."
echo "   Username: admin"
echo "   Password: admin"
echo "   Email: admin@simitra.com"
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@simitra.com 2>/dev/null || echo "ℹ  Admin user already exists"

echo ""
echo " Airflow initialization complete!"
echo ""
echo " Webserver: http://localhost:8080"
echo " Username: admin"
echo " Password: admin"
echo ""
echo ""

echo " Starting Airflow Webserver..."
airflow webserver &

echo " Starting Airflow Scheduler..."
airflow scheduler