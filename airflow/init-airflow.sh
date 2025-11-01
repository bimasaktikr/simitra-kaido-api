#!/bin/bash
set -e

echo " Initializing Airflow..."

# Initialize or migrate database
echo " Running database initialization/migration..."
airflow db init || airflow db migrate

# Create default admin user for easy access
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

# Start services
echo " Starting Airflow Webserver..."
airflow webserver &

echo " Starting Airflow Scheduler..."
airflow scheduler