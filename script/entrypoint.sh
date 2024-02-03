#!/bin/bash
set -e

REQUIREMENTS_FILE="/opt/airflow/requirements.txt"

if [ -f "$REQUIREMENTS_FILE" ]; then
    pip install --user -r "$REQUIREMENTS_FILE"
else
    echo "ERROR: Could not find or open requirements file: $REQUIREMENTS_FILE"
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
    airflow db init && \
    airflow users create \
        --username admin \
        --firstname admin \
        --lastname admin \
        --role Admin \
        --email admin@example.com \
        --password admin
fi

airflow db upgrade

exec airflow webserver
