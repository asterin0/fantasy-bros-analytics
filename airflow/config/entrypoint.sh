#!/usr/bin/env bash

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${AIRFLOW_POSTGRES_USER}:${AIRFLOW_POSTGRES_PASSWORD}@${AIRFLOW_POSTGRES_HOST}:${AIRFLOW_POSTGRES_PORT}/${AIRFLOW_POSTGRES_DB}"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN


# Initialize airflow db and migrate to Postgres
airflow db migrate

# Create airflow user
airflow users create \
   --username ${AIRFLOW_POSTGRES_USER} \
   --password ${AIRFLOW_POSTGRES_PASSWORD} \
   --firstname airflow \
   --lastname airflow \
   --role Admin \
   --email airflow@example.com

# Kick off airflow scheduler and webserver services
airflow scheduler &

airflow webserver