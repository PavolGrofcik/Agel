#! /bin/bash

echo "##############################"
echo "Initialization started!"
echo "##############################"

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@localhost:5432/airflow_db?options=-csearch_path%3Dairflow"
export AIRFLOW__DATABASE__SQL_ALCHEMY_SCHEMA="airflow"

airflow db migrate
airflow users create --username admin --password admin --firstname test  --lastname test --role Admin --email test@test.org

airflow scheduler

echo "##############################"
echo "Initialization successful!"
echo "##############################"