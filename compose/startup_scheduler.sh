#! /bin/bash

echo "##############################"
echo "Initialization started!"
echo "##############################"


#airflow db migrate
#airflow users create --username admin --password admin --firstname test  --lastname test --role Admin --email test@test.org

mkdir -p /root/airflow/dags /root/airflow/logs/scheduler
cp ./compose/dags/* /root/airflow/dags/
cp ./compose/airflow.cfg /root/airflow

python3 /root/airflow/dags/test.py

airflow scheduler

echo "##############################"
echo "Initialization successful!"
echo "##############################"