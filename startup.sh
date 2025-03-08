#! /bin/bash


echo "##############################"
echo "Initialization started!"
echo "##############################"
export AGEL_DIR=/Agel
export AIRFLOW_DIR=/root/airflow

airflow scheduler -D
airflow webserver -p 8080 -D
python3 $AIRFLOW_DIR/dags/test.py


echo "##############################"
echo "Initialization successful!"
echo "##############################"