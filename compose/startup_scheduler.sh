#! /bin/bash

echo "##############################"
echo "Initialization started!"
echo "##############################"


mkdir -p $AIRFLOW_HOME/dags $AIRFLOW_HOME/logs/scheduler
cp ./dags/* $AIRFLOW_HOME/dags/
#cp ./compose/airflow.cfg $HOME/airflow

#Just run test.py DAG
python3 $HOME/airflow/dags/agel_etl.py

airflow scheduler

echo "##############################"
echo "Initialization successful!"
echo "##############################"