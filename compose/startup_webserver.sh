#! /bin/bash

echo "##############################"
echo "Initialization started!"
echo "##############################"

#pip3 install apache-airflow-providers-postgres

#airflow db init
#airflow users create --username admin --password admin --firstname test  --lastname test --role Admin --email test@test.org

airflow webserver

echo "##############################"
echo "Initialization successful!"
echo "##############################"