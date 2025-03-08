#! /bin/bash


mkdir /root/airflow/dags
cp ./dags/* /root/airflow/dags/



#echo "##############################"
#echo "Initialazation of dirs structure starting..."
#echo "##############################"
#
##Dir structure
#mkdir -p ./Agel/data ./Agel/scripts ./Agel/logs
#
#cd ./Agel/scripts
#touch data_ingestion.py data_validation.py data_transformation.py data_saving.py
#chmod -R 711 ./data_*.py
#
#
#echo "##############################"
#echo "Initialization successful!"
#echo "##############################"
