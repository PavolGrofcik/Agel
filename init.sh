#! /bin/bash

echo "##############################"
echo "Initialazation starting..."
echo "##############################"

pip3 install pyarrow 
python3 -m pip install "dask[distributed]"
pip3 install -r ./requirements.txt

airflow db init
airflow users create --username admin --password admin --firstname test  --lastname test --role Admin --email test@test.org
apt update -y

mkdir /root/airflow/dags
cp ./dags/* /root/airflow/dags/

echo "##############################"
echo "Initialization successful!"
echo "##############################"