from airflow import DAG
from airflow.operators.bash import BashOperator

import os
import datetime as dt


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime.now(),
    'email': ['grofcik.pavol@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'schedule_interval': '@daily',
}

# Instantiate a DAG object
simpleDagTest = DAG(
        dag_id='simpleDagTest',
		description='Simpe Test DAG',
		schedule=None,
		catchup=False,
		default_args=default_args
)

# Creating 1.st task
bash_task = BashOperator(task_id='bash_task', bash_command='mkdir $AIRFLOW_HOME/bash_task', dag=simpleDagTest)


bash_task