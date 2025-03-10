########################################################
# Imports
########################################################
import logging
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow.models.dag import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.models import Variable



########################################################
# DAG ETL TEST
########################################################
with DAG(
    "AgelETL",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False, #Set True to send ETL notification
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="A simple ETL job for Agel assignment",
    schedule=None, #Manually triggered DAG ETL Job
    start_date=datetime(2025, 3, 5),
    catchup=False,
    tags=["AGEL ETL ASSIGNMENT"],
) as dag:


    ########################
    # Define ETL Tasks
    #######################

    #1. Run Python file to read data
    T1 = BashOperator(task_id="ETL_T1", bash_command="python3 $HOME/{{ var.value.AGEL_DIR }}/scripts/data_ingestion.py ", retries=0)

    # 2. Run Python file to validate data
    T2 = BashOperator(task_id="ETL_T2", bash_command="python3 $HOME/{{ var.value.AGEL_DIR }}/scripts/data_transformation.py ", retries=0)

    # 3. Run Python file to transform data
    T3 = BashOperator(task_id="ETL_T3", bash_command="python3 $HOME/{{ var.value.AGEL_DIR }}/scripts/data_validation.py ", retries=0)

    # 4. Run Python file to save finally transformed dataset
    T4 = BashOperator(task_id="ETL_T4", bash_command="python3 $HOME/{{ var.value.AGEL_DIR }}/scripts/data_saving.py ", retries=0)


########################
# Task Flow
#######################
T1 >> T2 >> T3 >> T4