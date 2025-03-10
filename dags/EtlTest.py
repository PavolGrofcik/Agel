import logging
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow.models.dag import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def t1(**kwargs):
    # raise Exception("This exception is thrown")
    #return 5
    ls = ['a', 'b', 'c']
    return ls

def t2(**kwargs):
    ti = kwargs['ti']
    ls = ti.xcom_pull(task_ids='ETL_T1')
    print(f'Pulled value from xcom is: {ls}')


########################################################
# DAG ETL TEST
########################################################
with DAG(
    "EtlTest",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
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
    description="A simple ETL test DAG",
    schedule=None,
    start_date=datetime(2025, 3, 5),
    catchup=False,
    tags=["ETL TEST"],
) as dag:


    T1 = PythonOperator(task_id="ETL_T1", python_callable=t1)
    T2 = PythonOperator(task_id="ETL_T2", python_callable=t2)
    #1. Run Python file to write data to a file
    #T1 = BashOperator(task_id="E1", bash_command="python3 $HOME/Plocha/DockerTest/script.py ", retries=0)

    # 2. Run Python file to append a file
    #T2 = BashOperator(task_id="E2", bash_command="python3 $HOME/Plocha/DockerTest/script2.py ", retries=0)


########################
# Task Flow
#######################
T1 >> T2