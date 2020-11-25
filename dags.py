from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import logging
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def sample_python_function(**kwargs):
    for k, v in os.environ.items():
        print(f'{k}={v}')

    
dag = DAG('example_python_operator',
            max_active_runs=3,
            catchup=True,
            schedule_interval='@daily',
            default_args=default_args)

with dag:

    start = DummyOperator(dag=dag, task_id='start')

    t1 = PythonOperator( 
            task_id='sample_python_function',
            python_callable=sample_python_function,
            dag=dag)

    start >> t1
