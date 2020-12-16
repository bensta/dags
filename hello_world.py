
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime, timedelta
import logging
import os

def train_model():
    print("hello world")

with DAG('hello_world', start_date=datetime(2016, 1, 1)) as dag:
    t1 = PythonOperator( 
        task_id='train',
        python_callable=train_model,
        dag=dag)
