from airflow.hooks.postgres_hook import PostgresHook

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from datetime import datetime, timedelta
from dateutil.parser import parse

from dependencies.gcs_clean_operator import PostgresToCleanGoogleCloudStorageOperator

from iso3166 import countries
import logging

def country_to_ISO_code(value):
    try:
        country = countries.get(value)
        return country.alpha3
    except KeyError:
        return value

bucket = "demo-bucket.airlaunch.ch"
filename = "data/{{ dag.dag_id }}_{{ run_id }}.json"
schema_filename = "data/{{ dag.dag_id }}_{{ run_id }}_schema.json"

with DAG('covid_pipeline', 
                start_date=datetime(2019, 11, 1),
                schedule_interval='@daily'
            ) as dag:
    
    t1 = PostgresToCleanGoogleCloudStorageOperator(
        task_id  = "extract",
        sql      = """SELECT * FROM public."jhu-covid" WHERE date >= '{start_date}' AND date < '{end_date}'""".format(start_date="{{ execution_date }}", end_date="{{ next_execution_date }}"),
        schema_filename = schema_filename,
        bucket   = bucket,
        filename = filename,
        export_format="json",
        transform_functions={
            "country_region": country_to_ISO_code
        }
    )

    t2 = GoogleCloudStorageToBigQueryOperator(
        task_id="load",
        bucket=bucket,
        bigquery_conn_id = 'google_cloud_default',
        source_objects=["data/{{ dag.dag_id }}_{{ run_id }}.json"],
        schema_object = schema_filename,
        source_format = "NEWLINE_DELIMITED_JSON",
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        destination_project_dataset_table='augmented-works-297410.demo_dataset.covid',
    )
    
    t1 >> t2