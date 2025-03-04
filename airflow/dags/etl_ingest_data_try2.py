import os
import re
import sys

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

current_file_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(os.path.dirname(current_file_path))
sys.path.append(parent_directory)

from common.file_config import *
from common.bq_queries import *
from gcp_operations import upload_mult_file_from_directory, update_bigquery_external_table

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# dataset setting
green_service_type = 'green'
green = Taxi_Config(green_service_type)

yellow_service_type = 'yellow'
yellow = Taxi_Config(yellow_service_type)

default_args = {
    "owner": "airflow",
    "start_date": datetime.today(),
    "depends_on_past": False,
    "retries": 0,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_green_dataset_task = BashOperator(
        task_id="download_green_dataset_task",
        bash_command=f" mkdir -p {green.local_input_path} && curl -sSL {green.ingestion_url} > {green.local_input_path}/{green.table_name}"
    )

    download_yellow_dataset_task = BashOperator(
        task_id="download_yellow_dataset_task",
        bash_command=f" mkdir -p {yellow.local_input_path} && curl -sSL {yellow.ingestion_url} > {yellow.local_input_path}/{yellow.table_name}"
    )

    spark_green_orgnise = SparkSubmitOperator(
        task_id="spark_green_orgnise",
        application="/opt/airflow/jobs/pyspark_green_orgnise_data.py", # Spark application path created in airflow and spark cluster
        name="spark-green-rename",
        # application_args = ['green'],
        conn_id="spark-conn"
    )

    spark_yellow_orgnise = SparkSubmitOperator(
        task_id="spark_yellow_orgnise",
        application="/opt/airflow/jobs/pyspark_yellow_orgnise_data.py", # Spark application path created in airflow and spark cluster
        name="spark-yellow-rename",
        # application_args = ['yellow'],
        conn_id="spark-conn"
    )

    local_green_to_gcs_task = PythonOperator(
        task_id="local_green_to_gcs_task",
        python_callable=upload_mult_file_from_directory,
        op_kwargs={
            "directory_path": green.local_output_path,
            "dest_bucket_name": BUCKET
        },
    )

    local_yellow_to_gcs_task = PythonOperator(
        task_id="local_yellow_to_gcs_task",
        python_callable=upload_mult_file_from_directory,
        op_kwargs={
            "directory_path": yellow.local_output_path,
            "dest_bucket_name": BUCKET
        },
    )

    bq_create_external_trips_records = PythonOperator(
        task_id="create_trips_records",
        python_callable = update_bigquery_external_table,
        op_kwargs={
            'BUCKET': BUCKET, 
            'PREFIX': 'pq', 
            'PROJECT_ID': PROJECT_ID, 
            'DATASET_ID': bq_trips_records_dataset_id, 
            'EXTERNAL_TABLE_ID': bq_external_trips_records
        },
    )

    bq_partitioned_trips_records = BigQueryInsertJobOperator(
        task_id='bq_partitioned_trips_records',
        configuration={
            'query': {
                'query': create_partitioned_trip_records_table_query,
                'useLegacySql': False,
            }
        },
    )

    bq_trips_daily_revenue = BigQueryInsertJobOperator(
        task_id='bq_trips_daily_revenue',
        configuration={
            'query': {
                'query': create_trips_daily_revenue,
                'useLegacySql': False,
            }
        },
    )


    [download_green_dataset_task >> spark_green_orgnise >> local_green_to_gcs_task , download_yellow_dataset_task >> spark_yellow_orgnise >> local_yellow_to_gcs_task] >> bq_create_external_trips_records >> bq_partitioned_trips_records >> bq_trips_daily_revenue