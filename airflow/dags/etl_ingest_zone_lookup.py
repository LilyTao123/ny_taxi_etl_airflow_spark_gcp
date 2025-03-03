import os
import re
import sys

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

current_file_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(os.path.dirname(current_file_path))
sys.path.append(parent_directory)

from common.file_config import *
from common.bq_queries import *
from gcp_operations import upload_to_gcs, upload_mult_file_from_directory, update_bigquery_external_table
from zone_lookup_ingest import ingest_zone

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# dataset setting


default_args = {
    "owner": "airflow",
    "start_date": datetime.today(),
    "depends_on_past": False,
    "retries": 0,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_taxi_zone_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_zone_lookup_task = PythonOperator(
        task_id="download_zone_lookup_task",
        python_callable=ingest_zone,
        op_kwargs = {
           'url' : taxi_zone_ingest_url,
           'table_name': taxi_zone_table_name,
           'output_path': taxi_zone_output_path
        }
    )

    upload_zone_table_to_gcs = PythonOperator(
        task_id="local_taxi_zone_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f'taxi_zone/{taxi_zone_table_name}',
            'local_file': f'/opt/airflow/data/taxi_zone/{taxi_zone_table_name}',
        },
    )

    bq_external_zone_table = BigQueryCreateExternalTableOperator(
        task_id="bq_external_zone_table",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": bq_trips_records_dataset_id,
                "tableId": bq_external_taxi_zone,
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/taxi_zone/{taxi_zone_table_name}"],
            },
        },
    )

    bq_create_trips_zone = BigQueryInsertJobOperator(
        task_id='bq_create_trips_zone',
        configuration={
            'query': {
                'query': create_trips_zone,
                'useLegacySql': False,
            }
        },
    )

    download_zone_lookup_task >> upload_zone_table_to_gcs >> bq_external_zone_table >> bq_create_trips_zone