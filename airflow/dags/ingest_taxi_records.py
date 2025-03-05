from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def extract_delta():
    # Get last extraction timestamp (default to a past date)
    last_extracted_time = Variable.get("last_extracted_time", default_var="2024-03-01")

    return "transform_data" if last_extracted_time <= current_time else "skip_pipeline"

    # Update the last extraction timestamp
    new_last_extracted_time = datetime.utcnow().strftime("%Y-%m-%d")
    Variable.set("last_extracted_time", new_last_extracted_time)