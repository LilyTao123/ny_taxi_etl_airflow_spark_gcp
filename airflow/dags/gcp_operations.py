from google.cloud import storage, bigquery
import glob
import os 
import logging

logger = logging.getLogger(__name__)

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed


GCS_CLIENT = storage.Client()
def upload_from_directory(directory_path: str, dest_bucket_name: str, dest_blob_name: str):
    rel_paths = glob.glob(directory_path + '/**', recursive=True)
    bucket = GCS_CLIENT.get_bucket(dest_bucket_name)
    for local_file in rel_paths:
        remote_path = local_file.replace("/opt/airflow/data/", "")
        # remote_path = f'{dest_blob_name}/{file_name}'
        logger.info(f"Remote path is {remote_path}")
        if os.path.isfile(local_file):
            blob = bucket.blob(remote_path)
            blob.upload_from_filename(local_file)
    logger.info('Dir has been loaded')

# import functions_framework
# from google.cloud import storage, 


def get_parquet_uris(BUCKET, PREFIX):
    """Fetch all Parquet file URIs from the GCS bucket."""
    # client = storage.Client()
    GREEN_PREFIX = f'{PREFIX}/green'
    YELLOW_PREFIX = f'{PREFIX}/yellow'
    green_blobs = GCS_CLIENT.list_blobs(BUCKET, prefix=GREEN_PREFIX)
    yellow_blobs = GCS_CLIENT.list_blobs(BUCKET, prefix=YELLOW_PREFIX)
    
    g_uris = [f"gs://{BUCKET}/{blob.name}" for blob in green_blobs if blob.name.endswith(".parquet")]
    y_uris = [f"gs://{BUCKET}/{blob.name}" for blob in yellow_blobs if blob.name.endswith(".parquet")]
    
    return g_uris + y_uris

# @functions_framework.cloud_event
def update_bigquery_external_table(BUCKET, PREFIX, PROJECT_ID, DATASET_ID, EXTERNAL_TABLE_ID):
    """Triggered when a file is added/removed in GCS, updates the external table in BigQuery."""
    bigquery_client = bigquery.Client()

    uris = get_parquet_uris(BUCKET, PREFIX)

    if not uris:
        print("No Parquet files found!")
        return
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{EXTERNAL_TABLE_ID}"

    query = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{table_ref}`
    OPTIONS (
      format = 'PARQUET',
      uris = {uris}
    );
    """
    
    query_job = bigquery_client.query(query)
    query_job.result()

    logger.info(f"Updated external table `{table_ref}` with {len(uris)} files.")
