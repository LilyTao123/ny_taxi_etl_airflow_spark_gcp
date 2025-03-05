from google.cloud import storage, bigquery
import glob
import os 
import logging

logger = logging.getLogger(__name__)

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
GCS_CLIENT = storage.Client()
def upload_to_gcs(bucket, object_name, local_file):
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    bucket = GCS_CLIENT.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def upload_mult_file_from_directory(directory_path: str, dest_bucket_name: str):
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
    blobs = GCS_CLIENT.list_blobs(BUCKET, prefix=PREFIX)
    
    uris = [f"gs://{BUCKET}/{blob.name}" for blob in blobs if blob.name.endswith(".parquet")]
    
    return uris

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
