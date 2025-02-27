from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import yaml

import pytest

with open('common/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Data ingest setting
year = config['year']
month = config['month']
url_prefix = config['prefix']
file_type = config['file_type']

BUCKET = os.environ.get("GCP_GCS_BUCKET")

# BigQuery file setting 
bq_trips_records = config['bq_trips_records']
bq_trips_records_dataset_id = config['bq_trips_records_dataset_id']
bq_external_trips_records = config['bq_external_trips_records']

class File_Config:
    def __init__(self, service_type, year=year, month=month):

        self.service_type = service_type

        self.month = month 
        self.year = year

        self.path_prefix = f"/opt/airflow/data"
        self.local_input_path = f'{self.path_prefix}/raw/{service_type}/{year}/{month:02d}'
        self.local_output_path = f'{self.path_prefix}/pq/{service_type}/{year}/{month:02d}'

        self.table_name = f'{service_type}_tripdata_{year}-{month:02d}.{file_type}'

        self.ingestion_url = f'{url_prefix}/trip-data/{self.table_name}' 

        self.gcp_path = f'gs://{BUCKET}/pq/{service_type}/'

    def pickup_col_name(self):
        mutual = 'pep_pickup_datetime'
        return f'l{mutual}' if self.service_type == 'green' else f't{mutual}'

    def dropoff_col_name(self):
        mutual = 'pep_dropoff_datetime'
        return f'l{mutual}' if self.service_type == 'green' else f't{mutual}'