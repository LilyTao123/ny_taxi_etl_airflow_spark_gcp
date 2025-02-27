from common.file_config import *
import os 

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

create_partitioned_trip_records_table_query = f""" 
    CREATE OR REPLACE TABLE {PROJECT_ID}.{bq_trips_records_dataset_id}.{bq_trips_records} 
    PARTITION BY DATE(pickup_datetime) 
    AS
    SELECT * FROM {PROJECT_ID}.{bq_trips_records_dataset_id}.{bq_external_trips_records};
""" 