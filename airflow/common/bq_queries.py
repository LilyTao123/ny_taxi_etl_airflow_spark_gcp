from common.file_config import *
import os 

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

create_partitioned_trip_records_table_query = f""" 
    CREATE OR REPLACE TABLE {PROJECT_ID}.{bq_trips_records_dataset_id}.{bq_trips_records} 
    PARTITION BY DATE(pickup_datetime) 
    CLUSTER BY pickup_month, PULocationID
    AS
    SELECT * FROM {PROJECT_ID}.{bq_trips_records_dataset_id}.{bq_external_trips_records};
""" 


create_trips_daily_revenue = f"""
CREATE OR REPLACE TABLE {PROJECT_ID}.{bq_trips_records_dataset_id}.trips_daily_revenue
AS
with revenue as (
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zoneId,
    Date(pickup_datetime) as pickup_date, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_daily_fare,
    SUM(extra) AS revenue_daily_extra,
    SUM(mta_tax) AS revenue_daily_mta_tax,
    SUM(tip_amount) AS revenue_daily_tip_amount,
    SUM(tolls_amount) AS revenue_daily_tolls_amount,
    SUM(improvement_surcharge) AS revenue_daily_improvement_surcharge,
    SUM(total_amount) AS revenue_daily_total_amount,
    SUM(congestion_surcharge) AS revenue_daily_congestion_surcharge,
    SUM(trip_distance) as daily_trip_distance,

    COUNT(passenger_count) as daily_passenger_cnt,
    COUNT(1) as daily_trips_cnt,
    -- Additional calculations
    AVG(passenger_count) AS avg_daily_passenger_cnt,
    AVG(trip_distance) AS avg_daily_trip_distance
FROM
    `trips_data_all.trips_records`
GROUP BY 
    1, 2, 3 )
    select a.*,
           b.Zone as pickup_zone,
           b.Borough from revenue as a 
    left join `trips_data_all.taxi_zone` as b 
    on a.revenue_zoneId = b.LocationID
"""

create_trips_zone = f"""
    CREATE OR REPLACE TABLE {PROJECT_ID}.{bq_trips_records_dataset_id}.taxi_zone 
    AS
    SELECT * FROM {PROJECT_ID}.{bq_trips_records_dataset_id}.{bq_external_taxi_zone};
"""
