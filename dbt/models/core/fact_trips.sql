{{
    config(
        materialized='table',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
        'field': 'pickup_datetime',
        'data_type': 'timestamp',
        'granularity': 'day'}
    )
}}

with green_tripdata as (
    select 
        'Green' as service_type,
        tripid,
        vendorid,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        ratecodeid,
        pulocationid,
        dolocationid,

        passenger_count,
        trip_distance,

        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        payment_type,
        payment_type_descripted
        
    from {{ref('stg_green_tripdata')}}
),
yelow_tripdata as (
    select 
        'Yellow' as service_type,
        tripid,
        vendorid,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        ratecodeid,
        pulocationid,
        dolocationid,

        passenger_count,
        trip_distance,

        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        payment_type,
        payment_type_descripted

    from {{ref("stg_yellow_tripdata")}}
),
trips_unioned as (
    select * from green_tripdata
    union all 
    select * from yelow_tripdata
),
dim_zones as (
    select * from {{ref("dim_zones")}}
    where borough != 'Unknown'
)
select 
    trips_unioned.* ,
    pickup_zone.borough as pickup_zone,
    dropoff_zone.borough as dropoff_zone
from trips_unioned
left join dim_zones as pickup_zone 
on trips_unioned.pulocationid = pickup_zone.locationid
left join dim_zones as dropoff_zone 
on trips_unioned.dolocationid = dropoff_zone.locationid

