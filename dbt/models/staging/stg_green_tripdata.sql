{{
    config(
        materialized='view'
    )
}}

with 

tripdata as (

    select *,
            row_number() over(partition by vendorid, pickup_datetime) as rn  
    from {{ source('staging', 'green_tripdata') }}
    where vendorid is not null
),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['vendorid', 'pickup_datetime', 'dropoff_datetime', 'trip_distance
', 'fare_amount']) }} as tripid,
        {{ dbt.safe_cast("vendorid", api.Column.translate_type("integer")) }} as vendorid,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        store_and_fwd_flag,
        {{ dbt.safe_cast("ratecodeid", api.Column.translate_type("integer")) }} as ratecodeid,
        {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pulocationid,
        {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dolocationid,

        {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count,
        cast(trip_distance as numeric) as trip_distance,
        {{dbt.safe_cast("trip_type", api.Column.translate_type("integer")) }} as trip_type,

        -- payment info
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(ehail_fee as numeric) as ehail_fee,
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount,
        payment_type,
        {{get_payment_type_description(payment_type)}} as payment_type_descripted,
        cast(congestion_surcharge as numeric) as congestion_surcharge,
        service_type,

        pickup_month,
        unique_row_id

    from tripdata
    where rn = 1
)

select * from renamed

{% if var('is_test_run', default=true) %}

    limit 100 

{% endif %}