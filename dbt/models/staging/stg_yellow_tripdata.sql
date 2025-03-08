with 

source as (

    select * from {{ source('staging', 'yellow_tripdata') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['vendorid', 'pickup_datetime', 'dropoff_datetime', 'trip_distance
', 'fare_amount']) }} as tripid,
        {{ dbt.safe_cast("vendorid", api.Column.translate_type("integer")) }} as vendorid,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count,
        cast(trip_distance as numeric) as trip_distance,

        {{ dbt.safe_cast("ratecodeid", api.Column.translate_type("integer")) }} as ratecodeid,
        store_and_fwd_flag,
        {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pulocationid,
        {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dolocationid,

        {{get_payment_type_description(payment_type)}} as payment_type_descripted,
        payment_type,
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount,
        cast(congestion_surcharge as numeric) as congestion_surcharge,
        cast(airport_fee as numeric) as airport_fee,

        service_type,

        pickup_month,
        unique_row_id

    from source

)

select * from renamed
