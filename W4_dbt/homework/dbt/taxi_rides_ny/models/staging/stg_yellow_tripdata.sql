select 
    -- identifiers
    cast(vendorid as int) as vendor_id,
    cast(ratecodeid as int) as rate_code_id,
    cast(pulocationid as int) as pickup_location_id,
    cast(dolocationid as int) as dropoff_location_id,

    -- timestamps
    cast(tpep_pickup_datetime as datetime) as pickup_datetime,
    cast(tpep_dropoff_datetime as datetime) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as int) as passenger_count,
    cast(trip_distance as float) as trip_distance,
    1 as trip_type, -- yellow taxis can only be street-hail, so we set trip_type to 1 for all records in yellow_tripdata

    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mrta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    0 as ehail_fee, -- this column is only for green_tripdata, so we set it to 0 for yellow_tripdata
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as int) as payment_type

from {{source('raw_data', 'yellow_tripdata')}}
where vendorid is not null
