--Filter out records where dispatching_base_num IS NULL
--Rename fields to match your project's naming conventions (e.g., PUlocationID → pickup_location_id)

select 
    -- identifiers
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(pickup_datetime as datetime) as pickup_datetime,
    cast(dropoff_datetime as datetime) as dropoff_datetime,
    cast(pulocationid as int) as pickup_location_id,
    cast(dolocationid as int) as dropoff_location_id,
    cast(sr_flag as string) as sr_flag,
    cast(affiliated_base_number as string) as affiliated_base_number

from {{source('raw_data', 'fhv_tripdata')}}
where dispatching_base_num is not null