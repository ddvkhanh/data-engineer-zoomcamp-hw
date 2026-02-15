with taxi_zone_lookup as (
    select * from "taxi_rides_ny"."prod"."taxi_zone_lookup"
),
renamed as (
    select 
        locationid as location_id,
        borough,
        zone,
        service_zone
    from taxi_zone_lookup
)

select * from renamed