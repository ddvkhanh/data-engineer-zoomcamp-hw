
  
    
    

    create  table
      "dev"."main"."dim_locations__dbt_tmp"
  
    as (
      with taxi_zone_lookup as (
    select * from "dev"."main"."taxi_zone_lookup"
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
    );
  
  