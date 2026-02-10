with trips_unioned as (
    select * from {{ref('int_trips_unioned')}}
),

vendors as (
    select distinct vendor_id,
    case
        when vendor_id = 1 then 'Creative Mobile Technologies, LLC'
        when vendor_id = 2 then 'VeriFone Inc.'
        when vendor_id = 4 then 'Unknown Vendor'
    end as vendor_name
    from trips_unioned
)

select * from vendors