{{
    config(
        materialized='table'
    )
}}

with fhv_data as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select f.*,     
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone
from fhv_data f
inner join dim_zones as pickup_zone
on f.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on f.dropoff_locationid = dropoff_zone.locationid