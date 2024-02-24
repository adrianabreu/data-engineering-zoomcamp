{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *,"Affiliated_base_number" as "affiliated", "SR_Flag" as sr_flag,"PUlocationID" as pulocationid, "DOlocationID" as dolocationid
  from {{ source('staging','fhv_tripdata') }}
),
casted as (
select
    -- identifiers
     affiliated,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast("dropOff_datetime" as timestamp) as dropoff_datetime
from tripdata
)
select *
from casted
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2020-01-01'
-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'