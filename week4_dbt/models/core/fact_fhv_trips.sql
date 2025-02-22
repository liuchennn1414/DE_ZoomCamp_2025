{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *, 
        'fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv_tripdata.dispatching_base_num, 
    fhv_tripdata.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_tripdata.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_tripdata.pickup_datetime, 
    EXTRACT(YEAR FROM fhv_tripdata.pickup_datetime) AS pickup_year,
    EXTRACT(MONTH FROM fhv_tripdata.pickup_datetime) AS pickup_month,
    fhv_tripdata.dropoff_datetime, 
    fhv_tripdata.SR_Flag,
    fhv_tripdata.Affiliated_base_number,
    fhv_tripdata.service_type
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid

-- you can do dbt build -s +fact_trips+ --vars'{'is_test_run':false}' to run the whole project 