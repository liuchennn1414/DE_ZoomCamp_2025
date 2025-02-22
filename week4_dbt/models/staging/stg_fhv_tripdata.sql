{{
    config(
        materialized='view'
    )
}}

select
    dispatching_base_num, 
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("integer")) }} as SR_Flag,     
    Affiliated_base_number 
from {{ source('staging','fhv_tripdata') }}
where dispatching_base_num is not null
--date(pickup_datetime) BETWEEN '2019-01-01' AND '2019-12-31'

-- variable valye can be changed via CLI: 
-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=false) %} 
  limit 100

{% endif %}