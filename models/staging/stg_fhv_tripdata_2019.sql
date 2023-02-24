{{ config(materialized='view') }}

select 
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(SR_Flag as string) as sr_flag,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    cast(Affiliated_base_number as string) as affiliated_base_number,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime
from {{ source('staging', 'fhv_tripdata_2019') }}
    

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
