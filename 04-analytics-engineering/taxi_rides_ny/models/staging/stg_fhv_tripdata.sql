{{ config(materialized='view') }}

select
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(sr_flag as string) as sr_flag,
    cast(affiliated_base_number as string) as affiliated_base_number
from {{ source('staging','fhv') }}
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2020-01-01'
{% if var('is_test_run', default=true) %} limit 100 {% endif %}
