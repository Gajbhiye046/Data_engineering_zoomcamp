{{ config(materialized="view") }}

with
    tripdata as (
        select *, row_number() over (partition by pickup_datetime) as rn
        from {{ source("staging", "fhv_tripdata_non_partitoned") }}
    )
select
    -- identifiers
    {{ dbt_utils.surrogate_key(["dispatching_base_num","affiliated_base_number", "pickup_datetime","pickup_locationid"]) }} as tripid,
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(affiliated_base_number as string) as affiliated_base_number,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    SR_flag as shared_ride_flag
    -- payment info (Null)

from tripdata
where rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
