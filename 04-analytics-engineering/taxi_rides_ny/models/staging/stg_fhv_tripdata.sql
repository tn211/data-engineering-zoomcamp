{{ config(materialized='view') }}

SELECT
    dispatching_base_num,
    CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(dropOff_datetime AS TIMESTAMP) AS dropoff_datetime,
    CAST(PUlocationID AS INTEGER) AS pickup_location_id,
    CAST(DOlocationID AS INTEGER) AS dropoff_location_id,
    SR_Flag AS sr_flag,
    Affiliated_base_number AS affiliated_base_number
FROM {{ source('raw', 'fhv_tripdata') }}
WHERE dispatching_base_num IS NOT NULL

{% if target.name == 'dev' %}
    AND pickup_datetime >= '{{ var("dev_start_date") }}'
    AND pickup_datetime < '{{ var("dev_end_date") }}'
{% endif %}