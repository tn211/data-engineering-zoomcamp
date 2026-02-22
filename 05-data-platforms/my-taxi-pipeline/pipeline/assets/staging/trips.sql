/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: create+replace

columns:
  - name: vendor_id
    type: integer
    description: ID of the taxi vendor
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: total_amount
    type: float
    description: Total trip amount
    checks:
      - name: non_negative

custom_checks:
  - name: no_negative_fare
    description: fare_amount should never be negative
    query: |
      SELECT COUNT(*) FROM staging.trips WHERE fare_amount < 0
    value: 0

@bruin */

SELECT
    t.vendor_id,
    t.rate_code_id,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.store_and_fwd_flag,
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.payment_type,
    p.payment_type_name AS payment_type_description,
    t.service_type,
    t.extracted_at
FROM ingestion.trips t
LEFT JOIN ingestion.payment_lookup p
    ON t.payment_type = p.payment_type_id
WHERE t.pickup_datetime >= '{{ start_datetime }}'
  AND t.pickup_datetime < '{{ end_datetime }}'
  AND t.vendor_id IS NOT NULL
  AND t.fare_amount >= 0