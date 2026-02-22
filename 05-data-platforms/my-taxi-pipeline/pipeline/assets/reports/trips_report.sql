/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: create+replace

columns:
  - name: service_type
    type: varchar
    description: yellow or green
    primary_key: true
  - name: pickup_date
    type: date
    description: Date of pickup
    primary_key: true
  - name: total_trips
    type: bigint
    description: Number of trips
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: Sum of total_amount
    checks:
      - name: non_negative

@bruin */

SELECT
    service_type,
    CAST(pickup_datetime AS DATE) AS pickup_date,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_amount,
    SUM(fare_amount) AS total_fare,
    SUM(tip_amount) AS total_tips,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(passenger_count) AS avg_passenger_count
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY service_type, CAST(pickup_datetime AS DATE)