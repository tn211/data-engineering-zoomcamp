-- Create schema. --
CREATE SCHEMA `abiding-bongo-485522-a4.nytaxi`
OPTIONS (location = 'us-east1');

-- Create external table. --]
CREATE OR REPLACE EXTERNAL TABLE `abiding-bongo-485522-a4.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://tyler_taxi_bucket/*.parquet']
);

-- Test it. --
SELECT * FROM `abiding-bongo-485522-a4.nytaxi.external_yellow_tripdata` LIMIT 10;

-- Create non-partitioned and partitioned tables, respectively. --
CREATE OR REPLACE TABLE abiding-bongo-485522-a4.nytaxi.yellow_tripdata_non_partitioned AS
SELECT * FROM abiding-bongo-485522-a4.nytaxi.external_yellow_tripdata;

CREATE OR REPLACE TABLE abiding-bongo-485522-a4.nytaxi.yellow_tripdata_partitioned
PARTITION BY DATE(tpep_pickup_datetime) AS
SELECT * FROM abiding-bongo-485522-a4.nytaxi.external_yellow_tripdata;

-- Impact of partition
-- Scanning 1.6GB of data
SELECT DISTINCT(VendorID)
FROM abiding-bongo-485522-a4.nytaxi.yellow_tripdata_non_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Scanning ~106 MB of DATA
SELECT DISTINCT(VendorID)
FROM abiding-bongo-485522-a4.nytaxi.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Let's look into the partitions
SELECT table_name, partition_id, total_rows
FROM `abiding-bongo-485522-a4.nytaxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE abiding-bongo-485522-a4.nytaxi.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM abiding-bongo-485522-a4.nytaxi.external_yellow_tripdata;

-- Query scans 1.1 GB
SELECT count(*) as trips
FROM abiding-bongo-485522-a4.nytaxi.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM abiding-bongo-485522-a4.nytaxi.yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;






