-- Create external table 
CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-448902.nytaxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ny-taxi-db-hw3/green_tripdata_2022-*.parquet']
);

SELECT count(*) FROM `kestra-sandbox-448902.nytaxi.external_green_tripdata`;

-- Create non_partitioned_non_clustered table 
CREATE OR REPLACE TABLE kestra-sandbox-448902.nytaxi.green_tripdata_non_partitoned AS
SELECT * FROM kestra-sandbox-448902.nytaxi.external_green_tripdata;

-- Q1
SELECT COUNT(*) FROM `kestra-sandbox-448902.nytaxi.green_tripdata_non_partitoned`; --840402

-- Q2 
SELECT COUNT(DISTINCT PULocationID)
FROM `kestra-sandbox-448902.nytaxi.green_tripdata_non_partitoned`; -- 6.41 MB

SELECT COUNT(DISTINCT PULocationID)
FROM `kestra-sandbox-448902.nytaxi.external_green_tripdata`; --0 B

-- Q3
SELECT COUNT(*) 
FROM `kestra-sandbox-448902.nytaxi.green_tripdata_non_partitoned`
WHERE fare_amount = 0; -- 1622

-- Q4: Partition by lpep_pickup_datetime Cluster on PUlocationID
CREATE OR REPLACE TABLE `kestra-sandbox-448902.nytaxi.green_tripdata_partitoned`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS (
  SELECT * FROM `kestra-sandbox-448902.nytaxi.green_tripdata_non_partitoned`
);

-- Q5
SELECT COUNT(DISTINCT PULocationID) FROM  `kestra-sandbox-448902.nytaxi.green_tripdata_partitoned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'; --1.12MB 

SELECT COUNT(DISTINCT PULocationID) FROM  `kestra-sandbox-448902.nytaxi.green_tripdata_non_partitoned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'; --12.82MB 

-- Q6: GCP Bucket

-- Q7: False

-- Q8
SELECT COUNT(*) 
FROM `kestra-sandbox-448902.nytaxi.green_tripdata_non_partitoned` -- 0B 