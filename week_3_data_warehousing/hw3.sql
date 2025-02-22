-- Create external table 
CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-448902.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ny-taxi-db-hw3/yellow_tripdata_2024-*.parquet']
);

SELECT count(*) FROM `kestra-sandbox-448902.nytaxi.external_yellow_tripdata`;

-- Create non_partitioned_non_clustered table 
CREATE OR REPLACE TABLE kestra-sandbox-448902.nytaxi.yellow_tripdata_non_partitoned AS
SELECT * FROM kestra-sandbox-448902.nytaxi.external_yellow_tripdata;

-- Q1
SELECT COUNT(*) FROM `kestra-sandbox-448902.nytaxi.yellow_tripdata_non_partitoned`; --20332093

-- Q2 
SELECT COUNT(DISTINCT PULocationID)
FROM `kestra-sandbox-448902.nytaxi.yellow_tripdata_non_partitoned`; -- 155.12 MB

SELECT COUNT(DISTINCT PULocationID)
FROM `kestra-sandbox-448902.nytaxi.external_yellow_tripdata`; --0 B

-- Q3 
SELECT PULocationID
FROM `kestra-sandbox-448902.nytaxi.yellow_tripdata_non_partitoned`; -- 155.12 MB

SELECT PULocationID, DOLocationID
FROM `kestra-sandbox-448902.nytaxi.yellow_tripdata_non_partitoned`; --310.24 B

--BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

-- Q4
SELECT COUNT(*) 
FROM `kestra-sandbox-448902.nytaxi.yellow_tripdata_non_partitoned`
WHERE fare_amount = 0; -- 8333

-- Q5: Partition by tpep_dropoff_datetime Cluster on VendorID 
CREATE OR REPLACE TABLE `kestra-sandbox-448902.nytaxi.yellow_tripdata_partitoned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `kestra-sandbox-448902.nytaxi.yellow_tripdata_non_partitoned`
);

-- Q6
SELECT COUNT(DISTINCT VendorID) FROM  `kestra-sandbox-448902.nytaxi.yellow_tripdata_partitoned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'; --26.84MB 

SELECT COUNT(DISTINCT VendorID) FROM  `kestra-sandbox-448902.nytaxi.yellow_tripdata_non_partitoned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'; --310.24MB 

-- Q7: GCP Bucket

-- Q8: False

-- Q8
SELECT COUNT(*) 
FROM `kestra-sandbox-448902.nytaxi.yellow_tripdata_non_partitoned` -- 0B 