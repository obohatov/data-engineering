-- Data Engineering Zoomcamp 2026 - Module 3: Data Warehouse Homework
-- Dataset: Yellow Taxi Trip Data (January - June 2024)

-- SETUP: Creating the tables
-- Creating the External Table pointing to GCS
CREATE OR REPLACE EXTERNAL TABLE `your_project.your_dataset.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your-bucket-name/taxi_data/yellow_tripdata_2024-*.parquet']
);

-- Creating the Regular (Materialized) Table from the External Table
CREATE OR REPLACE TABLE `your_project.your_dataset.yellow_tripdata_non_partitioned` AS
SELECT * FROM `your_project.your_dataset.external_yellow_tripdata`;


-- QUESTION 1:
-- What is count of records for the 2024 Yellow Taxi Data?
-- Answer: 20,332,093
SELECT count(*) FROM `your_project.your_dataset.yellow_tripdata_non_partitioned`;


-- QUESTION 2:
-- Estimated amount of data to be read for DISTINCT PULocationID?
-- Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table
SELECT COUNT(DISTINCT(PULocationID)) FROM `your_project.your_dataset.external_yellow_tripdata`;
SELECT COUNT(DISTINCT(PULocationID)) FROM `your_project.your_dataset.yellow_tripdata_non_partitioned`;


-- QUESTION 3:
-- Why are the estimated number of Bytes different?
-- Answer: BigQuery is a columnar database, and it only scans the specific columns requested. 
-- Querying two columns requires reading more data than querying one column.


-- QUESTION 4:
-- How many records have a fare_amount of 0?
-- Answer: 8,333
SELECT count(*) 
FROM `your_project.your_dataset.yellow_tripdata_non_partitioned` 
WHERE fare_amount = 0;


-- QUESTION 5:
-- What is the best strategy to make an optimized table?
-- Filter: tpep_dropoff_datetime, Order: VendorID
-- Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID
CREATE OR REPLACE TABLE `your_project.your_dataset.yellow_tripdata_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `your_project.your_dataset.yellow_tripdata_non_partitioned`;


-- QUESTION 6:
-- Retrieve distinct VendorIDs between 2024-03-01 and 2024-03-15 (inclusive).
-- Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
SELECT DISTINCT(VendorID)
FROM `your_project.your_dataset.yellow_tripdata_non_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT(VendorID)
FROM `your_project.your_dataset.yellow_tripdata_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';


-- QUESTION 7:
-- Where is the data stored in the External Table you created?
-- Answer: GCP Bucket


-- QUESTION 8:
-- It is best practice in Big Query to always cluster your data?
-- Answer: False


-- QUESTION 9:
-- Write a `SELECT count(*)` query FROM the materialized table. How many bytes will it read? 
-- Answer: 0 bytes. 
-- Explanation: BigQuery stores the total row count in the table's metadata for native tables.
SELECT count(*) FROM `your_project.your_dataset.yellow_tripdata_non_partitioned`;
