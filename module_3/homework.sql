 CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-project-449009.taxi_data.yellow_tripdata_2024`
 OPTIONS (
   format = 'PARQUET',
   uris = ['gs://dezoomcamp_hw3_2025_tmk/yellow_tripdata_2024-*.parquet']
);

-- Question 1
SELECT COUNT(*) FROM de-zoomcamp-project-449009.taxi_data.yellow_tripdata_2024;

-- Question 2
SELECT COUNT(DISTINCT PULocationID) FROM de-zoomcamp-project-449009.taxi_data.yellow_tripdata_2024;
SELECT COUNT(DISTINCT PULocationID) FROM de-zoomcamp-project-449009.taxi_data.yellow_tripdata_2024_non_partitoned;

-- Question 3
SELECT PULocationID FROM de-zoomcamp-project-449009.taxi_data.yellow_tripdata_2024_non_partitoned;
SELECT PULocationID, DOLocationID FROM de-zoomcamp-project-449009.taxi_data.yellow_tripdata_2024_non_partitoned;

-- Question 4
SELECT COUNT (*) FROM de-zoomcamp-project-449009.taxi_data.yellow_tripdata_2024 WHERE fare_amounnt = 0;

-- Question 5
CREATE OR REPLACE TABLE de-zoomcamp-project-449009.taxi_data.yellow_tripdata_2024_partitoned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM de-zoomcamp-project-449009.taxi_data.yellow_tripdata_2024;

-- Question 6
-- estimated_time 310.24 Mb
SELECT DISTINCT VendorID FROM de-zoomcamp-project-449009.taxi_data.yellow_tripdata_2024_non_partitoned
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' and '2024-03-15';

-- estimated_time 26.84 Mb
SELECT DISTINCT VendorID FROM de-zoomcamp-project-449009.taxi_data.yellow_tripdata_2024_partitoned_clustered
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' and '2024-03-15';