SELECT station_id, name  FROM `bigquery-public-data.new_york_citibike.citibike_stations` LIMIT 100;

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `optimum-airfoil-376815.trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_optimum-airfoil-376815/data/fhv/fhv_tripdata_2019-*.csv.gz', 'gs://dtc_data_lake_optimum-airfoil-376815/data/fhv/fhv_tripdata_2020-*.csv.gz']
);

-- Check fhv trip data
SELECT * FROM optimum-airfoil-376815.trips_data_all.external_fhv_tripdata limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE optimum-airfoil-376815.trips_data_all.fhv_tripdata_non_partitoned AS
SELECT * FROM optimum-airfoil-376815.trips_data_all.external_fhv_tripdata;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE optimum-airfoil-376815.trips_data_all.fhv_tripdata_partitoned
PARTITION BY
  DATE(pickup_datetime) AS
SELECT * FROM optimum-airfoil-376815.trips_data_all.external_fhv_tripdata;

-- Impact of partition
-- Scanning 844MB of data
SELECT DISTINCT(PUlocationID)
FROM optimum-airfoil-376815.trips_data_all.fhv_tripdata_non_partitoned
WHERE DATE(pickup_datetime) BETWEEN '2019-06-01' AND '2020-06-30';

--scanning  of data
-- Scanning 308MB of data
SELECT DISTINCT(PUlocationID)
FROM optimum-airfoil-376815.trips_data_all.fhv_tripdata_partitoned
WHERE DATE(pickup_datetime) BETWEEN '2019-06-01' AND '2020-06-30';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `trips_data_all.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'fhv_tripdata_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table (cluster with PULocationID)
CREATE OR REPLACE TABLE optimum-airfoil-376815.trips_data_all.fhv_tripdata_partitoned_clustered_loc
PARTITION BY DATE(pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM optimum-airfoil-376815.trips_data_all.external_fhv_tripdata;

-- Creating a partition and cluster table (cluster with Affiliated_base_number)
CREATE OR REPLACE TABLE optimum-airfoil-376815.trips_data_all.fhv_tripdata_partitoned_clustered_Aff_base
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM optimum-airfoil-376815.trips_data_all.external_fhv_tripdata;

-- Query scans 431 MB
SELECT count(*) as trips
FROM optimum-airfoil-376815.trips_data_all.fhv_tripdata_partitoned
WHERE DATE(pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
AND Affiliated_base_number='B00225';

-- Query scans 401.63 GB
SELECT count(*) as trips
FROM optimum-airfoil-376815.trips_data_all.fhv_tripdata_partitoned_clustered_Aff_base
WHERE DATE(pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
AND Affiliated_base_number='B00225';

-- Homework queries
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `optimum-airfoil-376815.trips_data_all.external_fhv_2019_hw`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_optimum-airfoil-376815/data/fhv/fhv_tripdata_2019-*.csv.gz']
);
-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE optimum-airfoil-376815.trips_data_all.fhv_2019_non_partitoned_hw AS
SELECT * FROM optimum-airfoil-376815.trips_data_all.external_fhv_2019_hw;
-- ques1:
SELECT count(*) as count_of_trips
FROM optimum-airfoil-376815.trips_data_all.external_fhv_2019_hw
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019;

-- ques2:
--External_table
SELECT count(DISTINCT(Affiliated_base_number)) as count_of_trips
FROM optimum-airfoil-376815.trips_data_all.external_fhv_2019_hw;
--Materialised table
SELECT count(DISTINCT(Affiliated_base_number)) as count_of_trips
FROM optimum-airfoil-376815.trips_data_all.fhv_2019_non_partitoned_hw;

--ques3:
--Materialised table
SELECT count(*) as count_of_trips
FROM optimum-airfoil-376815.trips_data_all.fhv_2019_non_partitoned_hw
WHERE PULocationID IS NULL AND DOLocationID IS NULL;

--ques4:
-- Creating a partition and cluster table (partition by pickup_datetime,cluster with PULocationID)
CREATE OR REPLACE TABLE optimum-airfoil-376815.trips_data_all.fhv_2019_partitoned_clustered_aff_base_hw
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM optimum-airfoil-376815.trips_data_all.fhv_2019_non_partitoned_hw;

--ques5:
--Materialised table (non partitioned)
SELECT count(DISTINCT(Affiliated_base_number)) as count_of_trips
FROM optimum-airfoil-376815.trips_data_all.fhv_2019_non_partitoned_hw
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

--Materialised table (partitioned : pickup_datetime, clustered : Affiliated_base_number)
SELECT count(DISTINCT(Affiliated_base_number)) as count_of_trips
FROM optimum-airfoil-376815.trips_data_all.fhv_2019_partitoned_clustered_aff_base_hw
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31'
