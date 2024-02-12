# SETUP:
Create an external table using the Green Taxi Trip Records Data for 2022 (without partition or cluster)

```
CREATE SCHEMA `spartan-shadow-194608.cohort4`
OPTIONS (
    location = 'EU'
);

CREATE OR REPLACE EXTERNAL TABLE `spartan-shadow-194608.cohort4.nytaxi_external_green_tripdata_2022`
OPTIONS (
format = 'PARQUET',
uris = ['gs://terraform-homework1-bucket/cohort4/green*.parquet']
);
```

And now create a materialized table

```
CREATE OR REPLACE  TABLE `spartan-shadow-194608.cohort4.nytaxi_materialized_green_tripdata_2022`
AS select * from `spartan-shadow-194608.cohort4.nytaxi_external_green_tripdata_2022`
```


## Question 1: 
Question 1: What is count of records for the 2022 Green Taxi Data??

Let's use the following query:
`select count(*) from `spartan-shadow-194608.cohort4.nytaxi_external_green_tripdata_2022`

It yields 840,402



## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

As we are using cache

```
SELECT count(distinct(PULocationID)) FROM `spartan-shadow-194608.cohort4.nytaxi_external_green_tripdata_2022`
```
WIill read 0 bs

```
SELECT count(distinct(PULocationID)) FROM `spartan-shadow-194608.cohort4.nytaxi_materialized_green_tripdata_2022`
```

Will read 6.41 MB


## Question 3:
How many records have a fare_amount of 0?

`SELECT count(*) FROM `spartan-shadow-194608.cohort4.nytaxi_materialized_green_tripdata_2022` where fare_amount = 0`

1622

## Question 4:
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

- Partition by lpep_pickup_datetime  Cluster on PUlocationID

```
CREATE OR REPLACE  TABLE `spartan-shadow-194608.cohort4.nytaxi_partitioned_and_clustered_green_tripdata_2022`
PARTITION BY DATE(lpep_pickup_datetime) 
CLUSTER BY PUlocationID
AS select * from `spartan-shadow-194608.cohort4.nytaxi_external_green_tripdata_2022`
```

## Quesion 5:

```
select distinct(PULocationID)
from `spartan-shadow-194608.cohort4.nytaxi_materialized_green_tripdata_2022`
where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30'
```

Will read: 12.82 MB


``` 
select distinct(PULocationID)
from `spartan-shadow-194608.cohort4.nytaxi_partitioned_and_clustered_green_tripdata_2022`
where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30'
```

Will read: 1.12 MB

## Question 6:

External data is located in the gcp bucket, I had to point the uri source in the option

## Question 7:

False, if your table with data size is less than 1GB it will add an extra overhead that's not needed.

## Question 8:

0 bytes, because tjat table is within big query storage and big query knows how many rows there are and use that statistic to answer the query
