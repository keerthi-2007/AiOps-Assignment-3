# DA3408 A3 — Spark vs. Ray: The Data Engineering Duel

## Overview

This project implements the same distributed data preprocessing pipeline using
Apache Spark and Ray Data and compares their performance on NYC Yellow Taxi
Trip Data.

The pipeline performs:

1. Ingestion of multiple Parquet files
2. Data cleansing and timestamp normalization
3. Location lookup joins
4. Average-speed calculation using a custom Python transformation
5. Parquet export

Both frameworks were deployed as distributed clusters with one head/master
and two workers.

---

## Dataset

NYC Yellow Taxi Trip Data was used along with the NYC Taxi Zone Lookup table.

Due to VM storage limitations, the experiment used 8 monthly Parquet files:

- January 2026
- February 2026
- March 2026
- April 2026
- May 2026
- June 2026
- July 2026
- August 2026

The pipeline processed:

**28,390,564 rows**

The same input data was used for both Spark and Ray.

---

## Project Structure

```text
nyc_taxi/
├── data/
│   ├── Trips/
│   └── taxi_zone_lookup.csv
├── output/
│   ├── spark/
│   └── ray/
├── scripts/
│   └── spark_pipeline.py
├── spark_clean.py
├── ray_clean.py
└── data_engineering.ipynb
```
## Spark Cluster

The Spark cluster consists of:

1 Spark Master
2 Spark Workers
PySpark 3.3.4

Each worker was configured with 1 CPU core and 1 GB executor memory.

The Spark Master UI was used to verify the two active workers.

## Ray Cluster

The Ray cluster consists of:

1 Ray Head
2 Ray Workers
Ray 2.59.0
Ray Data

The Ray Dashboard was used to verify the distributed cluster and available
resources.

## Processing Pipeline
1. Ingestion

Eight monthly Parquet files were loaded from the shared data directory.

2. Cleansing

The preprocessing stage:

Filters invalid/null timestamps
Filters invalid trip distances
Filters invalid pickup/dropoff locations
Ensures dropoff occurs after pickup
Normalizes column data types
3. Location Join

The taxi zone lookup table was joined with the trip data using:

PULocationID
DOLocationID

The resulting dataset contains pickup and dropoff borough, zone, and
service-zone information.

4. Python Transformation

Average speed was calculated from trip distance and trip duration:

average_speed_mph =
    trip_distance / trip_duration_hours

This transformation was implemented using Python in both frameworks.

5. Export

The processed dataset was written to Parquet.

