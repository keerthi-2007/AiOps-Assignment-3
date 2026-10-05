# DA3408 A3 — Spark vs. Ray: The Data Engineering Duel

## Overview

This project implements the same distributed data preprocessing pipeline using **Apache Spark** and **Ray Data** and compares their performance on NYC Yellow Taxi Trip Data.

The pipeline performs:

1. Ingestion of multiple Parquet files
2. Data cleansing and timestamp normalization
3. Location lookup joins
4. Average-speed calculation using a custom Python transformation
5. Parquet export

Both frameworks were deployed as distributed clusters with **one head/master and two workers**.

---

## Dataset

NYC Yellow Taxi Trip Data was used along with the NYC Taxi Zone Lookup table.

Due to VM storage limitations, the experiment used **8 monthly Parquet files**:

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

> The large NYC Taxi Parquet files are not included in the GitHub repository due to their size.

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

---

## Spark Cluster

The Spark cluster consists of:

- 1 Spark Master
- 2 Spark Workers
- PySpark 3.3.4

Each worker was configured with:

- 1 CPU core
- 1 GB executor memory

The Spark Master UI was used to verify the two active workers.

**Spark Cluster Architecture**

```text
                 Spark Master
                      |
             +--------+--------+
             |                 |
             v                 v
       Spark Worker A    Spark Worker B
        1 CPU / 1 GB      1 CPU / 1 GB
```

---

## Ray Cluster

The Ray cluster consists of:

- 1 Ray Head
- 2 Ray Workers
- Ray 2.59.0
- Ray Data

Each Ray node was configured with 1 CPU.

The Ray Dashboard was used to verify the distributed cluster and available resources.

**Ray Cluster Architecture**

```text
                   Ray Head
                      |
             +--------+--------+
             |                 |
             v                 v
        Ray Worker A      Ray Worker B
             1 CPU              1 CPU
```

---

## Data Access and Docker Setup

The NYC Taxi data was stored on the VM rather than copied into the Docker images.

The host data directory was mounted into the containers using Docker volumes.

```text
Host VM
/home/ariro/nyc_taxi/data/
            |
            | Docker volume mount
            v
      Spark / Ray containers
            |
            v
     Parquet input files
```

This allowed the Spark and Ray workers to access the same input dataset without creating large Docker images.

---

## Basic Commands

### 1. Start the Spark Cluster

Navigate to the Spark Docker Compose directory:

```bash
cd ~/docker-spark-cluster
```

Start the Spark cluster:

```bash
docker compose up -d
```

Check the running containers:

```bash
docker ps
```

The Spark Master UI is available at:http://localhost:9090

The Spark application UI is available at:http://localhost:4040


### 2. Run the Spark Pipeline

Copy the Spark application into the Spark master container:

```bash
docker cp ~/nyc_taxi/scripts/spark_pipeline.py spark_master:/opt/spark-apps/
```

Submit the Spark application:

```bash
docker exec -it spark_master spark-submit \
    --master spark://spark_master:7077 \
    /opt/spark-apps/spark_pipeline.py
```

The processed output is written to:data/output/spark/

### 3. Start the Ray Cluster

Navigate to the Ray cluster directory:

```bash
cd ~/ray-cluster
```

Start the Ray head and workers:

```bash
docker compose up -d
```

Check the running containers:

```bash
docker ps
```

Verify the Ray cluster:

```bash
docker exec -it ray_head ray status
```

The Ray Dashboard is available at:http://localhost:8265

### 4. Run the Ray Pipeline

The Ray pipeline can be executed from the Ray head node:

```bash
docker exec -it ray_head python /opt/nyc_taxi/ray_pipeline.py
```

The pipeline connects to the running Ray cluster and processes the same input data used by Spark.

The processed output is written to:data/output/ray/


### 5. Check Resource Usage

Docker-level CPU and memory usage can be inspected using:

**Ray**

```bash
docker stats --no-stream ray_head ray_worker_a ray_worker_b
```

**Spark**

```bash
docker stats --no-stream spark_master spark_worker_a spark_worker_b
```

These commands provide a point-in-time snapshot of CPU and memory usage.

### 6. Stop the Clusters

Stop the Spark cluster:

```bash
cd ~/docker-spark-cluster
docker compose down
```

Stop the Ray cluster:

```bash
cd ~/ray-cluster
docker compose down
```

---

## Processing Pipeline

### 1. Ingestion

Eight monthly Parquet files were loaded from the shared data directory.

The same input files were used for both Spark and Ray.

### 2. Cleansing

The preprocessing stage:

- Filters invalid/null timestamps
- Filters invalid trip distances
- Filters invalid pickup/dropoff locations
- Ensures dropoff occurs after pickup
- Normalizes column data types

The logical cleaning pipeline was matched between Spark and Ray.

However, explicit global duplicate removal was not available through the Ray Data API used in this experiment.

### 3. Location Join

The taxi zone lookup table was joined with the trip data using:

- `PULocationID`
- `DOLocationID`

The resulting dataset contains pickup and dropoff:

- Borough
- Zone
- Service zone

### 4. Python Transformation

Average speed was calculated from trip distance and trip duration:average_speed_mph = trip_distance / trip_duration_hours


The transformation checks for invalid or zero-duration trips before calculating the speed.

The transformation was implemented using Python in both frameworks.

### 5. Export

The processed dataset was written to Parquet.

---

## Output Validation

Both Spark and Ray produced:

- 28,390,564 rows
- The same 15 output columns
- Pickup and dropoff location information
- `average_speed_mph`
- Parquet output

The validation establishes structural and logical parity based on row count and schema.

A complete row-by-row equality comparison was not performed.

---

## Benchmark Results

The same dataset and logical processing pipeline were used for both frameworks.

| Stage           | Spark    | Ray       |
|------------------|---------:|----------:|
| Ingestion        | 5.25 s   | 0.38 s    |
| Cleaning         | 0.05 s   | 0.00 s    |
| Join             | 0.03 s   | 0.00 s    |
| UDF / Execution  | 35.30 s  | 583.98 s  |
| Export           | 108.74 s | 783.13 s  |
| **Total Pipeline** | **150.11 s** | **1367.96 s** |

Spark completed the complete pipeline in:

**150.11 seconds**

Ray completed the complete pipeline in:

**1367.96 seconds**

Therefore, Spark was approximately:

**9.11× faster overall**

The major performance difference occurred in the UDF/execution and export stages.

### UDF / Execution Analysis

The measured UDF/execution stage was:

- Spark: 35.30 seconds
- Ray: 583.98 seconds

This corresponds to approximately:

**16.54× faster for Spark**

However, this measurement should not be interpreted as a pure Python-function benchmark. Due to lazy execution and materialization, the measured stage can include upstream execution required to produce the result.

### Export Analysis

The export stage was:

- Spark: 108.74 seconds
- Ray: 783.13 seconds

Spark was approximately **7.20× faster** during export.

This stage contributed substantially to the overall difference between the two frameworks.

---

## Resource Utilization

Container-level CPU and memory usage was monitored using Docker Stats.

The captured values represent a point-in-time snapshot, not peak utilization over the complete pipeline.

| Framework | Container | CPU    | Memory    |
|-----------|-----------|-------:|----------:|
| Ray       | Head      | 3.51%  | 1.073 GiB |
| Ray       | Worker A  | 1.08%  | 321 MiB   |
| Ray       | Worker B  | 0.92%  | 321 MiB   |
| Spark     | Master    | 0.07%  | 161.7 MiB |
| Spark     | Worker A  | 0.08%  | 136.2 MiB |
| Spark     | Worker B  | 0.07%  | 149.3 MiB |

---

## Performance Discussion

Spark performed significantly better for this particular workload.

The workload is primarily structured ETL, involving:

- Parquet ingestion
- Data cleansing
- Structured joins
- Python-based transformation
- Parquet export

Spark is highly optimized for large-scale structured data processing, which makes it well suited to this workload.

Ray is a general distributed Python framework with strong support for Python-native and AI/ML workloads. However, being Python-native does not automatically make Ray faster for every type of data-processing workload.

The observed performance is dependent on:

- Dataset
- Workload
- Cluster configuration
- Available CPU and memory
- Framework implementation
- Storage and I/O characteristics

Therefore, the results should not be interpreted as a universal statement that Spark is always faster than Ray.

---

## Limitations

The experiment has the following limitations:

- Due to VM storage constraints, only 8 monthly files from January–August 2026 were used.
- The Spark and Ray clusters were relatively small.
- Each worker had only one CPU.
- Resource measurements were point-in-time snapshots rather than continuous peak measurements.
- The UDF/execution timing does not isolate pure Python function execution.
- A complete row-by-row equality comparison between Spark and Ray outputs was not performed.
- Explicit global duplicate removal was not available through the Ray Data API used in the experiment.

---

## Performance Tuning Note

AI tools were used as a supporting resource during development.

AI was used to:

- Debug Docker and Spark/Ray configuration issues
- Understand distributed data-processing concepts
- Translate equivalent processing logic between PySpark and Ray Data
- Help structure documentation and benchmark analysis

The cluster setup, commands, experiments, benchmarking, testing, and final results were executed and verified manually.

---

## Reproducibility

To reproduce the experiment:

1. Place the NYC Taxi Parquet files inside: `data/Trips/`
2. Place the taxi zone lookup table at: `data/taxi_zone_lookup.csv`
3. Start the Spark cluster and run the Spark pipeline.
4. Start the Ray cluster and run the Ray pipeline.
5. Verify the outputs under: `output/spark/` and `output/ray/`
6. Compare execution times and output schemas.

---

## Conclusion

The experiment compared Apache Spark and Ray Data using the same NYC Taxi preprocessing workload and the same input dataset.

Spark completed the pipeline in 150.11 seconds, while Ray required 1367.96 seconds.

Thus, Spark was approximately **9.11× faster** in the tested configuration.

The results demonstrate that framework performance depends strongly on the workload and execution environment. Spark was better suited to this structured ETL workload, while Ray's Python-native distributed architecture remains useful for Python-heavy and AI/ML-oriented workloads.
