from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import glob
import os
import time


# ============================================================
# 1. Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Spark-Pipeline")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

DATA_PATH = "/opt/spark-data/Trips"
LOOKUP_PATH = "/opt/spark-data/taxi_zone_lookup.csv"
OUTPUT_PATH = "/opt/spark-data/output/spark"


# ============================================================
# 2. Python UDF
#    Average Speed = Trip Distance / Trip Duration (hours)
# ============================================================

def calculate_average_speed(distance, pickup, dropoff):
    if distance is None or pickup is None or dropoff is None:
        return None

    duration_seconds = (dropoff - pickup).total_seconds()

    if duration_seconds <= 0:
        return None

    duration_hours = duration_seconds / 3600.0
    return float(distance) / duration_hours


average_speed_udf = F.udf(
    calculate_average_speed,
    DoubleType()
)


# ============================================================
# 3. Find input files
# ============================================================

input_files = sorted(
    glob.glob(os.path.join(DATA_PATH, "*.parquet"))
)

print("\n========================================")
print("NYC TAXI SPARK PIPELINE")
print("========================================")
print(f"Input files: {len(input_files)}")

for f in input_files:
    print(" -", os.path.basename(f))

print("========================================\n")


# ============================================================
# 4. INGESTION
# ============================================================

pipeline_start = time.time()
ingestion_start = time.time()

# Read files individually because the downloaded NYC Taxi
# files have some schema differences between months.

required_columns = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "total_amount"
]

trip_dfs = []

for path in input_files:

    df = spark.read.parquet(path)

    # Select only columns required by our pipeline
    df = df.select(*[
        c for c in required_columns
        if c in df.columns
    ])

    # Standardize data types
    df = (
        df
        .withColumn(
            "tpep_pickup_datetime",
            F.col("tpep_pickup_datetime").cast("timestamp")
        )
        .withColumn(
            "tpep_dropoff_datetime",
            F.col("tpep_dropoff_datetime").cast("timestamp")
        )
        .withColumn(
            "passenger_count",
            F.col("passenger_count").cast("double")
        )
        .withColumn(
            "trip_distance",
            F.col("trip_distance").cast("double")
        )
        .withColumn(
            "PULocationID",
            F.col("PULocationID").cast("long")
        )
        .withColumn(
            "DOLocationID",
            F.col("DOLocationID").cast("long")
        )
        .withColumn(
            "payment_type",
            F.col("payment_type").cast("long")
        )
        .withColumn(
            "total_amount",
            F.col("total_amount").cast("double")
        )
    )

    trip_dfs.append(df)


# Combine all monthly files
trips = trip_dfs[0]

for df in trip_dfs[1:]:
    trips = trips.unionByName(df, allowMissingColumns=True)


ingestion_time = time.time() - ingestion_start

print(f"Ingestion preparation time: {ingestion_time:.2f} seconds")


# ============================================================
# 5. CLEANING
# ============================================================

cleaning_start = time.time()

cleaned = (
    trips
    # Remove rows with missing essential values
    .filter(F.col("tpep_pickup_datetime").isNotNull())
    .filter(F.col("tpep_dropoff_datetime").isNotNull())
    .filter(F.col("trip_distance").isNotNull())
    .filter(F.col("PULocationID").isNotNull())
    .filter(F.col("DOLocationID").isNotNull())

    # Remove invalid trips
    .filter(F.col("trip_distance") > 0)
    .filter(
        F.col("tpep_dropoff_datetime")
        > F.col("tpep_pickup_datetime")
    )

    # Remove duplicates
    .dropDuplicates()
)

cleaning_time = time.time() - cleaning_start

print(f"Cleaning preparation time: {cleaning_time:.2f} seconds")


# ============================================================
# 6. LOAD TAXI ZONE LOOKUP
# ============================================================

lookup = (
    spark.read
    .option("header", True)
    .csv(LOOKUP_PATH)
)

lookup = (
    lookup
    .withColumn("LocationID", F.col("LocationID").cast("long"))
    .select(
        "LocationID",
        "Borough",
        "Zone",
        "service_zone"
    )
)


# ============================================================
# 7. JOIN — Pickup Location
# ============================================================

join_start = time.time()

pickup_lookup = lookup.select(
    F.col("LocationID").alias("PU_LocationID"),
    F.col("Borough").alias("PU_Borough"),
    F.col("Zone").alias("PU_Zone"),
    F.col("service_zone").alias("PU_ServiceZone")
)

joined = cleaned.join(
    pickup_lookup,
    cleaned.PULocationID == pickup_lookup.PU_LocationID,
    "left"
).drop("PU_LocationID")


# ============================================================
# 8. JOIN — Dropoff Location
# ============================================================

dropoff_lookup = lookup.select(
    F.col("LocationID").alias("DO_LocationID"),
    F.col("Borough").alias("DO_Borough"),
    F.col("Zone").alias("DO_Zone"),
    F.col("service_zone").alias("DO_ServiceZone")
)

joined = joined.join(
    dropoff_lookup,
    joined.DOLocationID == dropoff_lookup.DO_LocationID,
    "left"
).drop("DO_LocationID")


join_time = time.time() - join_start

print(f"Join preparation time: {join_time:.2f} seconds")


# ============================================================
# 9. PYTHON UDF TRANSFORMATION
# ============================================================

udf_start = time.time()

processed = joined.withColumn(
    "average_speed_mph",
    average_speed_udf(
        F.col("trip_distance"),
        F.col("tpep_pickup_datetime"),
        F.col("tpep_dropoff_datetime")
    )
)

# Force Spark to execute the UDF
processed_count = processed.count()

udf_time = time.time() - udf_start

print(f"Rows after processing: {processed_count:,}")
print(f"Python UDF execution time: {udf_time:.2f} seconds")


# ============================================================
# 10. EXPORT TO PARQUET
# ============================================================

export_start = time.time()

(
    processed
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

export_time = time.time() - export_start


# ============================================================
# 11. TOTAL BENCHMARK
# ============================================================

total_time = time.time() - pipeline_start

print("\n========================================")
print("PIPELINE COMPLETE")
print("========================================")
print(f"Input files              : {len(input_files)}")
print(f"Rows processed           : {processed_count:,}")
print(f"Ingestion time           : {ingestion_time:.2f} s")
print(f"Cleaning time            : {cleaning_time:.2f} s")
print(f"Join time                : {join_time:.2f} s")
print(f"Python UDF time          : {udf_time:.2f} s")
print(f"Export time              : {export_time:.2f} s")
print(f"TOTAL PIPELINE TIME      : {total_time:.2f} s")
print(f"Output                   : {OUTPUT_PATH}")
print("========================================\n")


spark.stop()

