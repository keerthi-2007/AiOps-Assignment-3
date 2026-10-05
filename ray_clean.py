import os
import glob
import time
import shutil

import ray
import pandas as pd


# ============================================================
# Configuration
# ============================================================

TRIPS_PATH = "/opt/nyc_taxi/data/Trips"
LOOKUP_PATH = "/opt/nyc_taxi/data/taxi_zone_lookup.csv"
OUTPUT_PATH = "/opt/nyc_taxi/data/output/ray"

BATCH_SIZE = 10000
NUM_PARTITIONS = 12


# ============================================================
# Initialize Ray
# ============================================================

ray.init(address="auto", ignore_reinit_error=True)

print("\n" + "=" * 70)
print("RAY CLUSTER")
print("=" * 70)

print(ray.cluster_resources())


# ============================================================
# Helper functions
# ============================================================

REQUIRED_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "total_amount",
]


def normalize_batch(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize schema across yearly/monthly Parquet files."""

    df = df.copy()

    # Datetime columns
    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"], errors="coerce"
    )
    df["tpep_dropoff_datetime"] = pd.to_datetime(
        df["tpep_dropoff_datetime"], errors="coerce"
    )

    # Numeric columns
    df["passenger_count"] = pd.to_numeric(
        df["passenger_count"], errors="coerce"
    ).astype("float64")

    df["trip_distance"] = pd.to_numeric(
        df["trip_distance"], errors="coerce"
    ).astype("float64")

    df["PULocationID"] = pd.to_numeric(
        df["PULocationID"], errors="coerce"
    ).astype("int64")

    df["DOLocationID"] = pd.to_numeric(
        df["DOLocationID"], errors="coerce"
    ).astype("int64")

    df["payment_type"] = pd.to_numeric(
        df["payment_type"], errors="coerce"
    ).astype("float64")

    df["total_amount"] = pd.to_numeric(
        df["total_amount"], errors="coerce"
    ).astype("float64")

    return df


def clean_batch(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the same validity conditions as the Spark pipeline."""

    df = df.copy()

    valid = (
        df["tpep_pickup_datetime"].notna()
        & df["tpep_dropoff_datetime"].notna()
        & df["trip_distance"].notna()
        & df["PULocationID"].notna()
        & df["DOLocationID"].notna()
        & (df["trip_distance"] > 0)
        & (
            df["tpep_dropoff_datetime"]
            > df["tpep_pickup_datetime"]
        )
    )

    return df.loc[valid].reset_index(drop=True)


def calculate_average_speed(df: pd.DataFrame) -> pd.DataFrame:
    """
    Custom Python transformation corresponding to the Spark UDF.

    Average speed = trip distance / trip duration in hours.
    """

    df = df.copy()

    duration_seconds = (
        df["tpep_dropoff_datetime"]
        - df["tpep_pickup_datetime"]
    ).dt.total_seconds()

    duration_hours = duration_seconds / 3600.0

    df["average_speed_mph"] = (
        df["trip_distance"] / duration_hours
    )

    return df


# ============================================================
# Start benchmark
# ============================================================

pipeline_start = time.perf_counter()


# ============================================================
# 1. INGESTION
# ============================================================

print("\n" + "=" * 70)
print("1. INGESTION")
print("=" * 70)

trip_files = sorted(
    glob.glob(os.path.join(TRIPS_PATH, "*.parquet"))
)

print(f"Input files: {len(trip_files)}")

if not trip_files:
    raise RuntimeError(f"No Parquet files found in {TRIPS_PATH}")

ingestion_start = time.perf_counter()

# Read files individually because the source dataset has
# schema drift between months.
datasets = []

for path in trip_files:
    print("Reading:", os.path.basename(path))

    ds_file = ray.data.read_parquet(
        path,
        columns=REQUIRED_COLUMNS,
        override_num_blocks=2,
    )

    ds_file = ds_file.map_batches(
        normalize_batch,
        batch_format="pandas",
        batch_size=BATCH_SIZE,
    )

    datasets.append(ds_file)

ds = datasets[0]
for d in datasets[1:]:
    ds = ds.union(d)

ingestion_time = time.perf_counter() - ingestion_start

print(f"Ingestion preparation time: {ingestion_time:.2f} s")


# ============================================================
# 2. CLEANING
# ============================================================

print("\n" + "=" * 70)
print("2. CLEANING")
print("=" * 70)

cleaning_start = time.perf_counter()

ds = ds.map_batches(
    clean_batch,
    batch_format="pandas",
    batch_size=BATCH_SIZE,
)

# Ray 2.59 does not expose Dataset.drop_duplicates().
#
# Therefore we intentionally do NOT perform batch-local
# drop_duplicates(), because that would NOT be equivalent to
# Spark's global dropDuplicates().
#
# The source data was inspected for duplicates during the
# Spark-side validation. This limitation will be documented
# in the Performance Tuning / Parity note.

cleaning_time = time.perf_counter() - cleaning_start

print(f"Cleaning preparation time: {cleaning_time:.2f} s")


# ============================================================
# 3. LOCATION LOOKUP
# ============================================================

print("\n" + "=" * 70)
print("3. LOCATION LOOKUP")
print("=" * 70)

lookup_start = time.perf_counter()

lookup_df = pd.read_csv(LOOKUP_PATH)

lookup_df["LocationID"] = pd.to_numeric(
    lookup_df["LocationID"],
    errors="coerce",
).astype("int64")

lookup = ray.data.from_pandas(lookup_df)

print(f"Lookup rows: {len(lookup_df)}")


# ------------------------------------------------------------
# Pickup location join
# ------------------------------------------------------------

# Location enrichment
# The lookup table has only 265 rows, so use a broadcast-style
# local Pandas join inside each Ray batch instead of Dataset.join().
# This avoids the large distributed hash shuffle.

lookup_start = time.perf_counter()

lookup_df = pd.read_csv(LOOKUP_PATH)
lookup_df["LocationID"] = pd.to_numeric(
    lookup_df["LocationID"], errors="coerce"
).astype("Int64")

pickup_lookup = lookup_df.rename(columns={
    "LocationID": "PULocationID",
    "Borough": "PU_Borough",
    "Zone": "PU_Zone",
    "service_zone": "PU_service_zone",
})

dropoff_lookup = lookup_df.rename(columns={
    "LocationID": "DOLocationID",
    "Borough": "DO_Borough",
    "Zone": "DO_Zone",
    "service_zone": "DO_service_zone",
})


def enrich_locations(batch):
    # Inner joins preserve the logical behavior of the
    # original Dataset.join() operations.
    batch = batch.merge(
        pickup_lookup,
        on="PULocationID",
        how="inner",
        validate="many_to_one",
    )

    batch = batch.merge(
        dropoff_lookup,
        on="DOLocationID",
        how="inner",
        validate="many_to_one",
    )

    return batch


ds = ds.map_batches(
    enrich_locations,
    batch_format="pandas",
    batch_size=BATCH_SIZE,
)

join_time = time.perf_counter() - lookup_start
print(f"Join/enrichment time: {join_time:.2f} s")


# ============================================================
# 4. PYTHON UDF
# ============================================================

print("\n" + "=" * 70)
print("4. PYTHON UDF")
print("=" * 70)

udf_start = time.perf_counter()

ds = ds.map_batches(
    calculate_average_speed,
    batch_format="pandas",
    batch_size=BATCH_SIZE,
)

# This count triggers execution of the complete lazy pipeline.
row_count = ds.count()

udf_execution_time = time.perf_counter() - udf_start

print(f"Rows processed: {row_count:,}")
print(f"UDF/execution time: {udf_execution_time:.2f} s")


# ============================================================
# 5. EXPORT
# ============================================================

print("\n" + "=" * 70)
print("5. EXPORT")
print("=" * 70)

if os.path.exists(OUTPUT_PATH):
    shutil.rmtree(OUTPUT_PATH)

export_start = time.perf_counter()

ds.write_parquet(
    OUTPUT_PATH,
    mode="overwrite",
)

export_time = time.perf_counter() - export_start


# ============================================================
# Final benchmark
# ============================================================

total_time = time.perf_counter() - pipeline_start

print("\n" + "=" * 70)
print("RAY PIPELINE RESULTS")
print("=" * 70)

print(f"Input files:              {len(trip_files)}")
print(f"Rows processed:           {row_count:,}")
print(f"Ingestion preparation:    {ingestion_time:.2f} s")
print(f"Cleaning preparation:     {cleaning_time:.2f} s")
print(f"Join preparation:         {join_time:.2f} s")
print(f"UDF/execution stage:      {udf_execution_time:.2f} s")
print(f"Export time:              {export_time:.2f} s")
print(f"TOTAL PIPELINE TIME:      {total_time:.2f} s")
print(f"Output path:              {OUTPUT_PATH}")

print("=" * 70)

ray.shutdown()
