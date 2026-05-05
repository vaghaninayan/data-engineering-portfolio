"""
Bronze -> Silver transformation.

Reads raw NYC TLC monthly Parquet from s3a://bronze/, normalises the differing
yellow vs. green schemas into one unified schema, cleans invalid rows, removes
duplicates, and writes the result to s3a://silver/ partitioned by trip_type
and pickup date.

Run via spark-submit (see scripts/run_bronze_to_silver.sh).
"""
from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, IntegerType, ShortType, StringType, TimestampType,
)

from _spark_session import build_spark

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bronze_to_silver")


# ---------------------------------------------------------------------------
# Unified Silver schema (lowercase snake_case, types aligned with Postgres)
# ---------------------------------------------------------------------------
SILVER_COLUMNS = [
    "trip_type",         # 'yellow' | 'green'
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pu_location_id",
    "do_location_id",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "trip_duration_s",
    "pickup_date",       # partition key (DATE)
]


# ---------------------------------------------------------------------------
# Schema conformance — turn raw yellow/green frames into the unified shape
# ---------------------------------------------------------------------------
def conform_yellow(df: DataFrame) -> DataFrame:
    """Yellow uses tpep_* prefixes for timestamps."""
    return (
        df.select(
            F.lit("yellow").alias("trip_type"),
            F.col("VendorID").cast(ShortType()).alias("vendor_id"),
            F.col("tpep_pickup_datetime").cast(TimestampType()).alias("pickup_datetime"),
            F.col("tpep_dropoff_datetime").cast(TimestampType()).alias("dropoff_datetime"),
            F.col("passenger_count").cast(IntegerType()).alias("passenger_count"),
            F.col("trip_distance").cast(DecimalType(14, 2)).alias("trip_distance"),
            F.col("PULocationID").cast(ShortType()).alias("pu_location_id"),
            F.col("DOLocationID").cast(ShortType()).alias("do_location_id"),
            F.col("fare_amount").cast(DecimalType(12, 2)).alias("fare_amount"),
            F.col("tip_amount").cast(DecimalType(12, 2)).alias("tip_amount"),
            F.col("total_amount").cast(DecimalType(12, 2)).alias("total_amount"),
        )
    )


def conform_green(df: DataFrame) -> DataFrame:
    """Green uses lpep_* prefixes for timestamps."""
    return (
        df.select(
            F.lit("green").alias("trip_type"),
            F.col("VendorID").cast(ShortType()).alias("vendor_id"),
            F.col("lpep_pickup_datetime").cast(TimestampType()).alias("pickup_datetime"),
            F.col("lpep_dropoff_datetime").cast(TimestampType()).alias("dropoff_datetime"),
            F.col("passenger_count").cast(IntegerType()).alias("passenger_count"),
            F.col("trip_distance").cast(DecimalType(14, 2)).alias("trip_distance"),
            F.col("PULocationID").cast(ShortType()).alias("pu_location_id"),
            F.col("DOLocationID").cast(ShortType()).alias("do_location_id"),
            F.col("fare_amount").cast(DecimalType(12, 2)).alias("fare_amount"),
            F.col("tip_amount").cast(DecimalType(12, 2)).alias("tip_amount"),
            F.col("total_amount").cast(DecimalType(12, 2)).alias("total_amount"),
        )
    )


# ---------------------------------------------------------------------------
# Data quality: drop rows that are obviously garbage
# ---------------------------------------------------------------------------
def apply_quality_filters(df: DataFrame, year: int) -> DataFrame:
    """Filter out rows that fail basic sanity checks."""
    return df.filter(
        # Timestamps must be ordered and within plausible year
        (F.col("pickup_datetime").isNotNull()) &
        (F.col("dropoff_datetime").isNotNull()) &
        (F.col("pickup_datetime") < F.col("dropoff_datetime")) &
        (F.year("pickup_datetime") == F.lit(year)) &
        # Passenger count: 1..8 (bigger groups don't fit a NYC cab)
        (F.col("passenger_count").isNotNull()) &
        (F.col("passenger_count") >= 1) &
        (F.col("passenger_count") <= 8) &
        # Distance: 0 < d < 200 miles (reject cross-state trips & garbage)
        (F.col("trip_distance") > 0) &
        (F.col("trip_distance") < 200) &
        # Fares: positive and bounded
        (F.col("fare_amount") > 0) &
        (F.col("fare_amount") < 1000) &
        (F.col("total_amount") > 0) &
        (F.col("total_amount") < 1000) &
        # Location IDs: 1..265 per TLC zone CSV
        (F.col("pu_location_id").between(1, 265)) &
        (F.col("do_location_id").between(1, 265))
    )


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Compute trip duration and partition key."""
    return df.withColumn(
        "trip_duration_s",
        (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long"))
        .cast(IntegerType())
    ).withColumn(
        "pickup_date",
        F.to_date("pickup_datetime")
    ).filter(
        # Trip duration sanity: 30 seconds to 6 hours
        (F.col("trip_duration_s") >= 30) &
        (F.col("trip_duration_s") <= 21600)
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Exact duplicates from re-ingestion: same vendor, pickup, dropoff, fare."""
    return df.dropDuplicates(
        ["trip_type", "vendor_id", "pickup_datetime", "dropoff_datetime",
         "pu_location_id", "do_location_id", "total_amount"]
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run(spark: SparkSession, year: int, bronze_uri: str, silver_uri: str) -> None:
    log.info("Reading Bronze yellow + green for year=%s", year)

    # readPath patterns include all months for the requested year
    yellow_path = f"{bronze_uri}/yellow/year={year:04d}/*/*.parquet"
    green_path  = f"{bronze_uri}/green/year={year:04d}/*/*.parquet"

    yellow_raw = spark.read.parquet(yellow_path)
    green_raw  = spark.read.parquet(green_path)

    log.info("Yellow rows in: %s", yellow_raw.count())
    log.info("Green  rows in: %s", green_raw.count())

    yellow = conform_yellow(yellow_raw)
    green  = conform_green(green_raw)

    unified = yellow.unionByName(green)

    cleaned = (
        unified
        .transform(lambda d: apply_quality_filters(d, year))
        .transform(add_derived_columns)
        .transform(deduplicate)
        .select(*SILVER_COLUMNS)
    )

    # Cache once because we both count and write
    cleaned.cache()
    rows_out = cleaned.count()
    log.info("Silver rows out (after cleaning + dedup): %s", rows_out)

    log.info("Writing Silver to %s (partitioned by trip_type, pickup_date)", silver_uri)
    (
        cleaned.write
        .mode("overwrite")
        .partitionBy("trip_type", "pickup_date")
        .parquet(silver_uri)
    )
    log.info("Silver write complete.")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--bronze", default="s3a://bronze")
    parser.add_argument("--silver", default="s3a://silver/trips")
    args = parser.parse_args()

    spark = build_spark(f"bronze_to_silver_{args.year}")
    try:
        run(spark, args.year, args.bronze, args.silver)
    except Exception as e:
        log.error("Job failed: %s", e, exc_info=True)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
