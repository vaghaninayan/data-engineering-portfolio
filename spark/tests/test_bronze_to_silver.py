"""
Unit tests for bronze_to_silver transformations.

Covers:
  - Schema conformance (yellow + green raw Parquet -> unified Silver schema)
  - Data quality filtering (drops obviously bad rows)
  - Trip-duration derivation
  - Deduplication
"""
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import Row

import sys
sys.path.insert(0, "/opt/spark/jobs")  # make jobs importable inside container

from bronze_to_silver import (
    conform_yellow,
    conform_green,
    apply_quality_filters,
    add_derived_columns,
    deduplicate,
    SILVER_COLUMNS,
)


# ---------------------------------------------------------------------------
# Conformance
# ---------------------------------------------------------------------------
def test_conform_yellow_produces_unified_schema(spark):
    """Yellow raw -> trip_type='yellow' + tpep timestamps renamed."""
    raw = spark.createDataFrame([
        Row(
            VendorID=1,
            tpep_pickup_datetime=datetime(2024, 1, 15, 8, 30),
            tpep_dropoff_datetime=datetime(2024, 1, 15, 8, 45),
            passenger_count=2,
            trip_distance=3.5,
            PULocationID=100,
            DOLocationID=200,
            fare_amount=15.5,
            tip_amount=2.0,
            total_amount=20.0,
        )
    ])
    result = conform_yellow(raw).collect()[0]
    assert result.trip_type == "yellow"
    assert result.pickup_datetime == datetime(2024, 1, 15, 8, 30)
    assert result.pu_location_id == 100


def test_conform_green_produces_unified_schema(spark):
    """Green raw -> trip_type='green' + lpep timestamps renamed."""
    raw = spark.createDataFrame([
        Row(
            VendorID=2,
            lpep_pickup_datetime=datetime(2024, 2, 10, 14, 0),
            lpep_dropoff_datetime=datetime(2024, 2, 10, 14, 20),
            passenger_count=1,
            trip_distance=1.8,
            PULocationID=42,
            DOLocationID=43,
            fare_amount=10.0,
            tip_amount=1.5,
            total_amount=12.5,
        )
    ])
    result = conform_green(raw).collect()[0]
    assert result.trip_type == "green"
    assert result.pickup_datetime == datetime(2024, 2, 10, 14, 0)


# ---------------------------------------------------------------------------
# Quality filtering
# ---------------------------------------------------------------------------
def _silver_row(**overrides):
    """Build a dict that's a valid Silver row by default; apply overrides."""
    base = dict(
        trip_type="yellow",
        vendor_id=1,
        pickup_datetime=datetime(2024, 3, 5, 10, 0),
        dropoff_datetime=datetime(2024, 3, 5, 10, 20),
        passenger_count=2,
        trip_distance=Decimal("3.5"),
        pu_location_id=100,
        do_location_id=200,
        fare_amount=Decimal("15.0"),
        tip_amount=Decimal("2.0"),
        total_amount=Decimal("20.0"),
    )
    base.update(overrides)
    return base


def test_quality_filter_keeps_valid_row(spark):
    df = spark.createDataFrame([_silver_row()])
    assert apply_quality_filters(df, year=2024).count() == 1


def test_quality_filter_drops_pickup_after_dropoff(spark):
    df = spark.createDataFrame([_silver_row(
        pickup_datetime=datetime(2024, 3, 5, 11, 0),
        dropoff_datetime=datetime(2024, 3, 5, 10, 0),  # earlier — invalid
    )])
    assert apply_quality_filters(df, year=2024).count() == 0


def test_quality_filter_drops_zero_passengers(spark):
    df = spark.createDataFrame([_silver_row(passenger_count=0)])
    assert apply_quality_filters(df, year=2024).count() == 0


def test_quality_filter_drops_too_many_passengers(spark):
    df = spark.createDataFrame([_silver_row(passenger_count=15)])
    assert apply_quality_filters(df, year=2024).count() == 0


def test_quality_filter_drops_zero_distance(spark):
    df = spark.createDataFrame([_silver_row(trip_distance=Decimal("0"))])
    assert apply_quality_filters(df, year=2024).count() == 0


def test_quality_filter_drops_negative_fare(spark):
    df = spark.createDataFrame([_silver_row(fare_amount=Decimal("-5.0"))])
    assert apply_quality_filters(df, year=2024).count() == 0


def test_quality_filter_drops_wrong_year(spark):
    df = spark.createDataFrame([_silver_row(
        pickup_datetime=datetime(2023, 12, 31, 23, 59),
        dropoff_datetime=datetime(2024, 1, 1, 0, 30),
    )])
    # Filtering for year=2024 should drop this 2023 pickup
    assert apply_quality_filters(df, year=2024).count() == 0


def test_quality_filter_drops_invalid_location(spark):
    df = spark.createDataFrame([_silver_row(pu_location_id=500)])  # >265
    assert apply_quality_filters(df, year=2024).count() == 0


# ---------------------------------------------------------------------------
# Derived columns
# ---------------------------------------------------------------------------
def test_add_derived_columns_computes_duration(spark):
    df = spark.createDataFrame([_silver_row(
        pickup_datetime=datetime(2024, 3, 5, 10, 0),
        dropoff_datetime=datetime(2024, 3, 5, 10, 20),  # 20 min = 1200s
    )])
    result = add_derived_columns(df).collect()[0]
    assert result.trip_duration_s == 1200
    assert str(result.pickup_date) == "2024-03-05"


def test_add_derived_columns_drops_too_short_trip(spark):
    """Trips < 30 seconds are filtered out as garbage."""
    df = spark.createDataFrame([_silver_row(
        pickup_datetime=datetime(2024, 3, 5, 10, 0, 0),
        dropoff_datetime=datetime(2024, 3, 5, 10, 0, 10),  # 10s
    )])
    assert add_derived_columns(df).count() == 0


def test_add_derived_columns_drops_too_long_trip(spark):
    """Trips > 6 hours are filtered out as data errors."""
    df = spark.createDataFrame([_silver_row(
        pickup_datetime=datetime(2024, 3, 5, 0, 0),
        dropoff_datetime=datetime(2024, 3, 5, 8, 0),  # 8 hours
    )])
    assert add_derived_columns(df).count() == 0


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------
def test_deduplicate_removes_exact_duplicates(spark):
    """Same trip ingested twice should collapse to one row."""
    rows = [_silver_row(), _silver_row()]
    df = spark.createDataFrame(rows)
    assert deduplicate(df).count() == 1


def test_deduplicate_keeps_distinct_trips(spark):
    """Two different trips at the same time but different fares are not duplicates."""
    rows = [
        _silver_row(total_amount=Decimal("20.0")),
        _silver_row(total_amount=Decimal("25.0")),
    ]
    df = spark.createDataFrame(rows)
    assert deduplicate(df).count() == 2


# ---------------------------------------------------------------------------
# Schema invariants
# ---------------------------------------------------------------------------
def test_silver_columns_constant_is_complete():
    """If someone adds a column to SILVER_COLUMNS without thinking, this catches it."""
    expected = {
        "trip_type", "vendor_id", "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance", "pu_location_id", "do_location_id",
        "fare_amount", "tip_amount", "total_amount", "trip_duration_s",
        "pickup_date",
    }
    assert set(SILVER_COLUMNS) == expected
