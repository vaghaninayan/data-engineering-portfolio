"""Unit tests for silver_to_gold aggregation logic."""
from datetime import datetime
from decimal import Decimal

import sys
sys.path.insert(0, "/opt/spark/jobs")

from pyspark.sql import Row
from silver_to_gold import aggregate_hourly


def _silver_row(**overrides):
    base = dict(
        trip_type="yellow",
        vendor_id=1,
        pickup_datetime=datetime(2024, 3, 5, 10, 15),
        dropoff_datetime=datetime(2024, 3, 5, 10, 30),
        passenger_count=2,
        trip_distance=Decimal("3.5"),
        pu_location_id=100,
        do_location_id=200,
        fare_amount=Decimal("15.0"),
        tip_amount=Decimal("2.0"),
        total_amount=Decimal("20.0"),
        trip_duration_s=900,
        pickup_date=datetime(2024, 3, 5).date(),
    )
    base.update(overrides)
    return base


def test_aggregate_hourly_collapses_same_hour(spark):
    """Two trips in the same hour, same zone, same vendor -> one Gold row."""
    rows = [
        _silver_row(pickup_datetime=datetime(2024, 3, 5, 10, 15), fare_amount=Decimal("15.0")),
        _silver_row(pickup_datetime=datetime(2024, 3, 5, 10, 45), fare_amount=Decimal("20.0")),
    ]
    silver = spark.createDataFrame(rows)
    gold = aggregate_hourly(silver, run_id="test-run").collect()
    assert len(gold) == 1
    g = gold[0]
    assert g.trip_count == 2
    assert g.total_passengers == 4
    assert g.sum_fare_amount == Decimal("35.00")
    assert g.pickup_hour == datetime(2024, 3, 5, 10, 0)


def test_aggregate_hourly_separates_different_hours(spark):
    """Trips in different hours produce separate Gold rows."""
    rows = [
        _silver_row(pickup_datetime=datetime(2024, 3, 5, 10, 0)),
        _silver_row(pickup_datetime=datetime(2024, 3, 5, 11, 0)),
    ]
    silver = spark.createDataFrame(rows)
    gold = aggregate_hourly(silver, run_id="test-run").collect()
    assert len(gold) == 2


def test_aggregate_hourly_separates_different_zones(spark):
    """Same hour, different pickup zones produce separate rows."""
    rows = [
        _silver_row(pu_location_id=100),
        _silver_row(pu_location_id=200),
    ]
    silver = spark.createDataFrame(rows)
    gold = aggregate_hourly(silver, run_id="test-run").collect()
    assert len(gold) == 2


def test_aggregate_hourly_separates_yellow_and_green(spark):
    """Yellow and green trips never combine into one Gold row."""
    rows = [
        _silver_row(trip_type="yellow"),
        _silver_row(trip_type="green"),
    ]
    silver = spark.createDataFrame(rows)
    gold = aggregate_hourly(silver, run_id="test-run").collect()
    assert len(gold) == 2
    assert {g.trip_type for g in gold} == {"yellow", "green"}


def test_aggregate_hourly_stamps_run_id(spark):
    """Every Gold row records which job run produced it."""
    silver = spark.createDataFrame([_silver_row()])
    gold = aggregate_hourly(silver, run_id="my-test-run-123").collect()
    assert gold[0].source_run_id == "my-test-run-123"


def test_aggregate_hourly_avg_duration(spark):
    """avg_trip_duration_s is the mean of trip_duration_s."""
    rows = [
        _silver_row(trip_duration_s=600),
        _silver_row(trip_duration_s=1800),
    ]
    silver = spark.createDataFrame(rows)
    gold = aggregate_hourly(silver, run_id="test-run").collect()
    assert float(gold[0].avg_trip_duration_s) == 1200.0
