"""Shared pytest fixtures."""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """One SparkSession for the whole test run — startup is the slow part."""
    s = (
        SparkSession.builder
        .appName("unit-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
