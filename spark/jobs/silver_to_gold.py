"""
Silver -> Gold transformation.

Reads cleaned trip records from s3a://silver/trips, aggregates them to one row
per (pickup_hour, pu_location_id, vendor_id, trip_type), and writes the result
into the gold.fact_trips_hourly Postgres table.

Also records the run in gold.pipeline_run for lineage tracking.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from _spark_session import build_spark

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("silver_to_gold")


# Postgres JDBC driver — pulled at runtime by spark-submit --packages
JDBC_URL_TEMPLATE = "jdbc:postgresql://{host}:{port}/{db}"
JDBC_DRIVER = "org.postgresql.Driver"


# ---------------------------------------------------------------------------
# Aggregation — pure function, easy to test
# ---------------------------------------------------------------------------
def aggregate_hourly(silver_df: DataFrame, run_id: str) -> DataFrame:
    """
    Aggregate Silver trips to one row per (pickup_hour, pu_location_id,
    vendor_id, trip_type). Output schema matches gold.fact_trips_hourly.
    """
    return (
        silver_df
        .withColumn("pickup_hour", F.date_trunc("hour", "pickup_datetime"))
        .groupBy("pickup_hour", "pu_location_id", "vendor_id", "trip_type")
        .agg(
            F.count("*").cast(IntegerType()).alias("trip_count"),
            F.sum("passenger_count").cast(IntegerType()).alias("total_passengers"),
            F.sum("fare_amount").alias("sum_fare_amount"),
            F.sum("tip_amount").alias("sum_tip_amount"),
            F.sum("trip_distance").alias("sum_trip_distance"),
            F.avg("trip_duration_s").alias("avg_trip_duration_s"),
        )
        .withColumn("source_run_id", F.lit(run_id))
        .select(
            "pickup_hour", "pu_location_id", "vendor_id", "trip_type",
            "trip_count", "total_passengers",
            "sum_fare_amount", "sum_tip_amount", "sum_trip_distance",
            "avg_trip_duration_s", "source_run_id",
        )
    )


# ---------------------------------------------------------------------------
# Postgres I/O
# ---------------------------------------------------------------------------
def jdbc_props() -> tuple[str, dict]:
    """Build (jdbc_url, props) from environment variables."""
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db   = os.environ["POSTGRES_DB"]
    url = JDBC_URL_TEMPLATE.format(host=host, port=port, db=db)
    props = {
        "user":     os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver":   JDBC_DRIVER,
        "stringtype": "unspecified",
    }
    return url, props


def write_gold(df: DataFrame, table: str, mode: str = "overwrite") -> None:
    url, props = jdbc_props()
    log.info("Writing %s rows to %s (mode=%s)", df.count(), table, mode)
    (
        df.write
        .mode(mode)
        .option("truncate", "true")
        .jdbc(url=url, table=table, properties=props)
    )


def record_run(spark: SparkSession, run_id: str, dag_id: str, task_id: str,
               started_at: datetime, finished_at: datetime,
               rows_in: int, rows_out: int, status: str,
               error: str | None) -> None:
    """Append a lineage row to gold.pipeline_run."""
    row = spark.createDataFrame([(
        run_id, dag_id, task_id, "silver_to_gold",
        started_at, finished_at, status, rows_in, rows_out, error,
    )], schema="""
        run_id string, dag_id string, task_id string, job_name string,
        started_at timestamp, finished_at timestamp,
        status string, rows_in long, rows_out long, error_message string
    """)
    url, props = jdbc_props()
    row.write.mode("append").jdbc(url=url, table="gold.pipeline_run", properties=props)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run(spark: SparkSession, silver_uri: str, run_id: str,
        dag_id: str, task_id: str) -> None:
    started = datetime.now(timezone.utc)
    log.info("Reading Silver from %s", silver_uri)

    silver = spark.read.parquet(silver_uri)
    rows_in = silver.count()
    log.info("Silver rows in: %s", rows_in)

    gold_df = aggregate_hourly(silver, run_id).cache()
    rows_out = gold_df.count()
    log.info("Gold rows out: %s", rows_out)

    try:
        write_gold(gold_df, "gold.fact_trips_hourly", mode="overwrite")
        finished = datetime.now(timezone.utc)
        record_run(spark, run_id, dag_id, task_id, started, finished,
                   rows_in, rows_out, "success", None)
        log.info("Gold write complete (run_id=%s)", run_id)
    except Exception as e:
        finished = datetime.now(timezone.utc)
        record_run(spark, run_id, dag_id, task_id, started, finished,
                   rows_in, 0, "failed", str(e)[:500])
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver", default="s3a://silver/trips")
    parser.add_argument("--run-id", default=None,
                        help="Lineage run ID; auto-generated if omitted")
    parser.add_argument("--dag-id", default="manual")
    parser.add_argument("--task-id", default="silver_to_gold")
    args = parser.parse_args()

    run_id = args.run_id or f"manual-{uuid.uuid4().hex[:12]}"

    spark = build_spark(f"silver_to_gold_{run_id}")
    try:
        run(spark, args.silver, run_id, args.dag_id, args.task_id)
    except Exception as e:
        log.error("Job failed: %s", e, exc_info=True)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
