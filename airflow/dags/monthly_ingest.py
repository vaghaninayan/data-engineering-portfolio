"""
monthly_ingest: TLC source -> MinIO Bronze -> MinIO Silver.

Runs on the 5th of each month and ingests the previous month's TLC data.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import docker
from airflow import DAG
from airflow.operators.python import PythonOperator

from _spark_runner import run_spark_job

log = logging.getLogger(__name__)


def ingest_tlc(**ctx) -> None:
    """Run the dep-ingestion image to download yellow + green for the prev month."""
    logical_date: datetime = ctx["logical_date"]
    target_month_first = (logical_date.replace(day=1) - timedelta(days=1)).replace(day=1)
    ym = target_month_first.strftime("%Y-%m")

    log.info("Ingesting TLC %s yellow + green into Bronze", ym)
    client = docker.from_env()
    result = client.containers.run(
        image="dep-ingestion:dev",
        command=["--start", ym, "--end", ym, "--types", "yellow,green"],
        environment={
            "MINIO_ENDPOINT":    os.environ["MINIO_ENDPOINT"],
            "MINIO_ACCESS_KEY":  os.environ["MINIO_ACCESS_KEY"],
            "MINIO_SECRET_KEY":  os.environ["MINIO_SECRET_KEY"],
        },
        network="data-engineering-portfolio_dep-net",
        remove=True,
        detach=False,
        stdout=True,
        stderr=True,
    )
    log.info(result.decode(errors="replace"))


def ensure_dim_location() -> None:
    """Idempotent upsert of TLC zone lookup into gold.dim_location."""
    log.info("Refreshing gold.dim_location")
    client = docker.from_env()
    result = client.containers.run(
        image="dep-ingestion:dev",
        entrypoint="python",
        command=["/app/load_dim_location.py"],
        environment={
            "POSTGRES_HOST":     "postgres",
            "POSTGRES_PORT":     "5432",
            "POSTGRES_DB":       os.environ["POSTGRES_DB"],
            "POSTGRES_USER":     os.environ["POSTGRES_USER"],
            "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
        },
        network="data-engineering-portfolio_dep-net",
        remove=True,
        detach=False,
        stdout=True,
        stderr=True,
    )
    log.info(result.decode(errors="replace"))


def run_bronze_to_silver(**ctx) -> None:
    year = ctx["logical_date"].year
    run_spark_job("bronze_to_silver.py", "--year", str(year))


default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

with DAG(
    dag_id="monthly_ingest",
    description="Ingest TLC monthly Parquet, refresh dimensions, run Bronze→Silver",
    default_args=default_args,
    start_date=datetime(2024, 1, 5),
    schedule="0 4 5 * *",
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "silver", "ingestion"],
) as dag:

    t_ingest = PythonOperator(
        task_id="ingest_tlc",
        python_callable=ingest_tlc,
    )

    t_dim = PythonOperator(
        task_id="ensure_dim_location",
        python_callable=ensure_dim_location,
    )

    t_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_bronze_to_silver,
    )

    [t_ingest, t_dim] >> t_silver
