"""
quarterly_aggregate: Silver -> Postgres Gold.

Runs on the 1st of Jan/Apr/Jul/Oct and aggregates Silver into gold.fact_trips_hourly.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from _spark_runner import run_spark_job


def run_silver_to_gold(**ctx) -> None:
    run_id = f"airflow-{ctx['run_id']}"
    run_spark_job(
        "silver_to_gold.py",
        "--run-id", run_id,
        "--dag-id", ctx["dag"].dag_id,
        "--task-id", ctx["task_instance"].task_id,
    )


default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

with DAG(
    dag_id="quarterly_aggregate",
    description="Aggregate Silver -> Gold (hourly facts) every quarter",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 6 1 1,4,7,10 *",
    catchup=False,
    max_active_runs=1,
    tags=["silver", "gold", "aggregation"],
) as dag:

    t_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=run_silver_to_gold,
    )
