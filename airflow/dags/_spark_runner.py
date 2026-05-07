"""
Shared helper: trigger a spark-submit by exec'ing into the spark-master
container via the Docker SDK. Returns the exit code; raises on non-zero.
"""
from __future__ import annotations

import logging
import os

import docker

log = logging.getLogger(__name__)

SPARK_PACKAGES = ",".join([
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.7.4",
])


def run_spark_job(job_filename: str, *job_args: str) -> None:
    """Exec spark-submit inside dep-spark-master. Raises if exit code != 0."""
    client = docker.from_env()
    container = client.containers.get("dep-spark-master")

    env = {
        "MINIO_ENDPOINT":    os.environ["MINIO_ENDPOINT"],
        "MINIO_ACCESS_KEY":  os.environ["MINIO_ACCESS_KEY"],
        "MINIO_SECRET_KEY":  os.environ["MINIO_SECRET_KEY"],
        "POSTGRES_HOST":     "postgres",
        "POSTGRES_PORT":     "5432",
        "POSTGRES_DB":       os.environ["POSTGRES_DB"],
        "POSTGRES_USER":     os.environ["POSTGRES_USER"],
        "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
    }

    cmd = [
        "spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "--packages", SPARK_PACKAGES,
        "--conf", "spark.driver.memory=1g",
        "--conf", "spark.executor.memory=1g",
        f"/opt/spark/jobs/{job_filename}",
        *job_args,
    ]

    log.info("Running in dep-spark-master: %s", " ".join(cmd))
    exit_code, output = container.exec_run(
        cmd=cmd, environment=env, stream=False, demux=False,
    )
    log_output = output.decode(errors="replace") if output else ""
    log.info("spark-submit exit_code=%s\n%s", exit_code, log_output[-3000:])

    if exit_code != 0:
        raise RuntimeError(
            f"spark-submit failed (exit={exit_code}). Last log lines:\n"
            f"{log_output[-2000:]}"
        )
