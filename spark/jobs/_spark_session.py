"""
Shared Spark session builder.

Centralises all the S3A / MinIO configuration so individual jobs stay focused
on transformation logic rather than boilerplate.

S3A is Hadoop's S3-compatible filesystem driver. MinIO speaks S3, so Spark
talks to MinIO using the same client code it would use for AWS S3.
"""
from __future__ import annotations

import os
from pyspark.sql import SparkSession


# Hadoop-AWS package bundle pulled at runtime by spark-submit --packages.
# Version 3.3.4 matches the Hadoop client bundled with Spark 3.5.x.
HADOOP_AWS_VERSION = "3.3.4"
AWS_SDK_VERSION = "1.12.262"


def build_spark(app_name: str) -> SparkSession:
    """Build a SparkSession configured for MinIO."""
    endpoint    = os.environ["MINIO_ENDPOINT"]            # e.g. http://minio:9000
    access_key  = os.environ["MINIO_ACCESS_KEY"]
    secret_key  = os.environ["MINIO_SECRET_KEY"]

    # Strip protocol prefix if present — S3A wants just "host:port"
    s3a_endpoint = endpoint.replace("http://", "").replace("https://", "")

    builder = (
        SparkSession.builder
        .appName(app_name)
        # ---- S3A endpoint pointed at MinIO ----------------------------------
        .config("spark.hadoop.fs.s3a.endpoint",            f"http://{s3a_endpoint}")
        .config("spark.hadoop.fs.s3a.access.key",          access_key)
        .config("spark.hadoop.fs.s3a.secret.key",          secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access",   "true")     # required for MinIO
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")  # MinIO is HTTP locally
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # ---- Performance: small files + commit safety -----------------------
        .config("spark.hadoop.fs.s3a.committer.name",      "directory")
        .config("spark.sql.parquet.compression.codec",     "snappy")
        .config("spark.sql.shuffle.partitions",            "8")  # tuned for 16 GB host
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
