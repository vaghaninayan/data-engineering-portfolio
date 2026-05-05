"""
NYC TLC Trip Record ingester — Bronze layer downloader.

Downloads monthly Parquet files for yellow and green taxi trips from the
public NYC TLC dataset and uploads them to MinIO under the `bronze/` bucket.

Usage:
    python ingest.py --start 2024-01 --end 2024-06
    python ingest.py --start 2024-01 --end 2024-01 --types yellow

Environment variables (all required):
    MINIO_ENDPOINT          e.g. http://minio:9000
    MINIO_ACCESS_KEY        MinIO root user
    MINIO_SECRET_KEY        MinIO root password
    BRONZE_BUCKET           default: bronze

Exit codes:
    0 — all months downloaded successfully (or already present)
    1 — one or more downloads failed permanently
    2 — configuration / argument error
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date
from typing import Iterator

import boto3
import requests
from botocore.client import Config
from botocore.exceptions import ClientError
from dateutil.relativedelta import relativedelta
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TRIP_TYPES = ("yellow", "green")
HTTP_TIMEOUT = (10, 300)  # (connect, read) seconds — TLC files can be large
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MiB streaming chunks

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ingest")


# ----------------------------------------------------------------------------
# Domain types
# ----------------------------------------------------------------------------
@dataclass(frozen=True)
class IngestSpec:
    """One unit of work: download {trip_type}_tripdata_{year}-{month}.parquet"""
    trip_type: str  # "yellow" or "green"
    year: int
    month: int

    @property
    def filename(self) -> str:
        return f"{self.trip_type}_tripdata_{self.year:04d}-{self.month:02d}.parquet"

    @property
    def source_url(self) -> str:
        return f"{TLC_BASE_URL}/{self.filename}"

    @property
    def bronze_key(self) -> str:
        # Date-partitioned layout: bronze/yellow/year=2024/month=01/yellow_tripdata_2024-01.parquet
        return (
            f"{self.trip_type}/"
            f"year={self.year:04d}/"
            f"month={self.month:02d}/"
            f"{self.filename}"
        )


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def parse_year_month(value: str) -> tuple[int, int]:
    """Parse YYYY-MM into (year, month). Raises ValueError on bad input."""
    parts = value.split("-")
    if len(parts) != 2:
        raise ValueError(f"Expected YYYY-MM format, got: {value!r}")
    year, month = int(parts[0]), int(parts[1])
    if not (1 <= month <= 12):
        raise ValueError(f"Month must be 1–12, got: {month}")
    if not (2009 <= year <= date.today().year):
        raise ValueError(f"Year out of plausible range: {year}")
    return year, month


def iter_specs(
    start: tuple[int, int],
    end: tuple[int, int],
    trip_types: tuple[str, ...],
) -> Iterator[IngestSpec]:
    """Yield one IngestSpec per (trip_type, month) in the inclusive range."""
    cur = date(start[0], start[1], 1)
    end_d = date(end[0], end[1], 1)
    while cur <= end_d:
        for trip_type in trip_types:
            yield IngestSpec(trip_type=trip_type, year=cur.year, month=cur.month)
        cur += relativedelta(months=1)


def make_s3_client():
    """Create an S3 client pointed at MinIO. Path-style addressing required."""
    endpoint = os.environ["MINIO_ENDPOINT"]
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret_key = os.environ["MINIO_SECRET_KEY"]
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",  # MinIO ignores this but boto3 requires it
    )


def object_exists(s3, bucket: str, key: str) -> bool:
    """Idempotency check: skip downloads whose target already exists in Bronze."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


# ----------------------------------------------------------------------------
# Core download logic — retried on network failures
# ----------------------------------------------------------------------------
@retry(
    retry=retry_if_exception_type((requests.RequestException, ClientError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=30),
    reraise=True,
)
def download_and_upload(spec: IngestSpec, s3, bucket: str) -> int:
    """Stream the file from TLC and upload to MinIO. Returns bytes uploaded."""
    log.info("Downloading %s -> s3://%s/%s", spec.source_url, bucket, spec.bronze_key)
    with requests.get(spec.source_url, stream=True, timeout=HTTP_TIMEOUT) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        # Stream directly to MinIO via multipart upload — never buffer the whole
        # file in memory. Critical for the larger green/yellow monthly files.
        s3.upload_fileobj(
            Fileobj=resp.raw,
            Bucket=bucket,
            Key=spec.bronze_key,
            ExtraArgs={"ContentType": "application/octet-stream"},
        )
    log.info("  uploaded %s (%.1f MB)", spec.bronze_key, total / 1024 / 1024)
    return total


# ----------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--start", required=True, help="First month (YYYY-MM)")
    parser.add_argument("--end",   required=True, help="Last month inclusive (YYYY-MM)")
    parser.add_argument(
        "--types",
        default="yellow,green",
        help="Comma-separated trip types (default: yellow,green)",
    )
    args = parser.parse_args()

    try:
        start = parse_year_month(args.start)
        end   = parse_year_month(args.end)
    except ValueError as e:
        log.error("Argument error: %s", e)
        return 2

    if start > end:
        log.error("--start (%s) must be <= --end (%s)", args.start, args.end)
        return 2

    trip_types = tuple(t.strip().lower() for t in args.types.split(","))
    for tt in trip_types:
        if tt not in TRIP_TYPES:
            log.error("Unknown trip type: %s (allowed: %s)", tt, TRIP_TYPES)
            return 2

    bucket = os.environ.get("BRONZE_BUCKET", "bronze")
    try:
        s3 = make_s3_client()
    except KeyError as e:
        log.error("Missing required environment variable: %s", e)
        return 2

    specs = list(iter_specs(start, end, trip_types))
    log.info("Plan: %d files (%s..%s, types=%s)", len(specs), args.start, args.end, trip_types)

    failures = 0
    skipped = 0
    bytes_total = 0
    for spec in specs:
        try:
            if object_exists(s3, bucket, spec.bronze_key):
                log.info("SKIP (already present): %s", spec.bronze_key)
                skipped += 1
                continue
            bytes_total += download_and_upload(spec, s3, bucket)
        except Exception as e:
            log.error("FAILED %s: %s", spec.bronze_key, e)
            failures += 1

    log.info(
        "Done. files=%d uploaded=%d skipped=%d failed=%d bytes=%.1fMB",
        len(specs), len(specs) - skipped - failures, skipped, failures,
        bytes_total / 1024 / 1024,
    )
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
