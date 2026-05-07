"""
One-shot loader: TLC Taxi Zone Lookup CSV -> gold.dim_location.

The fact_trips_hourly table foreign-keys pu_location_id to dim_location.
This script must run before any silver_to_gold load.

Idempotent — re-running upserts existing rows.
"""
from __future__ import annotations

import csv
import logging
import os
import sys

import psycopg2
import requests
from psycopg2.extras import execute_batch

ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("dim_location")


UPSERT_SQL = """
    INSERT INTO gold.dim_location (location_id, borough, zone, service_zone)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (location_id) DO UPDATE SET
        borough      = EXCLUDED.borough,
        zone         = EXCLUDED.zone,
        service_zone = EXCLUDED.service_zone
"""


def fetch_zones() -> list[tuple]:
    log.info("Downloading %s", ZONE_LOOKUP_URL)
    resp = requests.get(ZONE_LOOKUP_URL, timeout=30)
    resp.raise_for_status()

    rows = []
    for r in csv.DictReader(resp.text.splitlines()):
        rows.append((
            int(r["LocationID"]),
            (r.get("Borough") or "Unknown")[:50],
            (r.get("Zone")    or "Unknown")[:100],
            (r.get("service_zone") or None) and r["service_zone"][:50] or None,
        ))
    log.info("Parsed %d zones", len(rows))
    return rows


def upsert(rows: list[tuple]) -> int:
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        with conn:
            with conn.cursor() as cur:
                execute_batch(cur, UPSERT_SQL, rows, page_size=100)
                cur.execute("SELECT COUNT(*) FROM gold.dim_location")
                count = cur.fetchone()[0]
        log.info("dim_location now has %d rows", count)
        return count
    finally:
        conn.close()


def main() -> int:
    try:
        rows = fetch_zones()
        upsert(rows)
        return 0
    except Exception as e:
        log.error("Failed: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
