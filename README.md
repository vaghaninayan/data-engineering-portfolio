# NYC Taxi Batch Data Pipeline

Reproducible batch-processing data architecture built for the IU Data Engineering portfolio
(course DLMDSEDE02). Ingests NYC TLC Taxi & Limousine trip records, transforms them through a
medallion lakehouse (Bronze → Silver → Gold), and serves aggregated features to a downstream
machine-learning consumer.

## Architecture

See `docs/Architecture_Diagram_P1_S.png` for the full architecture diagram.

| Layer        | Technology                  | Purpose                                          |
|--------------|-----------------------------|--------------------------------------------------|
| Ingestion    | Python 3.12 + boto3         | Pulls monthly TLC Parquet, lands in MinIO Bronze |
| Storage      | MinIO (S3-compatible)       | Bronze (raw) and Silver (cleaned) layers         |
| Processing   | Apache Spark 3.5 (PySpark)  | Bronze → Silver and Silver → Gold transforms    |
| Quality      | Great Expectations          | Schema, null, range, and aggregate checks        |
| Serving      | PostgreSQL 16               | Gold layer aggregated tables                     |
| Orchestration| Apache Airflow 2.10         | Monthly ingest + quarterly aggregate DAGs        |

## Quick start

Prerequisites: Docker Desktop with WSL2 backend, 16 GB RAM recommended.

```bash
git clone https://github.com/vaghaninayan/data-engineering-portfolio.git
cd data-engineering-portfolio
cp .env.example .env          # fill in real secrets
docker compose up -d
```

Once running:

- Airflow UI: http://localhost:8080
- MinIO console: http://localhost:9001
- Postgres: `localhost:5432` (psql or any SQL client)

## Repository layout

## Author

Nayanbhai Vaghani — IU International University of Applied Sciences
