#!/usr/bin/env bash
# Run unit tests for Spark transformations in an isolated container.
# Usage: ./spark/run_tests.sh
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker run --rm \
  -v "${SCRIPT_DIR}":/spark \
  --user root \
  bitnamilegacy/spark:3.5.3 \
  bash -c '
    pip install --quiet pytest
    cd /spark
    PYTHONPATH="/spark/jobs:/opt/bitnami/spark/python:$(ls /opt/bitnami/spark/python/lib/py4j-*-src.zip | head -1)" \
      python -m pytest tests/ -v
  '
