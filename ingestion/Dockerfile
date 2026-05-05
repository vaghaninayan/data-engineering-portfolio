# Lightweight base image; matches the Python version the script tests against.
FROM python:3.12-slim

# Run as non-root user for security
RUN groupadd --gid 1000 ingest && \
    useradd --uid 1000 --gid 1000 --create-home --shell /bin/bash ingest

WORKDIR /app

# Layer caching: deps before code
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest.py .

USER ingest
ENTRYPOINT ["python", "ingest.py"]
