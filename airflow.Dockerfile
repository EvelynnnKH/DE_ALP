FROM apache/airflow:3.0.1

USER root

# Install git jika dbt membutuhkan dependensi dari modul eksternal
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Swap dbt-duckdb ke dbt-bigquery sesuai standard migrasi cloud
RUN pip install --no-cache-dir \
    dbt-core==1.11.7 \
    dbt-bigquery==1.9.0 \
    pandas \
    google-cloud-bigquery \
    pyarrow