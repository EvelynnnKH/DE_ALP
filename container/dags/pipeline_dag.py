from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# 1. Definisi argumen default untuk manajemen kegagalan (Sesuai standard Week-13)
default_args = {
    "owner": "bernard-pipeline-lead",
    "retries": 2,                               # Mengulang otomatis 2 kali jika error
    "retry_delay": timedelta(minutes=5),        # Jeda waktu antar retry
}

# 2. Definisi path di dalam container Docker (Sesuai volume mounting docker-compose)
DBT_PROJECT_DIR = "/opt/airflow/dbt"
INGESTION_DIR   = "/opt/airflow/ingestion"

# 3. Inisialisasi Master DAG
with DAG(
    dag_id="master_financial_pipeline",
    description="Pipeline End-to-End: Ingesti Data Saham harian dan Transformasi dbt",
    start_date=datetime(2026, 4, 1),           # Menyesuaikan rentang waktu dataset harian
    end_date=datetime(2026, 4, 30),
    schedule="@daily",                         # Eksekusi batch harian (Batch Processing)
    catchup=False,                             # Mencegah penumpukan eksekusi otomatis saat nyala
    max_active_runs=1,                         # Eksekusi berurutan agar BigQuery aman dari conflict
    default_args=default_args,
    tags=["final_project", "bigquery", "dbt", "incremental"],
) as dag:

    # TASK 1: Eksekusi skrip Python untuk Ingesti ke Bronze Layer (Akan diisi oleh Spesialis Ingesti)
    upload_transactions = BashOperator(
        task_id="upload_transactions",
        bash_command=f"python {INGESTION_DIR}/load_raw.py --date {{{{ ds }}}}"
    )

    # TASK 2: Refresh Staging Layer dbt (Mengubah data mentah menjadi View bersih di Silver Layer)
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --select staging"
    )

    # TASK 3: Membangun Ulang Tabel Dimensi (Drop and Recreate Table harian)
    dbt_run_dims = BashOperator(
        task_id="dbt_run_dims",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --select dim_customer dim_product dim_date"
    )

    # TASK 4: Transformasi Inkremental Gold Layer (Hanya memproses data tanggal terkait menggunakan Merge)
    dbt_run_incremental = BashOperator(
        task_id="dbt_run_incremental",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --select fact_sales fct_daily_summary --vars '{{\"execution_date\": \"{{{{ ds }}}}\" }}'"
    )

    # TASK 5: Data Quality Testing (Gerbang validasi Unique, Not Null, dan Relasi)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR}"
    )

    # 4. Menyusun urutan eksekusi pipeline (Dependency Chain)
    upload_transactions >> dbt_run_staging >> dbt_run_dims >> dbt_run_incremental >> dbt_test