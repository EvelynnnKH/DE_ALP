import argparse
import pandas as pd
import os
from google.cloud import bigquery
from google.oauth2 import service_account

def load_raw_data(execution_date):
    # 1. Konfigurasi Path (Menyesuaikan dengan folder DE_ALP)
    current_dir = os.path.dirname(os.path.abspath(__file__)) # /opt/airflow/ingestion
    project_root = os.path.abspath(os.path.join(current_dir, '..')) # /opt/airflow
    
    # Path ke file CSV dan Credentials
    # Pastikan file stock_prices.csv ada di folder utama DE_ALP
    csv_path = os.path.join(current_dir, 'stock_prices.csv')
    key_path = os.path.join(project_root, 'credentials', 'service-account.json')

    # 2. Konfigurasi BigQuery (Ubah PROJECT_ID sesuai milikmu)
    PROJECT_ID = 'de-final-project-2026' 
    DATASET_ID = 'raw'
    TABLE_ID = 'raw_stock_prices'
    # Jika ingin idempotent per tanggal, gunakan sharded table (misal: raw_stock_prices_20260529)
    # atau biarkan satu tabel jika menggunakan WRITE_TRUNCATE untuk landing zone.
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # 3. Inisialisasi BigQuery Client
    if not os.path.exists(key_path):
        print(f"Error: File service-account.json tidak ditemukan di {key_path}")
        return

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    # 4. Membaca dan Memfilter Data
    print(f"Membaca data dari {csv_path}...")
    chunks_list = []
    
    # Filter data berdasarkan execution_date (format YYYY-MM-DD)
    # Kolom Date di CSV berformat '2020-01-03 00:00:00-05:00'
    for chunk in pd.read_csv(csv_path, chunksize=10000):
        chunk['Date'] = pd.to_datetime(chunk['Date'], utc=True)
        filtered_chunk = chunk[chunk['Date'].astype(str).str.startswith(execution_date)].copy()
        if not filtered_chunk.empty:
            filtered_chunk['Date'] = pd.to_datetime(filtered_chunk['Date'], utc=True)
            chunks_list.append(filtered_chunk)

    if not chunks_list:
        print(f"Peringatan: Tidak ada data untuk tanggal {execution_date}")
        return

    filtered_df = pd.concat(chunks_list, ignore_index=True)

    if filtered_df.empty:
        print(f"Peringatan: Tidak ada data untuk tanggal {execution_date}")
        return

    # 5. Ingesti ke BigQuery secara Idempotent
    # WRITE_TRUNCATE akan menghapus isi tabel lama dan menggantinya dengan yang baru
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.PARQUET if hasattr(pd, 'to_parquet') else bigquery.SourceFormat.CSV,
        autodetect=True,
    )

    print(f"Memuat {len(filtered_df)} baris data untuk tanggal {execution_date} ke {full_table_id}...")
    
    try:
        job = client.load_table_from_dataframe(filtered_df, full_table_id, job_config=job_config)
        job.result()  # Menunggu job selesai
        print(f"Berhasil! Data tanggal {execution_date} telah diunggah.")
    except Exception as e:
        print(f"Gagal mengunggah data: {e}")

if __name__ == "__main__":
    # Setup Argument Parser untuk Airflow
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', required=True, help='Tanggal eksekusi dengan format YYYY-MM-DD')
    args = parser.parse_args()

    load_raw_data(args.date)