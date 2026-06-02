import argparse
import pandas as pd
import os
from google.cloud import bigquery
from google.oauth2 import service_account

# MAIN INGESTION FUNCTION (DAILY INCREMENTAL LOAD)
def load_raw_data(execution_date):
    """
    Fungsi operasional harian untuk menarik HANYA data baru 
    sesuai tanggal eksekusi Airflow, lalu menyisipkannya (APPEND) ke BigQuery.
    """
    
    current_dir = os.path.dirname(os.path.abspath(__file__)) 
    project_root = os.path.abspath(os.path.join(current_dir, '..')) 
    
    csv_path = os.path.join(current_dir, 'stock_prices.csv')
    key_path = os.path.join(project_root, 'credentials', 'service-account.json')

    PROJECT_ID = 'de-final-project-2026' 
    DATASET_ID = 'raw'
    TABLE_ID = 'raw_stock_prices'
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    if not os.path.exists(key_path):
        print(f"Error: Kredensial tidak ditemukan.")
        return

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    # A. Ekstraksi & Filter Data Harian (Logika Evelyn)
    print(f"Mencari data khusus tanggal {execution_date} dari {csv_path}...")
    
    chunks_list = []
    for chunk in pd.read_csv(csv_path, chunksize=10000):
        # FILTER: Hanya ambil baris yang tanggalnya cocok dengan Airflow hari ini
        filtered_chunk = chunk[chunk['Date'].astype(str).str.startswith(execution_date)].copy()
        
        if not filtered_chunk.empty:
            filtered_chunk['Date'] = pd.to_datetime(filtered_chunk['Date'], utc=True)
            chunks_list.append(filtered_chunk)

    if not chunks_list:
        print(f"Aman: Tidak ada data saham baru untuk tanggal {execution_date} (Mungkin hari libur bursa).")
        return

    filtered_df = pd.concat(chunks_list, ignore_index=True)

    # B. Idempotency Check (Pembersihan Hari H)
    # Jika kita melakukan "Rerun" (Clear Task) di Airflow untuk hari ini,
    # hapus dulu data hari ini yang sudah terlanjur masuk agar tidak double.
    try:
        delete_query = f"DELETE FROM `{full_table_id}` WHERE DATE(date) = '{execution_date}'"
        client.query(delete_query).result()
    except Exception as e:
        pass # Abaikan jika gagal (misal tabel belum ada)

    # C. Proses Ingesti (APPEND)
    # Gunakan WRITE_APPEND untuk menyambung data baru di bawah data historis
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND", 
        source_format=bigquery.SourceFormat.PARQUET if hasattr(pd, 'to_parquet') else bigquery.SourceFormat.CSV,
        autodetect=True,
    )

    print(f"Menyisipkan {len(filtered_df)} baris data baru ke BigQuery...")
    
    try:
        job = client.load_table_from_dataframe(filtered_df, full_table_id, job_config=job_config)
        job.result() 
        print(f"Berhasil! Data tanggal {execution_date} telah ditambahkan.")
    except Exception as e:
        print(f"Gagal mengunggah data: {e}")

# SCRIPT EXECUTION ENTRY POINT
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', required=True, help='Tanggal eksekusi dari Airflow')
    args = parser.parse_args()

    load_raw_data(args.date)