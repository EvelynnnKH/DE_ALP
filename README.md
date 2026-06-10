# **Final Project: End-to-End Financial Data Engineering Pipeline**

**Course:** Data Engineering  
**Level:** Undergraduate (3rd Year)  
**Institution:** Universitas Ciputra Surabaya

---

# **1. Project Overview**

Proyek ini merupakan implementasi End-to-End Batch Data Pipeline yang memproses data pasar saham historis menggunakan ekosistem Modern Data Stack.

Infrastruktur dirancang dalam lingkungan terisolasi menggunakan kontainer (Docker), yang mengotomatisasi ekstraksi data mentah, transformasi relasional berbasis Star Schema, hingga penyajian visualisasi analitik interaktif.

---

# **2. System Architecture**

```mermaid
graph TD

%% Komponen Infrastruktur
CSV[Dataset CSV Lokal<br/>stock_prices.csv]
Airflow[[Apache Airflow 3.0.1<br/>Orchestrator]]
Python[Python Pandas<br/>load_raw.py]
BQ_Raw[(Google BigQuery<br/>Bronze Layer: raw_stock_prices)]
DBT_Staging[dbt Staging<br/>Silver Layer: Views]
DBT_Marts[(Google BigQuery<br/>Gold Layer: Star Schema)]
Metabase[Metabase<br/>Business Intelligence]

%% Alur Data Utama
CSV -->|Ekstraksi Chunking 10k baris| Python
Python -->|Load Data mentah| BQ_Raw
BQ_Raw -->|Pembersihan & Deduplikasi| DBT_Staging
DBT_Staging -->|Transformasi MD5 & Relasi| DBT_Marts
DBT_Marts -->|Kueri Analitik Cepat| Metabase

%% Alur Orkestrasi (Garis putus-putus)
Airflow -.->|Task 1: upload_stock_prices| Python
Airflow -.->|Task 2: dbt_run_staging| DBT_Staging
Airflow -.->|Task 3 & 4: dbt_run_dims & incremental| DBT_Marts

class BQ_Raw,DBT_Marts db;
class Airflow airflow;
class DBT_Staging dbt;
```

Sistem ini mengadopsi arsitektur Medallion (Bronze, Silver, Gold).

## **a. Ingestion Layer (Bronze Layer)**

### Teknologi

- Python (Pandas)
- Google Cloud BigQuery

### Proses

Skrip `load_raw.py` membaca file CSV lokal dalam potongan 10.000 baris (*chunking*) untuk efisiensi RAM.

Skrip ini dikonfigurasi untuk menjalankan Daily Incremental Load berdasarkan Execution Date dari Airflow.

### Keamanan

Menerapkan logika Idempotency (operasi penghapusan data pada hari-H sebelum penyisipan data baru) untuk mencegah duplikasi apabila terjadi kegagalan sistem.

Data dimuat ke dataset raw di BigQuery.

---

## **b. Transformation Layer (Silver & Gold Layer)**

### Teknologi

- dbt (Data Build Tool)

### Staging (Silver)

Melakukan standardisasi tipe data, pembersihan nama kolom, pengubahan teks menjadi huruf kapital, dan deduplikasi menggunakan Window Function di model `stg_stock_prices`.

### Marts (Gold - Star Schema)

Membangun empat tabel dimensi:

- `dim_stock`
- `dim_sector`
- `dim_industry`
- `dim_date`

menggunakan Surrogate Keys deterministik berbasis fungsi hash `MD5()`.

Tabel fakta (`fact_stock_prices`) memuat kalkulasi metrik seperti perubahan harga harian (*price change percentage*).

### Data Quality

Diimplementasikan pengujian:

- `not_null`
- `unique`

pada Primary Keys melalui konfigurasi YAML.

---

## **c. Orchestration Layer**

### Teknologi

- Apache Airflow 3.0.1

### Proses

Penjadwal terpusat (`master_financial_pipeline`) yang mengatur urutan eksekusi secara berantai:

```text
Ekstraksi Python
      ↓
dbt Staging
      ↓
dbt Dimensions
      ↓
dbt Fact
      ↓
dbt Test
```

---

## **d. Serving & Analytics Layer**

### Teknologi

- Metabase

### Proses

Terhubung langsung ke BigQuery (Marts Layer) untuk menyajikan dashboard analitik tanpa membebani basis data transaksional.

---

# **3. Dataset Description**

Dataset yang digunakan adalah US Stock Market Historical OHLCV.

Dataset ini berisi riwayat pergerakan harga saham dari berbagai perusahaan di bursa efek Amerika Serikat.

Kolom utama meliputi:

- Date
- Open
- High
- Low
- Close
- Adj Close
- Volume

Data diperkaya dengan entitas:

- Sector
- Industry

---

# **4. Local Installation & Execution Guide**

Karena batas ukuran file dan kebijakan keamanan, file kredensial (JSON) dan file data mentah (CSV) tidak disertakan di dalam repositori ini.

Ikuti instruksi berikut secara berurutan untuk menjalankan proyek dari awal.

---

## **Tahap 1: Prasyarat Sistem & Clone Repository**

Pastikan Anda telah menginstal:

- Docker Desktop (dalam status Running)
- Git
- VS Code

### 1. Kloning Repository

Buka terminal dan jalankan:

```bash
git clone https://github.com/EvelynnnKH/DE_ALP.git

cd DE_ALP
```

### 2. Buat File Konfigurasi Environment

Salin file `.env.example` menjadi:

```text
.env
```

---

## **Tahap 2: Konfigurasi Google Cloud Platform (GCP)**

Anda wajib menyediakan Service Account GCP untuk memberikan akses tulis/baca ke BigQuery.

### 1. Membuat Project GCP

- Buka Google Cloud Console.
- Buat Project baru.
- Catat Project ID Anda.

### 2. Mengaktifkan BigQuery API

Buka:

```text
APIs & Services
    └── Library
```

Cari:

```text
BigQuery API
```

Kemudian klik:

```text
Enable
```

### 3. Membuat Service Account

Buka:

```text
IAM & Admin
    └── Service Accounts
```

Klik:

```text
Create Service Account
```

Berikan nama khusus, misalnya:

```text
airflow-bq
```

### 4. Memberikan Hak Akses

Pada bagian:

```text
Grant this service account access to project
```

berikan role:

```text
BigQuery Admin
```

### 5. Membuat Credential JSON

- Buka Service Account yang baru dibuat.
- Masuk ke tab **Keys**.
- Klik **Add Key**.
- Pilih **Create New Key**.
- Pilih format **JSON**.

### 6. Simpan Credential

Ubah nama file yang terunduh menjadi:

```text
service-account.json
```

### 7. Letakkan Credential pada Folder Project

Buat folder:

```text
credentials/
```

Kemudian letakkan file:

```text
service-account.json
```

di dalam folder tersebut.

Struktur direktori:

```text
DE_ALP/
│
├── credentials/
│   └── service-account.json
│
├── ingestion/
├── dbt/
├── dags/
└── docker-compose.yml
```

---

## **Penting - Perubahan Kode**

Anda wajib menyesuaikan Project ID pada kode sumber agar mengarah ke proyek GCP Anda.

Ganti string berikut:

```text
de-final-project-2026
```

dengan Project ID GCP milik Anda pada file:

- `ingestion/load_raw.py` (variabel PROJECT_ID)
- `dbt/sources.yml` (variabel project)
- `dbt/profiles.yml` (variabel project)

---

## **Tahap 3: Mengunduh Dataset**

1. Kunjungi dataset Kaggle:

   **[US Stock Market Historical OHLCV Dataset](https://www.kaggle.com/datasets/asadullahcreative/us-stock-market-historical-ohlcv-dataset)**

2. Unduh (*download*) dan ekstrak dataset tersebut.

3. Ubah nama file utama menjadi:

```text
stock_prices.csv
```

4. Pindahkan file tersebut ke dalam folder:

```text
ingestion/
```

Struktur folder yang diharapkan:

```text
DE_ALP/
│
├── ingestion/
│   └── stock_prices.csv
│
├── credentials/
├── dbt/
└── dags/
```

---

## **Tahap 4: Menyalakan Infrastruktur**

### 1. Menjalankan Docker Compose

Buka terminal pada direktori proyek dan jalankan:

```bash
docker compose up -d
```

### 2. Menunggu Seluruh Service Aktif

Tunggu sekitar 1–2 menit hingga seluruh kontainer berikut berstatus healthy:

- Airflow
- PostgreSQL
- Metabase

---

## **Tahap 5: Eksekusi Pipeline (Airflow)**

### 1. Akses Airflow

Buka browser dan masuk ke:

```text
http://localhost:8081
```

### 2. Login

Gunakan kredensial berikut:

```text
Username: airflow
Password: airflow
```

### 3. Aktifkan DAG

Temukan DAG:

```text
master_financial_pipeline
```

Ubah status dari:

```text
Paused → Unpaused
```

### 4. Trigger DAG

Klik tombol:

```text
Play (Trigger DAG)
```

pada bagian kanan atas.

### 5. Monitoring Eksekusi

Pantau proses pada tab:

```text
Grid
```

Seluruh task harus berstatus:

```text
Success (Hijau)
```

---

## **Tahap 6: Pembuatan Dokumentasi Data (dbt Docs)**

### 1. Buka Terminal Baru

Buka terminal baru pada direktori proyek Anda.

### 2. Generate Dokumentasi dbt

Masuk ke dalam kontainer Airflow dan jalankan:

```bash
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt docs generate"
```

### 3. Akses Hasil Dokumentasi

Dokumen katalog akan dibuat di dalam folder:

```text
dbt/target/
```

Anda dapat membukanya secara lokal dengan:

- Menjalankan Python web server
- Membuka file `index.html` secara langsung

---

## **Tahap 7: Konfigurasi Metabase (Visualisasi)**

### 1. Akses Metabase

Buka browser dan masuk ke:

```text
http://localhost:3001
```

### 2. Registrasi Akun

Selesaikan proses registrasi akun lokal Metabase.

### 3. Hubungkan ke BigQuery

Pada halaman:

```text
Add your data
```

pilih:

```text
Google BigQuery
```

### 4. Upload Credential

Unggah file:

```text
service-account.json
```

dari folder:

```text
credentials/
```

untuk melakukan autentikasi.

### 5. Sinkronisasi Data

Metabase akan memindai Data Warehouse Anda secara otomatis.

---

## **Panduan Pembuatan Chart**

### **Tren Harga Saham**

Buat Question baru dengan konfigurasi berikut:

```text
Table          : fact_stock_prices
Summarize      : Average of Close Price
Group By       : Trade Date
Visualization  : Line Chart
```

### **Volume Perdagangan per Sektor**

Buat Question baru dengan konfigurasi berikut:

```text
Table          : fact_stock_prices
Summarize      : Sum of Volume
Group By       : Sector
Visualization  : Bar Chart
```

### **Top Kenaikan Harga**

Buat Question baru dengan konfigurasi berikut:

```text
Table          : fact_stock_prices
Field          : price_change_pct
Sort           : Descending
Limit          : 10
Visualization  : Table
```

### **Dashboard Utama**

Simpan ketiga chart tersebut ke dalam satu Dashboard utama.

---

# **5. Expected Output**

Sistem dinyatakan berhasil dieksekusi apabila memenuhi kriteria berikut.

## **Airflow**

Grafik eksekusi DAG menampilkan status:

```text
Success
```

pada seluruh tugas secara berurutan.

## **BigQuery**

Tabel berikut berhasil terbentuk tanpa redundansi data:

```text
raw_stock_prices
dim_stock
dim_sector
dim_industry
dim_date
fact_stock_prices
```

## **Metabase**

Dashboard berhasil melakukan kalkulasi analitik dan menampilkan visualisasi grafik interaktif.

---

# **6. Findings & Conclusion**

Implementasi End-to-End Pipeline ini membuktikan efektivitas pemodelan Star Schema dalam mempercepat kueri analitik pada basis data berorientasi kolom seperti BigQuery.

Penerapan Surrogate Keys menggunakan fungsi `MD5()` memastikan integritas relasi tabel fakta dan tabel dimensi tetap terjamin secara deterministik.

Berdasarkan visualisasi data yang dihasilkan, sektor Technology secara konsisten mendominasi pergerakan volume perdagangan harian dibandingkan sektor lainnya.

---

# **7. Known Limitations**

## **1. BigQuery DML Restrictions**

Pada konfigurasi versi Free Tier (Sandbox) GCP, operasi:

```sql
MERGE
```

(DML) dilarang.

Sebagai penyesuaian, model fakta dalam proyek ini diatur menggunakan materialisasi:

```text
table (Full Refresh)
```

meskipun logika kueri incremental telah dirancang dan disertakan di dalam struktur kodenya.

---

## **2. Ketergantungan File Lokal**

Arsitektur ingestion saat ini mengandalkan ketersediaan file CSV secara statis.

Skalabilitas masa depan dapat dicapai dengan mengintegrasikan konektor API real-time seperti:

- Yahoo Finance
- Alpha Vantage