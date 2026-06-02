# Setup Infrastruktur Final Project Data Engineering

Dokumen ini berisi panduan wajib untuk semua anggota tim agar bisa menyalakan *environment* lokal (Airflow 3, Metabase, dbt, dan PostgreSQL) di laptop masing-masing tanpa *error*.

## Prasyarat
Pastikan aplikasi berikut sudah ter-*install* dan berjalan di laptop kalian:
- **Docker Desktop** (Pastikan mesin Docker sudah dalam status *Running*)
- **Git**
- **VS Code**

## Langkah Setup (Wajib Diikuti Secara Berurutan)

### 1. Ambil Versi Terbaru dari GitHub
Buka terminal kalian, arahkan ke folder proyek, lalu pastikan kalian berada di *branch* utama dan tarik pembaruan terakhir:
`git pull origin main`

### 2. Konfigurasi Kredensial GCP (Sangat Penting!)
File kunci rahasia `service-account.json` ke Arsitek Pipeline, lalu letakkan file tersebut tepat di dalam folder `credentials/`.
*(Catatan: File ini sengaja diblokir oleh GitHub demi keamanan proyek kita, jadi harus ditaruh secara manual).*

### 3. Nyalakan Container Docker
Buka terminal di dalam folder proyek (tempat file `docker-compose.yml` berada), lalu jalankan perintah ini untuk membangun dan menyalakan semua sistem:
`docker compose up --build -d`
*Tunggu sekitar 15-20 detik hingga sistem selesai mengunduh gambar dan PostgreSQL berstatus 'healthy'.*

### 4. Inisialisasi Database (Hanya Dilakukan 1x Saat Setup Awal)
Kita perlu membuat wadah internal agar Metabase dan Airflow tidak mengalami *crash loop*:

**a. Buat database untuk Metabase:**
`docker exec -it de_alp-postgres-1 psql -U airflow -c "CREATE DATABASE metabase;"`

**b. Bangun struktur tabel untuk Airflow 3:**
`docker compose run --rm airflow-scheduler airflow db migrate`

**c. Segarkan semua layanan agar membaca database baru:**
`docker compose restart`

### 5. Akses Dashboard
Setelah sistem di-*restart*, tunggu sekitar 20-30 detik untuk proses *booting* Java dan Python, lalu buka di *browser* kalian:
- **Apache Airflow (Orkestrasi):** `http://localhost:8080` (Gunakan otentikasi Admin yang sudah diatur).
- **Metabase (Visualisasi):** `http://localhost:3001` (Jika muncul tulisan error, tunggu sebentar dan *refresh* karena Metabase butuh waktu untuk membangun tabel awalnya).

### 6 Persiapan Environment Variables & Credentials (WAJIB)
Sebelum memicu pipeline, pastikan konfigurasi keamanan dan koneksi Docker kalian sudah sinkron dengan langkah-langkah berikut:

#### **a. Membuat File `.env`**
Buat file bernama `.env` di folder utama proyek kalian (`DE_ALP/`), lalu salin kode di bawah ini. Kita **wajib menggunakan Fernet Key yang sama** agar enkripsi data *connection* di database Airflow kita tidak mengalami konflik (*decryption error*):

```env
AIRFLOW_FERNET_KEY=<40 characters fernet key mu>
```
#### **b. Membuat Fernet Key Secara Mandiri (Opsional)**

Jika di masa depan tim perlu membuat *Fernet Key* baru secara mandiri, gunakan package `cryptography` bawaan Python dengan menjalankan perintah berikut di terminal:

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Perintah tersebut akan menghasilkan string acak sepanjang 44 karakter yang dapat digunakan sebagai nilai `AIRFLOW_FERNET_KEY`.

> ⚠️ Penting: Pastikan file `.env` sudah dimasukkan ke dalam `.gitignore` agar *secret key* tidak ikut ter-*push* ke repositori publik GitHub.

---

#### **c. Menyiapkan Kredensial Google Cloud Platform (GCP)**

Setiap anggota tim yang ingin menjalankan pipeline wajib meminta file kredensial GCP (`service-account.json`) kepada spesialis ingestion atau pemegang akses cloud project.

Langkah penempatan file:

1. Buat folder baru bernama `credentials` di root proyek (`DE_ALP/`), sejajar dengan folder seperti `ingestion/`, `dags/`, atau `dbt/`.

2. Letakkan file service account pada path berikut:

```bash
DE_ALP/credentials/service-account.json
```

3. Path tersebut sudah otomatis di-*mount* oleh Docker ke dalam container Airflow pada lokasi:

```bash
/opt/airflow/credentials/service-account.json
```

Konfigurasi ini memungkinkan pipeline ingestion, BigQuery Hook, maupun proses dbt untuk melakukan autentikasi ke Google Cloud secara otomatis tanpa konfigurasi tambahan di dalam container.

---

### 7. Pengujian Data Ingestion & Pemicuan Pipeline (Handover Guide)

Seluruh *package* Python sudah terisolasi dan terinstall otomatis di dalam container Docker. Bagian **Data Ingestion (Bronze Layer)** pada skrip `load_raw.py` telah selesai 100% dan dikonfigurasi secara *idempotent* menggunakan metode `WRITE_TRUNCATE` ke Google BigQuery.

Tabel `raw_stock_prices` bertindak sebagai **Landing Zone** harian. Untuk menguji fungsionalitas integrasi parameter tanggal Airflow (`--date {{ ds }}`), pemicuan pipeline dapat dilakukan melalui dua alternatif berikut:

#### **Alternatif A: Memicu (Trigger) Langsung via Web UI Airflow (Paling Mudah)**

1. Buka dashboard Airflow di browser (`http://localhost:8080`).

2. Cari DAG bernama `master_financial_pipeline` di daftar utama, lalu klik nama DAG tersebut.

3. Di pojok kanan atas halaman DAG, klik tombol **Play (Trigger DAG)** biru, lalu pilih **Trigger DAG w/ config**.

4. Pada bagian parameter tanggal eksekusi (*Logical Date*), set tanggal ke:

```text
2020-01-15
```

(atau tanggal historis lain yang tersedia di dataset CSV).

5. Klik **Trigger**, lalu pantau progres pipeline pada tampilan *Grid* atau *Graph*.

---

#### **Alternatif B: Memicu (Trigger) via CLI Terminal**

Jika lebih nyaman menggunakan terminal host, pipeline juga dapat dipicu secara manual menggunakan perintah berikut:

```bash
docker exec -it de_alp-airflow-scheduler-1 airflow dags trigger master_financial_pipeline --logical-date 2020-01-15
```

Perintah tersebut akan menjalankan simulasi ingestion batch harian menggunakan tanggal override yang diberikan secara manual.

---

### 8. Verifikasi Hasil Pipeline

Jika pipeline berhasil dijalankan tanpa error:

* Data hasil ingestion akan masuk ke tabel BigQuery pada layer `raw`.
* Task DAG di Airflow akan berstatus hijau (*success*).
* Tabel hasil transformasi dbt dapat diverifikasi melalui BigQuery maupun Metabase.
* Log detail setiap task dapat dilihat melalui menu **Logs** pada Airflow UI.

Jika terjadi kegagalan pipeline:

* Periksa status container menggunakan:

```bash
docker ps
```

* Periksa log Airflow:

```bash
docker compose logs airflow-scheduler
```

* Pastikan file:

  * `.env`
  * `credentials/service-account.json`

  sudah berada pada lokasi yang benar.
