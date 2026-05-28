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
