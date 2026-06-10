# 🎓 Implementasi Big Data Ecosystem untuk Analisis Pemerataan Pendidikan di Indonesia Menggunakan Apache Spark

> **Big Data | Apache Spark | ETL Pipeline | Feature Engineering | PCA | K-Means Clustering**

---

# 📖 Deskripsi Proyek

Pemerataan pendidikan merupakan salah satu indikator penting dalam pembangunan sumber daya manusia di Indonesia. Namun, data yang digunakan untuk mengevaluasi kondisi pendidikan umumnya tersebar pada berbagai sumber dengan struktur dan karakteristik yang berbeda sehingga memerlukan proses integrasi dan transformasi sebelum dapat dianalisis.

Proyek ini mengimplementasikan **Big Data Ecosystem berbasis Apache Spark** untuk membangun sebuah *pipeline* pengolahan data pendidikan yang terdiri atas **Bronze Layer**, **Silver Layer**, dan **Gold Layer**. Melalui proses **Extract, Transform, Load (ETL)**, data dari berbagai sumber dibersihkan, diintegrasikan, dan ditransformasikan menjadi dataset analisis yang siap digunakan untuk proses *machine learning*.

Tahap analisis dilakukan menggunakan **Principal Component Analysis (PCA)** untuk reduksi dimensi dan **K-Means Clustering** untuk mengelompokkan provinsi berdasarkan karakteristik pemerataan pendidikan.

---

# 🎯 Tujuan

Implementasi ini bertujuan untuk:

* Membangun ekosistem Big Data menggunakan Apache Spark.
* Mengintegrasikan berbagai dataset pendidikan menjadi satu dataset terpusat.
* Melakukan proses ETL secara terstruktur melalui arsitektur Bronze–Silver–Gold.
* Membentuk fitur baru (*feature engineering*) untuk menghasilkan indikator yang lebih representatif.
* Mengidentifikasi pola pemerataan pendidikan menggunakan K-Means Clustering.
* Mengevaluasi kualitas clustering menggunakan Silhouette Score.

---

# 🏛️ Arsitektur Pipeline

```text
                    RAW DATASET

        APK ───────────────┐
        APM ───────────────┤
        Data SD ───────────┤
        Data SMP ──────────┤
        Lab SD ────────────┤
        Lab SMP ───────────┘
                  │
                  ▼
        ┌──────────────────────┐
        │     Bronze Layer     │
        │    (Raw Dataset)     │
        └──────────────────────┘
                  │
                  ▼
        Data Cleaning
        Data Validation
        Standardization
        Data Aggregation
                  │
                  ▼
        ┌──────────────────────┐
        │     Silver Layer     │
        │ Integrated Dataset   │
        └──────────────────────┘
                  │
                  ▼
        Feature Engineering
                  │
                  ▼
        ┌──────────────────────┐
        │      Gold Layer      │
        │  Analysis Dataset    │
        └──────────────────────┘
                  │
                  ▼
          StandardScaler
                  │
                  ▼
 Principal Component Analysis
                  │
                  ▼
        K-Means Clustering
                  │
                  ▼
      Cluster Interpretation
```

---

# 📂 Dataset

Penelitian memanfaatkan enam dataset pendidikan yang diperoleh dari sumber resmi pemerintah.

| Dataset          | Deskripsi                                                                 |
| ---------------- | ------------------------------------------------------------------------- |
| APK              | Angka Partisipasi Kasar Menurut Provinsi dan Jenjang Pendidikan           |
| APM              | Angka Partisipasi Murni Menurut Provinsi dan Jenjang Pendidikan           |
| Data SD          | Jumlah Sekolah, Guru, dan Murid Sekolah Dasar Menurut Provinsi            |
| Data SMP         | Jumlah Sekolah, Guru, dan Murid Sekolah Menengah Pertama Menurut Provinsi |
| Laboratorium SD  | Kondisi Laboratorium IPA SD Menurut Kabupaten/Kota Tahun 2024             |
| Laboratorium SMP | Kondisi Laboratorium IPA SMP Menurut Kabupaten/Kota Tahun 2024            |

## 📊 Ringkasan Dataset

Penelitian ini memanfaatkan **enam dataset pendidikan** yang berasal dari sumber resmi pemerintah Indonesia. Setiap dataset memiliki struktur, tingkat granularitas, dan jumlah atribut yang berbeda sehingga diperlukan proses *data cleaning*, seleksi atribut, serta integrasi menggunakan **Apache Spark** sebelum dilakukan analisis.

| Dataset                                                               | Jumlah Data | Jumlah Atribut Asli | Atribut yang Digunakan                                                  |
| --------------------------------------------------------------------- | ----------: | ------------------: | ----------------------------------------------------------------------- |
| Angka Partisipasi Kasar (APK) Menurut Provinsi dan Jenjang Pendidikan |    40 baris |                   4 | `provinsi`, `apk_sd`, `apk_smp`                                         |
| Angka Partisipasi Murni (APM) Menurut Provinsi dan Jenjang Pendidikan |    40 baris |                   4 | `provinsi`, `apm_sd`, `apm_smp`                                         |
| Jumlah Sekolah, Guru, dan Murid SD Menurut Provinsi                   |    39 baris |                   7 | `provinsi`, `jumlah_sekolah_sd`, `jumlah_guru_sd`, `jumlah_murid_sd`    |
| Jumlah Sekolah, Guru, dan Murid SMP Menurut Provinsi                  |    39 baris |                   7 | `provinsi`, `jumlah_sekolah_smp`, `jumlah_guru_smp`, `jumlah_murid_smp` |
| Kondisi Laboratorium IPA SD Menurut Kabupaten/Kota Tahun 2024         |   514 baris |                   4 | `provinsi`, `jumlah_laboratorium_sd`                                    |
| Kondisi Laboratorium IPA SMP Menurut Kabupaten/Kota Tahun 2024        |   514 baris |                   4 | `provinsi`, `jumlah_laboratorium_smp`                                   |

Dataset **APK** dan **APM** digunakan sebagai indikator partisipasi pendidikan, sedangkan dataset **SD** dan **SMP** digunakan untuk memperoleh informasi jumlah sekolah, guru, dan murid pada masing-masing jenjang pendidikan. Sementara itu, dataset **laboratorium IPA SD** dan **laboratorium IPA SMP** pada awalnya memiliki unit analisis pada tingkat **kabupaten/kota**, sehingga dilakukan proses agregasi berdasarkan atribut **provinsi** menggunakan Apache Spark agar dapat diintegrasikan dengan dataset lainnya.

Setelah melalui proses *data cleaning*, standardisasi, agregasi, dan integrasi, diperoleh **Silver Layer** yang terdiri atas **38 observasi (provinsi)** dan **15 atribut**. Selanjutnya dilakukan proses *feature engineering* untuk membentuk **Gold Layer** dengan **38 observasi** dan **8 variabel analisis**, yang kemudian digunakan sebagai input pada tahap **StandardScaler**, **Principal Component Analysis (PCA)**, dan **K-Means Clustering**.

---


# ⚙️ Tahapan Implementasi

## 1. Bronze Layer

Bronze Layer merupakan tahap awal dalam pipeline Big Data yang berfungsi sebagai tempat penyimpanan data mentah (*raw data*).

Pada tahap ini seluruh dataset dibaca menggunakan **Apache Spark** dan dimuat ke dalam **Spark DataFrame** tanpa mengubah struktur maupun isi data.

### Aktivitas

* Import dataset CSV
* Membuat Spark DataFrame
* Menyimpan raw dataset

### Output

```
bronze/
├── apk.csv
├── apm.csv
├── sd.csv
├── smp.csv
├── lab_sd.csv
└── lab_smp.csv
```

---

## 2. Data Cleaning

Tahap *data cleaning* dilakukan untuk meningkatkan kualitas data sebelum proses integrasi.

### APK

* Menghapus metadata
* Standardisasi nama provinsi
* Konversi tipe data numerik

### APM

* Menghapus metadata
* Standardisasi nama provinsi
* Konversi tipe data numerik

### Data SD

* Menghapus data agregat Indonesia
* Standardisasi nama provinsi
* Seleksi atribut

### Data SMP

* Menghapus data agregat Indonesia
* Standardisasi nama provinsi
* Seleksi atribut

### Laboratorium SD

* Filtering data
* Agregasi berdasarkan provinsi

### Laboratorium SMP

* Filtering data
* Agregasi berdasarkan provinsi

### Output

```
apk_clean
apm_clean
sd_clean
smp_clean
lab_sd_clean
lab_smp_clean
```

---

## 3. Silver Layer

Dataset hasil *cleaning* kemudian diintegrasikan berdasarkan atribut **provinsi** menggunakan operasi **join** pada Apache Spark.

Tahap ini menghasilkan satu dataset terpusat yang memuat seluruh indikator pendidikan.

### Hasil

| Keterangan       | Nilai |
| ---------------- | ----: |
| Jumlah observasi |    38 |
| Jumlah atribut   |    15 |

### Output

```
silver_master.csv
```

---

## 4. Feature Engineering (Gold Layer)

Tahap ini bertujuan menghasilkan indikator yang lebih representatif dibandingkan data absolut.

Empat fitur baru dibentuk:

| Fitur                  | Rumus                          |
| ---------------------- | ------------------------------ |
| Rasio Guru SD          | Murid SD ÷ Guru SD             |
| Rasio Guru SMP         | Murid SMP ÷ Guru SMP           |
| Rasio Laboratorium SD  | Laboratorium SD ÷ Sekolah SD   |
| Rasio Laboratorium SMP | Laboratorium SMP ÷ Sekolah SMP |

Keempat fitur tersebut dikombinasikan dengan:

* APK SD
* APK SMP
* APM SD
* APM SMP

sehingga menghasilkan dataset analisis dengan total **8 variabel**.

### Hasil

| Keterangan       | Nilai |
| ---------------- | ----: |
| Jumlah observasi |    38 |
| Jumlah variabel  |     8 |

### Output

```
gold_pendidikan.csv
```

---

## 5. Standardisasi Data

Karena setiap variabel memiliki satuan yang berbeda, dilakukan standardisasi menggunakan **StandardScaler**.

### Tujuan

* Menyamakan skala fitur
* Menghindari dominasi variabel tertentu
* Meningkatkan kualitas clustering

### Output

```
scaled_features
```

---

## 6. Principal Component Analysis (PCA)

PCA digunakan untuk mereduksi dimensi data tanpa kehilangan sebagian besar informasi.

### Hasil Explained Variance

| Komponen | Persentase |
| -------- | ---------: |
| PC1      |     49.26% |
| PC2      |     29.90% |
| PC3      |      8.77% |

Total informasi yang dipertahankan:

> **87.93%**

### Output

```
pca_features
```

---

## 7. K-Means Clustering

Dataset hasil PCA digunakan sebagai input algoritma K-Means.

### Parameter

```
k = 3
```

### Distribusi Cluster

| Cluster   | Jumlah Provinsi |
| --------- | --------------: |
| Cluster 0 |              10 |
| Cluster 1 |              26 |
| Cluster 2 |               2 |

### Interpretasi

Cluster 0 – Provinsi dengan Akses dan Fasilitas Pendidikan Relatif Baik

Cluster 0 terdiri atas provinsi-provinsi yang memiliki nilai Angka Partisipasi Kasar (APK), Angka Partisipasi Murni (APM), serta rasio laboratorium yang relatif lebih tinggi dibandingkan cluster lainnya. Karakteristik tersebut menunjukkan bahwa provinsi dalam kelompok ini memiliki tingkat partisipasi pendidikan yang baik dan didukung oleh ketersediaan fasilitas pendidikan yang lebih memadai.

Cluster 1 – Provinsi dengan Karakteristik Pendidikan Menengah

Cluster 1 merupakan kelompok dengan jumlah anggota terbanyak dan mencakup sebagian besar provinsi di Indonesia. Provinsi pada cluster ini memiliki nilai indikator pendidikan yang cenderung berada pada tingkat menengah, baik dari sisi partisipasi pendidikan maupun ketersediaan sumber daya pendidikan. Kelompok ini merepresentasikan kondisi pemerataan pendidikan yang relatif stabil, namun masih memiliki peluang untuk ditingkatkan.

Cluster 2 – Provinsi dengan Tantangan Pemerataan Pendidikan

Cluster 2 terdiri atas Papua Tengah dan Papua Pegunungan, yang memiliki nilai APK, APM, dan rasio laboratorium relatif lebih rendah dibandingkan cluster lainnya. Karakteristik tersebut menunjukkan masih adanya tantangan dalam pemerataan akses pendidikan dan penyediaan fasilitas pendukung pembelajaran, sehingga wilayah dalam cluster ini memerlukan perhatian dan kebijakan yang lebih terarah untuk meningkatkan kualitas layanan pendidikan.

---

## 8. Evaluasi Model

Kualitas clustering dievaluasi menggunakan **Silhouette Score**.

### Hasil

| Metrik           |      Nilai |
| ---------------- | ---------: |
| Silhouette Score | **0.6100** |

Nilai tersebut menunjukkan bahwa cluster yang terbentuk memiliki tingkat pemisahan (*separation*) dan kekompakan (*cohesion*) yang cukup baik.

---

# 📁 Struktur Repository

```
project/
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── notebooks/
│
├── scripts/
│
├── output/
│
├── docker-compose.yml
│
└── README.md
```

---

# 🚀 Cara Menjalankan

1. Jalankan Docker.
2. Buat Spark Session menggunakan PySpark.
3. Import seluruh dataset ke Bronze Layer.
4. Jalankan proses *data cleaning*.
5. Integrasikan data menjadi Silver Layer.
6. Lakukan *feature engineering* untuk membentuk Gold Layer.
7. Terapkan StandardScaler.
8. Jalankan PCA.
9. Jalankan K-Means Clustering.
10. Evaluasi hasil menggunakan Silhouette Score.

---

# 📊 Ringkasan Hasil

| Tahapan      | Hasil                                        |
| ------------ | -------------------------------------------- |
| Bronze Layer | 6 dataset mentah                             |
| Silver Layer | 38 observasi, 15 atribut                     |
| Gold Layer   | 38 observasi, 8 variabel                     |
| PCA          | 3 komponen utama (87.93% explained variance) |
| K-Means      | 3 cluster                                    |
| Evaluasi     | Silhouette Score = 0.6100                    |

---

# 🛠️ Teknologi

* Apache Spark (PySpark)
* Docker
* Python
* Pandas
* NumPy

---

# 👥 Tim

* Nurul Izzah Istiqoamah
* Tesalonika Hutajulu
* Mia Al Musdari
* Hanifah Inaya Sani

---
