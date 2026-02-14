from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg

# 1. Inisialisasi Spark
spark = SparkSession.builder \
    .appName("LatihanMigrasi") \
        .master("spark://spark-master:7077") \
            .getOrCreate()

# biar log ga berisik
spark.sparkContext.setLogLevel("WARN")

print("="*50)
print("[*] MEMULAI PROSES ETL...")
print("="*50)

# 2. READ DATA
print("[*] Sedang membaca transactions.csv...")
df = spark.read.csv("file:///opt/airflow/dags/transactions.csv", header=True, inferSchema=True)

# 3. TRANSFORMATION
print("[*] Sedang menghitung omzet per kategori...")
result = df.filter(col("quantity") > 2) \
           .groupBy("category") \
               .agg(sum("price").alias("total_sales"), avg("quantity").alias("avg_qty"))

# 4. ACTION (SHOW)
print("\n" + "="*50)
print("[*] HASIL PERHITUNGAN AKHIR:")
print("="*50)
result.show() # <--- INI TABEL YANG KAMU CARI
print("="*50 + "\n")

# 5. SAVE (Opsional: Kita aktifkan biar ada bukti fisik)
print("[*] Menyimpan hasil ke folder output...")
result.write.mode("overwrite").parquet("output_laporan_omzet")
print("[+] Selesai tersimpan!")

spark.stop()