from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

# 1. Setup Spark
spark = SparkSession.builder \
    .appName("LatihanPartisi") \
        .master("spark://spark-master:7077") \
            .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("="*50)
print("[*] EXPERIMENT: PARTITIONING")
print("="*50)

# 2. Baca Data
df = spark.read.csv("transactions.csv", header=True, inferSchema=True)

# Cek jumlah partisi awal (Default Spark)
print(f"[*] Jumlah Partisi Awal: {df.rdd.getNumPartitions()}")

# 3. THE MAGIC: Paksa bagi jadi 4 potong
# pisah menjadi 4 bagian agar bisa dikerjakan 4 worker
print("[*] Sedang memotong data menjadi 4 partisi...")
df_partitioned = df.repartition(4)

print(f"[*] Jumlah Partisi Sekarang: {df_partitioned.rdd.getNumPartitions()}")

# 4. Buktikan isinya
# Hitung ada berapa baris data di setiap partisi
print("[*] Statistik per Partisi:")
df_partitioned.withColumn("partition_id", spark_partition_id()) \
    .groupBy("partition_id") \
        .count() \
            .show()

# 5. Simpan
print("[*] Menyimpan output...")
df_partitioned.write.mode("overwrite").parquet("output_partitioned")
print("[*] Selesai! Cek folder output_partitioned.")

spark.stop()