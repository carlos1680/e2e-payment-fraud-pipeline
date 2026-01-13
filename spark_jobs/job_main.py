import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from common.schemas import PAYMENTS_RAW_SCHEMA

# 1. Configuración de rutas
# Leemos del disco local (donde ya tenés los archivos)
INPUT_PATH = "file:///opt/minio/shareddata/bronze/payments/"
# Escribimos al Bucket de MinIO
OUTPUT_PATH = "s3a://data/silver/payments_clean/"

def create_spark_session():
    return (
        SparkSession.builder
        .appName("Fraud_Bronze_to_Silver_Bucket")
        # Configuración para poder escribir en el Bucket
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "admin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    
    print(f"📖 Leyendo JSONs desde DISCO: {INPUT_PATH}")
    # Leemos los JSON que ya tenés en la carpeta bronze
    df_raw = spark.read.schema(PAYMENTS_RAW_SCHEMA).json(INPUT_PATH)

    if df_raw.count() == 0:
        print("⚠️ No se encontraron datos en la carpeta bronze.")
        return

    print("✨ Limpiando datos...")
    df_cleaned = (
        df_raw
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("ingest_ts", F.current_timestamp())
        .dropDuplicates(["event_id"])
    )

    print(f"💾 Guardando datos limpios en BUCKET: {OUTPUT_PATH}")
    # Guardamos en el bucket 'data' en formato Parquet
    df_cleaned.write.mode("overwrite").parquet(OUTPUT_PATH)
    
    print("✅ Proceso completado con éxito.")
    spark.stop()

if __name__ == "__main__":
    main()