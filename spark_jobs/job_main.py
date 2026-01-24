import argparse
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from common.schemas import PAYMENTS_RAW_SCHEMA

# 1. Entrada: Landing Zone (Disco Compartido)
INPUT_PATH_LANDING = "file:///opt/minio/shareddata/landing/payments/"

# 2. Salidas S3 (Bucket Fraude)
BUCKET = "fraude"
OUTPUT_PATH_BRONZE = f"s3a://{BUCKET}/bronze/payments/"
OUTPUT_PATH_SILVER = f"s3a://{BUCKET}/silver/payments_clean/"

def ensure_bucket(endpoint, access_key, secret_key, bucket_name):
    """Crea bucket si no existe usando boto3"""
    try:
        s3 = boto3.resource('s3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1')
        if s3.Bucket(bucket_name).creation_date is None:
            s3.create_bucket(Bucket=bucket_name)
            print(f"📦 Bucket '{bucket_name}' creado.")
    except Exception as e:
        print(f"⚠️ Warning bucket check: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--minio-endpoint", default="http://minio:9000")
    parser.add_argument("--minio-access-key", default="admin")
    parser.add_argument("--minio-secret-key", default="admin123")
    args, _ = parser.parse_known_args()

    ensure_bucket(args.minio_endpoint, args.minio_access_key, args.minio_secret_key, BUCKET)

    spark = SparkSession.builder \
        .appName("Fraud_Ingest_Bronze_Silver") \
        .config("spark.hadoop.fs.s3a.endpoint", args.minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", args.minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", args.minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # --- PASO 1: Ingesta (Landing -> Bronze) ---
    print(f"📖 Leyendo Landing (Share): {INPUT_PATH_LANDING}")
    df_landing = spark.read.schema(PAYMENTS_RAW_SCHEMA).json(INPUT_PATH_LANDING)
    
    if df_landing.count() == 0:
        print("⚠️ No hay datos en Landing.")
        return

    print(f"📦 Escribiendo BRONZE (Raw Copy) en S3: {OUTPUT_PATH_BRONZE}")
    # Guardamos copia exacta en Bronze
    df_landing.write.mode("overwrite").parquet(OUTPUT_PATH_BRONZE)

    # --- PASO 2: Procesamiento (Bronze -> Silver) ---
    print("✨ Limpiando datos para Silver...")
    
    # Leemos de lo que acabamos de escribir en Bronze (para asegurar consistencia) 
    # o usamos el dataframe en memoria df_landing
    df_silver = df_landing.withColumn("event_ts", F.to_timestamp("event_ts")) \
                          .withColumn("ingest_ts", F.current_timestamp()) \
                          .dropDuplicates(["event_id"])

    print(f"💾 Escribiendo SILVER en S3: {OUTPUT_PATH_SILVER}")
    df_silver.write.mode("overwrite").parquet(OUTPUT_PATH_SILVER)
    
    print("✅ Pipeline Bronze -> Silver completado.")
    spark.stop()

if __name__ == "__main__":
    main()