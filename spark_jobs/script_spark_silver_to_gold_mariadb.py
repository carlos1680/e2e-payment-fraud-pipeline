import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main():
    parser = argparse.ArgumentParser()
    # Argumentos obligatorios que pasa el DAG
    parser.add_argument("--silver", required=True)
    parser.add_argument("--gold", required=True)
    parser.add_argument("--jdbc-host", required=True)
    parser.add_argument("--jdbc-db", required=True)
    parser.add_argument("--jdbc-user", required=True)
    parser.add_argument("--jdbc-pass", required=True)
    parser.add_argument("--jdbc-table", required=True)
    # Opcionales MinIO
    parser.add_argument("--minio-endpoint", default="http://minio:9000")
    parser.add_argument("--minio-access-key", default="admin")
    parser.add_argument("--minio-secret-key", default="admin123")
    # Ignorar args extra si los hay
    args, _ = parser.parse_known_args()

    spark = SparkSession.builder.appName("SilverToGold") \
        .config("spark.hadoop.fs.s3a.endpoint", args.minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", args.minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", args.minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print(f"📥 Leyendo Silver: {args.silver}")
    df = spark.read.parquet(args.silver)

    # Lógica de negocio
    w = Window.partitionBy("user_id").orderBy(F.col("event_ts").cast("long")).rangeBetween(-60, 0)
    df_gold = df.withColumn("tx_count_window", F.count("*").over(w)) \
                .withColumn("fraud_reason", 
                            F.when(F.col("amount") > 5000, "HIGH_AMOUNT")
                            .when(F.col("tx_count_window") > 3, "VELOCITY")
                            .otherwise("NONE")) \
                .withColumn("is_fraud", F.col("fraud_reason") != "NONE") \
                .withColumn("gold_ts", F.current_timestamp())

    print(f"💾 Guardando Gold: {args.gold}")
    df_gold.write.mode("overwrite").parquet(args.gold)

    # MariaDB
    jdbc_url = f"jdbc:mysql://{args.jdbc_host}:3306/{args.jdbc_db}?permitMysqlScheme=true"
    print(f"🗄️ Insertando en MariaDB: {args.jdbc_table}")
    
    df_gold.select("event_id", "user_id", "amount", "event_ts", "fraud_reason", "is_fraud", "gold_ts") \
           .write.format("jdbc") \
           .option("url", jdbc_url) \
           .option("dbtable", args.jdbc_table) \
           .option("user", args.jdbc_user) \
           .option("password", args.jdbc_pass) \
           .option("driver", "org.mariadb.jdbc.Driver") \
           .mode("append").save()

    spark.stop()

if __name__ == "__main__":
    main()