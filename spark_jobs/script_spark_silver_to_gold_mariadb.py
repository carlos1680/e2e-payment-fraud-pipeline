import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def create_spark(minio_endpoint: str, access_key: str, secret_key: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("SilverToGoldAndMariaDB")
        # --- MinIO / S3A ---
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # (opcional pero útil)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--silver", required=True)  # s3a://data/silver/payments_clean/
    p.add_argument("--gold", required=True)    # s3a://data/gold/payments_scored/

    p.add_argument("--minio-endpoint", default="http://minio:9000")
    p.add_argument("--minio-access-key", default="admin")
    p.add_argument("--minio-secret-key", default="admin123")

    p.add_argument("--jdbc-host", required=True)
    p.add_argument("--jdbc-port", default="3306")
    p.add_argument("--jdbc-db", required=True)
    p.add_argument("--jdbc-user", required=True)
    p.add_argument("--jdbc-pass", required=True)
    p.add_argument("--jdbc-table", required=True)
    p.add_argument("--write-mode", default="append", choices=["append", "overwrite"])

    # reglas (MVP)
    p.add_argument("--rule-high-amount", type=float, default=5000.0)
    p.add_argument("--rule-velocity-max-tx", type=int, default=3)
    p.add_argument("--rule-velocity-window-seconds", type=int, default=60)

    args = p.parse_args()

    spark = create_spark(args.minio_endpoint, args.minio_access_key, args.minio_secret_key)

    try:
        print(f"📥 Leyendo SILVER desde: {args.silver}")
        df = spark.read.parquet(args.silver)

        required = {"event_id", "user_id", "amount", "event_ts"}
        missing = required - set(df.columns)
        if missing:
            raise RuntimeError(f"❌ SILVER no tiene columnas requeridas {sorted(missing)}. Tiene: {df.columns}")

        # Normalización defensiva
        df0 = (
            df
            .withColumn("event_id", F.col("event_id").cast("string"))
            .withColumn("user_id", F.col("user_id").cast("string"))
            .withColumn("amount", F.col("amount").cast("double"))
            .withColumn("event_ts", F.to_timestamp("event_ts"))
        )

        # Regla de VELOCIDAD: más de N tx en ventana de X segundos por user_id
        # Usamos timestamp en segundos para poder hacer rangeBetween
        df1 = df0.withColumn("event_ts_sec", F.col("event_ts").cast("long"))
        w = (
            Window
            .partitionBy("user_id")
            .orderBy(F.col("event_ts_sec"))
            .rangeBetween(-args.rule_velocity_window_seconds, 0)
        )
        df2 = df1.withColumn("tx_count_window", F.count("*").over(w))

        # Reglas
        high_amount = F.col("amount") > F.lit(args.rule_high_amount)
        velocity = F.col("tx_count_window") > F.lit(args.rule_velocity_max_tx)

        df_gold = (
            df2
            .withColumn(
                "fraud_reason",
                F.when(high_amount, F.lit("HIGH_AMOUNT"))
                 .when(velocity, F.lit("VELOCITY"))
                 .otherwise(F.lit("NONE"))
            )
            .withColumn("is_fraud", (F.col("fraud_reason") != F.lit("NONE")))
            .withColumn(
                "risk_score",
                F.when(high_amount, F.lit(0.9))
                 .when(velocity, F.lit(0.7))
                 .otherwise(F.lit(0.1))
            )
            .withColumn("gold_ts", F.current_timestamp())
            .drop("event_ts_sec")
        )

        print("🧪 Muestra GOLD:")
        df_gold.select("event_id", "user_id", "amount", "event_ts", "tx_count_window", "fraud_reason", "is_fraud", "risk_score").show(10, truncate=False)

        # 1) Write GOLD to MinIO
        print(f"💾 Escribiendo GOLD (Parquet) a: {args.gold}")
        df_gold.write.mode("overwrite").parquet(args.gold)

        # 2) Write to MariaDB
        jdbc_url = (
            f"jdbc:mysql://{args.jdbc_host}:{args.jdbc_port}/{args.jdbc_db}"
            "?useUnicode=true"
            "&permitMysqlScheme=true"
            "&characterEncoding=utf8"
            "&serverTimezone=UTC"
            "&tinyInt1isBit=false"
            "&zeroDateTimeBehavior=convertToNull"
        )

        print(f"🗄️ Insertando en MariaDB: {jdbc_url} -> table={args.jdbc_table} (mode={args.write_mode})")

        (
            df_gold
            .select(
                "event_id", "user_id", "amount", "event_ts",
                "tx_count_window", "fraud_reason", "is_fraud", "risk_score", "gold_ts"
            )
            .write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", args.jdbc_table)
            .option("user", args.jdbc_user)
            .option("password", args.jdbc_pass)
            .option("driver", "org.mariadb.jdbc.Driver")
            .option("batchsize", "1000")
            .mode(args.write_mode)
            .save()
        )

        total = df_gold.count()
        frauds = df_gold.filter(F.col("is_fraud") == True).count()
        print(f"✅ OK: GOLD escrito + DB actualizado. total={total} frauds={frauds}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()