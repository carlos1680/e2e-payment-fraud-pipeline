import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array


def create_spark(minio_endpoint: str, access_key: str, secret_key: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("ScoreFraudModelAndWriteToMariaDB")
        # --- MinIO / S3A ---
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--silver", required=True)
    p.add_argument("--model", required=True)
    p.add_argument("--output", required=True)

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

    # Ventana de velocidad (igual que silver->gold)
    p.add_argument("--rule-velocity-window-seconds", type=int, default=60)

    args = p.parse_args()

    spark = create_spark(args.minio_endpoint, args.minio_access_key, args.minio_secret_key)

    try:
        print(f"📥 Leyendo Silver desde: {args.silver}")
        df = spark.read.parquet(args.silver)

        required = {"event_id", "user_id", "event_ts"}
        missing = required - set(df.columns)
        if missing:
            raise RuntimeError(f"❌ Silver no tiene columnas requeridas {sorted(missing)}. Tiene: {df.columns}")

        # Normalización defensiva (igual que silver->gold)
        df = (
            df
            .withColumn("event_id", F.col("event_id").cast("string"))
            .withColumn("user_id", F.col("user_id").cast("string"))
            .withColumn("event_ts", F.to_timestamp("event_ts"))
        )

        if "amount" in df.columns:
            df = df.withColumn("amount", F.col("amount").cast("double"))

        # Features temporales
        df = df.withColumn("hour", F.hour("event_ts"))
        df = df.withColumn("dow", F.dayofweek("event_ts"))

        # Calcular tx_count_window (igual que silver->gold)
        df = df.withColumn("event_ts_sec", F.col("event_ts").cast("long"))
        w = (
            Window
            .partitionBy("user_id")
            .orderBy(F.col("event_ts_sec"))
            .rangeBetween(-args.rule_velocity_window_seconds, 0)
        )
        df = df.withColumn("tx_count_window", F.count("*").over(w)).drop("event_ts_sec")
        print("✅ tx_count_window calculada.")

        # Cargar modelo
        print(f"📦 Cargando modelo desde: {args.model}")
        model = PipelineModel.load(args.model)
        print("✅ Modelo cargado.")

        # Aplicar modelo
        print("🔮 Aplicando modelo...")
        preds = model.transform(df)

        # Extraer fraud_probability (clase 1)
        if "probability" in preds.columns:
            preds = preds.withColumn(
                "fraud_probability",
                vector_to_array(F.col("probability")).getItem(1)
            )
        else:
            preds = preds.withColumn("fraud_probability", F.lit(None).cast("double"))

        # Normalizar prediction
        if "prediction" not in preds.columns and "predictedLabel" in preds.columns:
            preds = preds.withColumnRenamed("predictedLabel", "prediction")

        # Seleccionar columnas finales
        output_cols = [
            "event_id", "event_ts", "payment_id", "user_id", "merchant_id",
            "amount", "currency", "payment_method", "ip_address", "device_id",
            "status", "ingest_ts", "tx_count_window",
            "prediction", "fraud_probability"
        ]
        output_cols = [c for c in output_cols if c in preds.columns]

        df_final = preds.select(*output_cols).withColumn("scored_ts", F.current_timestamp())

        print("🧪 Muestra de predicciones:")
        df_final.select("event_id", "user_id", "amount", "tx_count_window", "prediction", "fraud_probability").show(10, truncate=False)

        # 1) Escribir a MinIO
        print(f"💾 Escribiendo predicciones (Parquet) a: {args.output}")
        df_final.write.mode("overwrite").parquet(args.output)

        # 2) Escribir a MariaDB (IGUAL QUE SILVER->GOLD)
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
            df_final
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

        total = df_final.count()
        print(f"✅ OK: Predicciones escritas en MinIO + MariaDB. total={total}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()