import argparse
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--jdbc-host", required=True)
    parser.add_argument("--jdbc-db", required=True)
    parser.add_argument("--jdbc-user", required=True)
    parser.add_argument("--jdbc-pass", required=True)
    parser.add_argument("--jdbc-table", required=True)
    parser.add_argument("--minio-endpoint", default="http://minio:9000")
    parser.add_argument("--minio-access-key", default="admin")
    parser.add_argument("--minio-secret-key", default="admin123")
    args, _ = parser.parse_known_args()

    spark = SparkSession.builder.appName("ScoreModel") \
        .config("spark.hadoop.fs.s3a.endpoint", args.minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", args.minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", args.minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    df = spark.read.parquet(args.silver)
    print(f"📦 Cargando modelo: {args.model}")
    model = PipelineModel.load(args.model)
    predictions = model.transform(df)

    print(f"💾 Guardando predicciones: {args.output}")
    predictions.write.mode("overwrite").parquet(args.output)

    jdbc_url = f"jdbc:mysql://{args.jdbc_host}:3306/{args.jdbc_db}?permitMysqlScheme=true"
    predictions.select("event_id", "user_id", "prediction") \
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