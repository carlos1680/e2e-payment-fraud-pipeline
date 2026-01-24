import argparse
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold", required=True)
    parser.add_argument("--model-out", required=True)
    parser.add_argument("--minio-endpoint", default="http://minio:9000")
    parser.add_argument("--minio-access-key", default="admin")
    parser.add_argument("--minio-secret-key", default="admin123")
    args, _ = parser.parse_known_args()

    spark = SparkSession.builder.appName("TrainModel") \
        .config("spark.hadoop.fs.s3a.endpoint", args.minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", args.minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", args.minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    df = spark.read.parquet(args.gold)
    
    # Preparación features
    df = df.withColumn("label", (df["is_fraud"]).cast("integer"))
    assembler = VectorAssembler(inputCols=["amount", "tx_count_window"], outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    lr = LogisticRegression(featuresCol="scaledFeatures", labelCol="label", maxIter=10)
    
    pipeline = Pipeline(stages=[assembler, scaler, lr])
    model = pipeline.fit(df)

    print(f"💾 Guardando modelo: {args.model_out}")
    model.write().overwrite().save(args.model_out)
    spark.stop()

if __name__ == "__main__":
    main()