import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
    StandardScaler,
)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator


LEAK_COLS = {"is_fraud", "fraud_reason", "risk_score"}  # no usar como features


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--gold", required=True)          # s3a://data/gold/payments_scored/
    p.add_argument("--model-out", required=True)     # s3a://data/models/fraud_lr_v1/
    p.add_argument("--minio-endpoint", default="http://minio:9000")
    p.add_argument("--minio-access-key", default="admin")
    p.add_argument("--minio-secret-key", default="admin123")
    p.add_argument("--seed", type=int, default=42)

    args = p.parse_args()

    spark = (
        SparkSession.builder
        .appName("TrainFraudModelFromGold")
        .config("spark.hadoop.fs.s3a.endpoint", args.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", args.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", args.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(args.gold)
        print("✅ Gold leído. Columnas:", df.columns)

        # Label
        if "is_fraud" not in df.columns:
            raise RuntimeError("❌ No existe columna is_fraud en Gold.")
        df = df.withColumn("label", F.col("is_fraud").cast("int"))

        # Features numéricas
        numeric_candidates = ["amount", "tx_count_window"]
        num_features = [c for c in numeric_candidates if c in df.columns]

        # Features temporales
        if "event_ts" in df.columns:
            df = df.withColumn("event_ts", F.to_timestamp("event_ts"))
            df = df.withColumn("hour", F.hour("event_ts"))
            df = df.withColumn("dow", F.dayofweek("event_ts"))
            num_features += ["hour", "dow"]

        # Features categóricas (las que vos tenés en Gold)
        cat_candidates = ["currency", "payment_method", "merchant_id", "status"]
        cat_features = [c for c in cat_candidates if c in df.columns]

        # Opcional: ip_address y device_id pueden “memorizar” (sirve pero cuidado).
        # Para MVP los incluyo porque son útiles, pero si querés generalizar mejor,
        # se pueden sacar o hacer hashing.
        if "device_id" in df.columns:
            cat_features.append("device_id")
        if "ip_address" in df.columns:
            cat_features.append("ip_address")

        # Eliminar leakage + seleccionar
        keep_cols = set(num_features + cat_features + ["label"])
        df = df.select(*keep_cols).dropna()

        # Chequeo de balance
        counts = {r["label"]: r["count"] for r in df.groupBy("label").count().collect()}
        pos = counts.get(1, 0)
        neg = counts.get(0, 0)
        print("📊 label counts:", counts)
        if pos == 0 or neg == 0:
            raise RuntimeError(f"❌ Dataset inválido: no hay ambas clases. counts={counts}")

        # Peso para desbalance
        w_pos = neg / pos
        df = df.withColumn("class_weight", F.when(F.col("label") == 1, F.lit(w_pos)).otherwise(F.lit(1.0)))

        # Split (MVP random). Si querés, hacemos split temporal después.
        train, test = df.randomSplit([0.8, 0.2], seed=args.seed)

        stages = []

        # Indexers + OneHot para categóricas
        indexed_cols = []
        ohe_input_cols = []
        ohe_output_cols = []

        for c in cat_features:
            idx = StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
            stages.append(idx)
            indexed_cols.append(f"{c}_idx")
            ohe_input_cols.append(f"{c}_idx")
            ohe_output_cols.append(f"{c}_ohe")

        if cat_features:
            ohe = OneHotEncoder(inputCols=ohe_input_cols, outputCols=ohe_output_cols, handleInvalid="keep")
            stages.append(ohe)

        # Ensamblado
        assembler_inputs = num_features + ohe_output_cols
        assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features_raw")
        scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=False, withStd=True)

        stages += [assembler, scaler]

        lr = LogisticRegression(
            featuresCol="features",
            labelCol="label",
            weightCol="class_weight",
            maxIter=50,
            regParam=0.01,
            elasticNetParam=0.0,
        )
        stages.append(lr)

        pipeline = Pipeline(stages=stages)
        model = pipeline.fit(train)

        pred = model.transform(test)

        auc_roc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC").evaluate(pred)
        auc_pr = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR").evaluate(pred)

        print(f"📈 AUC ROC: {auc_roc}")
        print(f"📈 AUC PR : {auc_pr}")

        print(f"💾 Guardando modelo en: {args.model_out}")
        model.write().overwrite().save(args.model_out)

        print("✅ Entrenamiento OK.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()