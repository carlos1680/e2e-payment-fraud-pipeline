from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {"owner": "airflow", "retries": 0}
SPARK_APP_PATH = "/opt/spark/app/fraud_pipeline/script_spark_train_model_from_gold.py"

with DAG(
    dag_id="dag_train_model_from_gold_v1",
    default_args=default_args,
    description="Entrenamiento ML (Configuración Final)",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["fraud", "train", "ml"],
) as dag:

    train_model = BashOperator(
        task_id="train_fraud_model",
        bash_command=f"""
            docker exec -e HOME=/tmp spark-master /opt/spark/bin/spark-submit \
              --master spark://spark-master:7077 \
              --conf spark.eventLog.enabled=true \
              --conf spark.eventLog.dir=file:///tmp/spark-events \
              --conf spark.jars.ivy=/tmp/.ivy2 \
              --conf spark.sql.parquet.enableVectorizedReader=false \
              {SPARK_APP_PATH} \
              --gold s3a://fraude/gold/payments_scored/ \
              --model-out s3a://fraude/models/fraud_lr_v1/ \
              --minio-endpoint http://minio:9000 \
              --minio-access-key admin \
              --minio-secret-key admin123
        """
    )