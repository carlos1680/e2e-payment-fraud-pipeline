from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DOCKER_BIN = "docker"
SPARK_CONTAINER_NAME = "spark-master"
SPARK_SUBMIT_PATH = "/opt/spark/bin/spark-submit"
SPARK_MASTER_URL = "spark://spark-master:7077"

SPARK_APP_PATH = "/opt/spark/app/fraud_pipeline/script_spark_train_model_from_gold.py"

with DAG(
    dag_id="dag_train_model_from_gold_v1",
    default_args=default_args,
    description="Entrena modelo ML desde Gold (MinIO) y guarda el modelo en MinIO",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spark", "ml", "minio", "gold", "model"],
) as dag:

    train_model = BashOperator(
        task_id="train_fraud_model",
        bash_command=f"""
            set -e
            echo "🚀 Entrenando modelo desde Gold..."

            {DOCKER_BIN} exec {SPARK_CONTAINER_NAME} bash -lc '
              mkdir -p /tmp/.ivy2 && chmod -R 777 /tmp/.ivy2

              {SPARK_SUBMIT_PATH} \
                --master {SPARK_MASTER_URL} \
                --conf spark.jars.ivy=/tmp/.ivy2 \
                --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
                --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
                --conf spark.hadoop.fs.s3a.access.key=admin \
                --conf spark.hadoop.fs.s3a.secret.key=admin123 \
                --conf spark.hadoop.fs.s3a.path.style.access=true \
                --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
                --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
                {SPARK_APP_PATH} \
                --gold s3a://data/gold/payments_scored/ \
                --model-out s3a://data/models/fraud_lr_v1/
            '
        """,
    )

    train_model