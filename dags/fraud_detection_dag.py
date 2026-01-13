from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="payment_fraud_batch_pipeline",
    default_args=default_args,
    description="ETL Batch for Fraud Detection - Bronze to Silver (MinIO S3)",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fraud", "spark", "minio", "s3"],
) as dag:

    process_payments_batch = BashOperator(
        task_id="process_payments_batch",
        bash_command="""
            set -e
            echo "🚀 Ejecutando Fraud Pipeline (Spark + MinIO S3)...";
            
            docker exec spark-master /opt/spark/bin/spark-submit \
              --master spark://spark-master:7077 \
              --conf spark.jars.ivy=/tmp/.ivy2 \
              --packages \
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
              /opt/spark/app/fraud_pipeline/job_main.py \
              --mode batch \
              --bucket data \
              --minio-endpoint http://minio:9000 \
              --minio-access-key admin \
              --minio-secret-key admin123 \
              --input s3a://data/bronze/payments/ \
              --output s3a://data/silver/payments_clean/
        """,
    )

    process_payments_batch