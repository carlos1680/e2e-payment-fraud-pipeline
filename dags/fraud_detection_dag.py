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
    dag_id="payment_fraud_batch_pipeline_v1",
    default_args=default_args,
    description="ETL Batch: Landing -> Bronze -> Silver (Configuración Final)",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fraud", "ingest", "spark"],
) as dag:

    process_batch = BashOperator(
        task_id="process_payments_batch",
        bash_command="""
            set -e
            echo "🚀 Ejecutando Ingesta (Landing -> Silver)..."
            
            # FIX FINAL:
            # - Sin --packages (Hadoop y AWS ya están en Docker)
            # - Sin userClassPathFirst (Para que Netty no choque)
            # - HOME=/tmp e Ivy en /tmp (Para que no falle por usuario sin home)
            
            docker exec -e HOME=/tmp spark-master /opt/spark/bin/spark-submit \
              --master spark://spark-master:7077 \
              --conf spark.eventLog.enabled=true \
              --conf spark.eventLog.dir=file:///tmp/spark-events \
              --conf spark.jars.ivy=/tmp/.ivy2 \
              --conf spark.sql.parquet.enableVectorizedReader=false \
              /opt/spark/app/fraud_pipeline/job_main.py \
              --minio-endpoint http://minio:9000 \
              --minio-access-key admin \
              --minio-secret-key admin123
        """
    )