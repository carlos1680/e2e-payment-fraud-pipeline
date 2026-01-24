from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

SPARK_APP_PATH = "/opt/spark/app/fraud_pipeline/script_spark_silver_to_gold_mariadb.py"

with DAG(
    dag_id="dag_silver_to_gold_mariadb_v1",
    default_args=default_args,
    description="Silver -> Gold + MariaDB (Configuración Final)",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["fraud", "silver_to_gold", "mariadb"],
) as dag:

    silver_to_gold = BashOperator(
        task_id="silver_to_gold_and_mariadb",
        bash_command=f"""
            set -e
            echo "🚀 Ejecutando Silver -> Gold + MariaDB..."

            # CONFIGURACIÓN FINAL:
            # 1. Sin --packages (Todo ya está en el Docker).
            # 2. Sin userClassPathFirst (Para evitar el crash de Netty).
            # 3. HOME=/tmp para logs.
            
            docker exec -e HOME=/tmp spark-master /opt/spark/bin/spark-submit \
              --master spark://spark-master:7077 \
              --conf spark.eventLog.enabled=true \
              --conf spark.eventLog.dir=file:///tmp/spark-events \
              --conf spark.jars.ivy=/tmp/.ivy2 \
              --conf spark.sql.parquet.enableVectorizedReader=false \
              {SPARK_APP_PATH} \
              --silver s3a://fraude/silver/payments_clean/ \
              --gold s3a://fraude/gold/payments_scored/ \
              --minio-endpoint http://minio:9000 \
              --minio-access-key admin \
              --minio-secret-key admin123 \
              --jdbc-host 172.28.0.10 \
              --jdbc-db bigdata_db \
              --jdbc-user bigdata_user \
              --jdbc-pass bigdata_pass \
              --jdbc-table payments_gold
        """
    )

    silver_to_gold