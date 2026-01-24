from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {"owner": "airflow", "retries": 0}
SPARK_APP_PATH = "/opt/spark/app/fraud_pipeline/script_spark_score_and_write_to_mariadb.py"

with DAG(
    dag_id="dag_score_and_write_mariadb_v1",
    default_args=default_args,
    description="Scoring + MariaDB (Configuración Final)",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["fraud", "score"],
) as dag:

    score_task = BashOperator(
        task_id="score_and_write",
        bash_command=f"""
            docker exec -e HOME=/tmp spark-master /opt/spark/bin/spark-submit \
              --master spark://spark-master:7077 \
              --conf spark.eventLog.enabled=true \
              --conf spark.eventLog.dir=file:///tmp/spark-events \
              --conf spark.jars.ivy=/tmp/.ivy2 \
              --conf spark.sql.parquet.enableVectorizedReader=false \
              {SPARK_APP_PATH} \
              --silver s3a://fraude/gold/payments_scored/ \
              --model s3a://fraude/models/fraud_lr_v1/ \
              --output s3a://fraude/gold/predictions/ \
              --minio-endpoint http://minio:9000 \
              --minio-access-key admin \
              --minio-secret-key admin123 \
              --jdbc-host 172.28.0.10 \
              --jdbc-db bigdata_db \
              --jdbc-user bigdata_user \
              --jdbc-pass bigdata_pass \
              --jdbc-table predictions
        """
    )
    # NOTA: En la línea '--silver', cambié la ruta a s3a://fraude/gold/...
    # Mantuve el nombre del argumento '--silver' para no tener que tocar tu script Python,
    # aunque conceptualmente ahora le estamos pasando datos Gold.

    score_task