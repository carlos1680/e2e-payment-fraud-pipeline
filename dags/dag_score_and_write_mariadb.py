from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DOCKER_BIN = "docker"
SPARK_CONTAINER_NAME = "spark-master"
SPARK_SUBMIT_PATH = "/opt/spark/bin/spark-submit"
SPARK_MASTER_URL = "spark://spark-master:7077"

SPARK_APP_PATH = "/opt/spark/app/fraud_pipeline/script_spark_score_and_write_to_mariadb.py"

with DAG(
    dag_id="dag_score_and_write_mariadb_v1",
    default_args=default_args,
    description="Score desde Silver, guarda en MinIO y en MariaDB",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spark", "scoring", "mariadb"],
) as dag:

    score_and_write = BashOperator(
        task_id="score_and_write",
        bash_command=f"""
            set -e
            echo "🚀 Scoring + write to MariaDB..."

            {DOCKER_BIN} exec {SPARK_CONTAINER_NAME} {SPARK_SUBMIT_PATH} \
              --master {SPARK_MASTER_URL} \
              --conf spark.jars.ivy=/tmp/.ivy2 \
              --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.mariadb.jdbc:mariadb-java-client:3.1.2 \
              {SPARK_APP_PATH} \
              --silver s3a://data/silver/payments_clean/ \
              --model s3a://data/models/fraud_lr_v1/ \
              --output s3a://data/gold/predictions/ \
              --minio-endpoint http://minio:9000 \
              --minio-access-key admin \
              --minio-secret-key admin123 \
              --jdbc-host 172.28.0.10 \
              --jdbc-port 3306 \
              --jdbc-db bigdata_db \
              --jdbc-user bigdata_user \
              --jdbc-pass bigdata_pass \
              --jdbc-table predictions \
              --write-mode append
        """,
    )

    score_and_write