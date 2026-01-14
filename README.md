# E2E Payment Fraud Pipeline (BigDataStack)

End-to-end **payment fraud detection** demo built to run on the local Docker-based environment provided by **BIGDATASTACK**.

This project showcases a practical, portfolio-ready workflow:

- **Bronze → Silver → Gold (Medallion Architecture)** on **MinIO (S3-compatible)**
- **Batch ETL** with **Apache Spark**
- **Orchestration** with **Apache Airflow**
- **Serving/analytics** via **MariaDB** (and optionally **Superset**)
- Optional **ML training** (Spark ML) + **batch scoring**

Base environment (required): [BIGDATASTACK](https://github.com/carlos1680/big-data-stack.git)

Project repo: [e2e-payment-fraud-pipeline](https://github.com/carlos1680/e2e-payment-fraud-pipeline.git)

## 1) Architecture (high level)

**Data flow**

1. **Synthetic events** are generated as **JSON Lines** (`data/input_events/*.json`).
2. Events are copied into the BIGDATASTACK shared MinIO data directory:
   - Host path: `big-data-stack/volumenes/shared/minio/data/bronze/payments/`
   - Inside Spark containers: `/opt/minio/shareddata/bronze/payments/`
3. **Spark (Bronze → Silver)** reads raw JSON and writes cleaned Parquet to MinIO bucket:
   - `s3a://data/silver/payments_clean/`
4. **Spark (Silver → Gold + Rules)** computes simple fraud signals (MVP rules), writes:
   - `s3a://data/gold/payments_scored/`
   - and appends results to **MariaDB** (`payments_gold` table).
5. **Spark (Train model)** trains a **Logistic Regression** model from Gold and saves:
   - `s3a://data/models/fraud_lr_v1/`
6. **Spark (Score)** loads Silver + model, produces predictions and writes:
   - `s3a://data/gold/predictions/`
   - and appends to **MariaDB** (`predictions` table).

## 2) What’s included

### Airflow DAGs (`dags/`)

- `payment_fraud_batch_pipeline` (**Bronze → Silver**) — runs `spark_jobs/job_main.py`
- `dag_silver_to_gold_mariadb_v1` (**Silver → Gold + MariaDB**) — runs `spark_jobs/script_spark_silver_to_gold_mariadb.py`
- `dag_train_model_from_gold_v1` (**Train ML model**) — runs `spark_jobs/script_spark_train_model_from_gold.py`
- `dag_score_and_write_mariadb_v1` (**Score + MariaDB**) — runs `spark_jobs/script_spark_score_and_write_to_mariadb.py`

### Spark jobs (`spark_jobs/`)

- `job_main.py`: Bronze → Silver (reads raw JSON from `/opt/minio/shareddata/bronze/payments/`, writes Parquet to `s3a://data/silver/payments_clean/`).
- `script_spark_silver_to_gold_mariadb.py`: Silver → Gold using **rule-based** detection + writes to MariaDB.
  - Rules (defaults):
    - `HIGH_AMOUNT`: amount > `5000`
    - `VELOCITY`: more than `3` tx in `60` seconds per user
- `script_spark_train_model_from_gold.py`: trains a **LogisticRegression** model from Gold (prevents leakage by excluding `is_fraud`, `fraud_reason`, `risk_score` as features).
- `script_spark_score_and_write_to_mariadb.py`: loads the saved PipelineModel, computes `fraud_probability` and writes results.

### Data generator (`data/`)

- `raw_generator.py`: generates a small batch of synthetic payment events as JSON Lines.

### SQL (`sql/`)

- `init_db.sql`: optional demo DB objects (e.g., `blacklist_users`).

## 3) Prerequisites

1. Clone and start **BIGDATASTACK** first.
2. Ensure the MinIO bucket name matches the code (typically `data`).
3. You need enough RAM (recommended **16GB**) because Spark + Airflow + Superset can be heavy.

## 4) Run it end-to-end (recommended flow)

### Step A — Start BIGDATASTACK

In the BIGDATASTACK repo:

```bash
chmod +x controller.sh
./controller.sh up
```

Useful local URLs (defaults):

- Airflow: http://localhost:8090
- MinIO Console: http://localhost:9001
- Superset: http://localhost:8088
- Adminer (MariaDB UI): http://localhost:8089
- Spark Master UI: http://localhost:8080

### Step B — Generate sample events

In this repo:

```bash
python3 data/raw_generator.py
```

This creates a file like:

- `data/input_events/payments_YYYYmmdd_HHMMSS.json`

### Step C — Publish files into BIGDATASTACK shared volumes

This repo includes a helper script `publish.sh` that copies:

- Spark jobs → `big-data-stack/volumenes/shared/scripts_airflow/fraud_pipeline/`
- Airflow DAGs → `big-data-stack/volumenes/shared/dags_airflow/`
- Input events → `big-data-stack/volumenes/shared/minio/data/bronze/payments/`

**Important:** `publish.sh` contains a hardcoded `BIGDATA_BASE`. Update it to your local path of `big-data-stack/volumenes`.

Then run:

```bash
chmod +x publish.sh
./publish.sh
```

After ~30–60 seconds the DAGs should appear in Airflow.

### Step D — Run DAGs in Airflow

Open Airflow (http://localhost:8090) and trigger in this order:

1. `payment_fraud_batch_pipeline` (Bronze → Silver)
2. `dag_silver_to_gold_mariadb_v1` (Silver → Gold + MariaDB)
3. `dag_train_model_from_gold_v1` (Train ML model)
4. `dag_score_and_write_mariadb_v1` (Score + MariaDB)

## 5) Where to see results

### MinIO (bucket `data`)

- **Silver**: `silver/payments_clean/`
- **Gold (rules)**: `gold/payments_scored/`
- **Model**: `models/fraud_lr_v1/`
- **Predictions**: `gold/predictions/`

### MariaDB

The DAGs write to MariaDB via Spark JDBC. By default, they target:

- Database: `bigdata_db`
- Tables:
  - `payments_gold`
  - `predictions`

You can browse them via **Adminer** at http://localhost:8089.

## 6) Notes / gotchas

- The DAGs currently use `--jdbc-host 172.28.0.10`. If your BIGDATASTACK uses a different IP, either:
  - update the DAGs, or
  - (recommended) switch to `--jdbc-host mariadb` (service name on the Docker network).
- `spark_jobs/job_main.py` reads from the *shared MinIO disk mount* (`/opt/minio/shareddata/...`). This is intentional for the demo and matches BIGDATASTACK volume mappings.
- If you get permission errors writing to MinIO paths, re-run `./publish.sh` (it applies `chmod/chown` to key folders).

## 7) Repository structure

```text
.
├── dags/
│   ├── fraud_detection_dag.py
│   ├── dag_silver_to_gold_mariadb.py
│   ├── dag_train_model_from_gold.py
│   └── dag_score_and_write_mariadb.py
├── data/
│   ├── raw_generator.py
│   └── input_events/               # generated files (not always committed)
├── spark_jobs/
│   ├── common/schemas.py
│   ├── job_main.py
│   ├── script_spark_silver_to_gold_mariadb.py
│   ├── script_spark_train_model_from_gold.py
│   └── script_spark_score_and_write_to_mariadb.py
├── sql/
│   └── init_db.sql
└── publish.sh
```

## 8) Next improvements (optional)

- Add true streaming ingestion (Kafka → Bronze) and watermarking.
- Add Great Expectations / data quality checks on Silver.
- Add feature store pattern + model registry versioning.
- Replace rule-only labeling with a proper labeled dataset or semi-supervised approach.

---

If you want, I can also propose small code tweaks to make the pipeline more production-like (config files, env vars, removing hardcoded credentials, using `mariadb` hostname, etc.).