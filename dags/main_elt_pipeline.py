"""
Modern Data Lakehouse — Main Airflow DAG
Masimbonge Portfolio
Edit: sources, schedule, and transformations below
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests
import json
import logging

log = logging.getLogger(__name__)

# ── DAG config — edit these ───────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "masimbonge",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,      # ← Set True + add your email for alerts
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

S3_BUCKET   = "masimbonge-lakehouse-raw"   # ← Edit: your S3 bucket name
S3_PREFIX   = "raw"
SNOWFLAKE_CONN = "snowflake_default"       # ← Edit: Airflow connection name

# ── Data sources — add/remove as needed ──────────────────────────────────────
DATA_SOURCES = {
    "weather": "https://api.open-meteo.com/v1/forecast?latitude=-26&longitude=28&hourly=temperature_2m",
    # "finance": "https://api.example.com/finance",   # ← Add more sources
    # "transport": "https://api.example.com/transport",
}


def extract_api_data(source_name: str, url: str, **context):
    """Fetch data from API and upload raw JSON to S3."""
    log.info(f"Extracting: {source_name}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    date_str = context["ds"]
    s3_key = f"{S3_PREFIX}/{source_name}/{date_str}/data.json"

    s3 = S3Hook(aws_conn_id="aws_default")
    s3.load_string(
        string_data=json.dumps(data),
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=True,
    )
    log.info(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")
    return s3_key


def load_to_snowflake_stage(source_name: str, **context):
    """Copy raw S3 data into Snowflake staging table."""
    date_str = context["ds"]
    s3_key = f"{S3_PREFIX}/{source_name}/{date_str}/data.json"

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
    hook.run(f"""
        COPY INTO RAW.{source_name.upper()}_STAGE
        FROM @my_s3_stage/{s3_key}
        FILE_FORMAT = (TYPE = JSON)
        ON_ERROR = CONTINUE;
    """)
    log.info(f"Loaded {source_name} into Snowflake staging")


def run_data_quality(**context):
    """Run Great Expectations checkpoints after load."""
    import subprocess
    result = subprocess.run(
        ["great_expectations", "checkpoint", "run", "main_checkpoint"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise ValueError(f"Data quality check failed:\n{result.stdout}")
    log.info("Data quality checks passed ✓")


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    "main_elt_pipeline",
    default_args=DEFAULT_ARGS,
    description="ELT pipeline: APIs → S3 → Snowflake → dbt",
    schedule_interval="0 6 * * *",   # ← Edit: runs daily at 6am UTC
    catchup=False,
    tags=["masimbonge", "elt", "lakehouse"],
) as dag:

    # Extract tasks — one per source
    extract_tasks = []
    load_tasks = []

    for name, url in DATA_SOURCES.items():
        extract = PythonOperator(
            task_id=f"extract_{name}",
            python_callable=extract_api_data,
            op_kwargs={"source_name": name, "url": url},
        )
        load = PythonOperator(
            task_id=f"load_{name}_to_snowflake",
            python_callable=load_to_snowflake_stage,
            op_kwargs={"source_name": name},
        )
        extract >> load
        extract_tasks.append(extract)
        load_tasks.append(load)

    # dbt run — transforms raw → clean models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . --target prod",
    )

    # dbt test
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir . --target prod",
    )

    # Data quality
    quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_data_quality,
    )

    # Pipeline order: all loads → dbt run → dbt test → quality
    for load in load_tasks:
        load >> dbt_run
    dbt_run >> dbt_test >> quality_check
