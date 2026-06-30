# 🏗 Modern Data Lakehouse — dbt + Airflow + Snowflake

> **Masimbonge Portfolio**
> Data Engineering · ELT · Analytics

Production-grade ELT pipeline: live API data → AWS S3 → Snowflake → dbt transformations → Grafana/Metabase dashboards. Includes data quality checks, lineage graphs, and full test coverage.

---

## Architecture

```
Public APIs → Airflow DAG → AWS S3 (raw zone)
                                   ↓
                             Snowflake COPY INTO
                                   ↓
                         dbt (staging → marts)
                                   ↓
                   Great Expectations quality gate
                                   ↓
                         Metabase dashboards
```

---

## Folder Structure

```
project2-data-lakehouse/
├── dags/
│   └── main_elt_pipeline.py     # Airflow DAG — edit sources here
├── dbt/
│   ├── models/
│   │   ├── staging/             # Raw → cleaned views
│   │   │   ├── stg_weather.sql
│   │   │   └── schema.yml       # Column tests live here
│   │   └── marts/               # Clean analytics tables
│   │       └── mart_weather_daily.sql
│   └── tests/                   # Custom dbt tests
├── airflow/
│   └── docker-compose.yml       # Start Airflow locally
└── scripts/
    └── setup_snowflake.sql      # Create Snowflake objects
```

---

## Quick Start

### Prerequisites
- Docker Desktop
- Python 3.11+
- AWS account + CLI configured
- Snowflake free trial account

### Steps

```bash
# 1. Clone
git clone https://github.com/masimbonge/data-lakehouse
cd data-lakehouse

# 2. Copy and fill environment variables
cp .env.example .env
# Edit .env with your Snowflake + AWS credentials

# 3. Start Airflow
cd airflow
docker-compose up -d
# Open http://localhost:8080 — user: admin / pass: admin

# 4. Install dbt
pip install dbt-snowflake
cd ../dbt
dbt deps      # Install dbt packages
dbt debug     # Verify Snowflake connection

# 5. Trigger the pipeline in Airflow UI
# Enable DAG: main_elt_pipeline → trigger manually

# 6. After pipeline runs, test dbt models
dbt run && dbt test

# 7. Run data quality checks
great_expectations checkpoint run main_checkpoint
```

---

## Environment Variables (edit in .env)

| Variable | Description |
|---|---|
| `AWS_ACCESS_KEY_ID` | AWS key for S3 access |
| `AWS_SECRET_ACCESS_KEY` | AWS secret |
| `SNOWFLAKE_ACCOUNT` | e.g. `abc123.us-east-1` |
| `SNOWFLAKE_USER` | Your Snowflake username |
| `SNOWFLAKE_PASSWORD` | Your Snowflake password |
| `SNOWFLAKE_DATABASE` | e.g. `MASIMBONGE_DB` |
| `SNOWFLAKE_WAREHOUSE` | e.g. `COMPUTE_WH` |

---

## What you'll demonstrate
- Airflow DAG design and scheduling
- dbt modelling (staging → marts pattern)
- Snowflake COPY INTO and schema management
- Great Expectations data quality gates
- AWS S3 as a data lake raw zone
- Data lineage with dbt docs

---

*Built by Masimbonge*
