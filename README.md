# spark_sight

PySpark batch analytics pipeline over 100M+ events. Computes hourly counts, daily revenue, and user retention cohorts, loads results into ClickHouse OLAP, and serves a React + Recharts dashboard.

## Architecture

```mermaid
flowchart TB
    raw[("Raw event Parquet<br/>event_id, user_id, event_type,<br/>properties JSON, ts, revenue")]

    subgraph spark["PySpark batch job — src/jobs/event_transforms.py"]
        rd["read_events<br/>explicit EVENT_SCHEMA, no inference"]
        h["hourly_event_counts<br/>date_trunc hour, group by hour + event_type"]
        d["daily_revenue<br/>drop null revenue, group by date,<br/>sum / count / avg"]
        r["user_retention<br/>min ts per user, join back,<br/>datediff to days_since_first,<br/>countDistinct users"]
    end

    stage[("Staging Parquet<br/>output_dir/hourly_counts<br/>output_dir/daily_revenue<br/>output_dir/user_retention")]

    loader["clickhouse_loader.load_all<br/>reads each staging path,<br/>collects rows, client.insert per table"]

    subgraph ch["ClickHouse — sql/schema.sql"]
        t1[("hourly_event_counts<br/>MergeTree order by hour, event_type")]
        t2[("daily_revenue<br/>MergeTree order by date")]
        t3[("user_retention<br/>MergeTree order by days_since_first")]
    end

    api["FastAPI — src/api/main.py<br/>GET /health<br/>GET /api/hourly-counts<br/>GET /api/daily-revenue<br/>GET /api/retention"]
    ui["Vite + React dashboard<br/>Recharts LineChart and BarChart"]

    raw --> rd
    rd --> h
    rd --> d
    rd --> r
    h --> stage
    d --> stage
    r --> stage
    stage --> loader
    loader --> t1
    loader --> t2
    loader --> t3
    t1 --> api
    t2 --> api
    t3 --> api
    api -->|"JSON over HTTP"| ui
```

The three aggregate tables the whole pipeline exists to produce:

```mermaid
erDiagram
    HOURLY_EVENT_COUNTS {
        DateTime hour
        String event_type
        UInt64 event_count
    }
    DAILY_REVENUE {
        Date date
        Float64 total_revenue
        UInt64 transaction_count
        Float64 avg_revenue
    }
    USER_RETENTION {
        Int32 days_since_first
        UInt64 active_users
    }
```

## Components

| Component | Description |
|-----------|-------------|
| `src/jobs/event_transforms.py` | PySpark jobs: hourly counts, daily revenue, user retention |
| `src/jobs/clickhouse_loader.py` | Reads staging Parquet, inserts into ClickHouse |
| `src/api/main.py` | FastAPI endpoints proxying ClickHouse queries |
| `frontend/` | Vite + React dashboard with Recharts line/bar charts |
| `sql/schema.sql` | ClickHouse DDL for all three tables |

## Setup

### Python backend
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # set CLICKHOUSE_HOST etc.
```

### Run Spark job
```bash
spark-submit src/jobs/event_transforms.py \
  s3://my-bucket/events/2024/ \
  /tmp/spark_sight_output

python -c "
from pyspark.sql import SparkSession
from src.jobs.clickhouse_loader import load_all
spark = SparkSession.builder.getOrCreate()
load_all(spark, '/tmp/spark_sight_output')
"
```

### Start API
```bash
uvicorn src.api.main:app --reload
```

### Start frontend
```bash
cd frontend && npm install && npm run dev
```

## ClickHouse schema

Run `sql/schema.sql` against your ClickHouse instance:
```bash
clickhouse-client --multiquery < sql/schema.sql
```

## API endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/hourly-counts?event_type=&limit=168` | Hourly event counts |
| `GET /api/daily-revenue?days=30` | Daily revenue aggregates |
| `GET /api/retention` | User retention day-N curve |
| `GET /health` | Health check |

## Tests

```bash
pytest tests/ -v  # requires local Spark (master=local[1])
```

## Environment variables

| Variable | Default |
|----------|---------|
| `CLICKHOUSE_HOST` | `localhost` |
| `CLICKHOUSE_PORT` | `8123` |
| `CLICKHOUSE_USER` | `default` |
| `CLICKHOUSE_PASSWORD` | `` |
| `CLICKHOUSE_DB` | `spark_sight` |
