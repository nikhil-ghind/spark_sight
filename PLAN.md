# Spark Sight

## Project Overview
Apache Spark for distributed batch transformation across 100M+ events, storing aggregated results in ClickHouse columnar OLAP database for sub-second analytical queries. React dashboard with SQL-backed filters and drill-downs.

## Tech Stack
- **Batch Processing:** Apache Spark 3.5+ (PySpark)
- **OLAP Database:** ClickHouse
- **Frontend:** React 18, TypeScript, Recharts
- **API:** FastAPI (query proxy)
- **Data Format:** Parquet (intermediate), ClickHouse MergeTree (final)
- **Orchestration:** Airflow or cron
- **Container:** Docker, Docker Compose

## Architecture Overview
```
┌──────────────┐  Parquet  ┌──────────────┐  Insert  ┌────────────┐
│ Raw Events   │ ────────► │ Apache Spark │ ───────► │ ClickHouse │
│ (S3/local)   │           │ (transform)  │          │ (OLAP)     │
└──────────────┘           └──────────────┘          └─────┬──────┘
                                                           │ SQL
                                                    ┌──────▼──────┐
                                                    │  FastAPI    │
                                                    │  (proxy)    │
                                                    └──────┬──────┘
                                                           │
                                                    ┌──────▼──────┐
                                                    │  React      │
                                                    │  Dashboard  │
                                                    └─────────────┘
```

## Phase 1: Data Generation & Spark Processing
**Goal:** Generate sample events and build Spark transformation pipeline.

### Tasks
1. Project structure:
   ```
   realtimeAnalyticsPipeline/
   ├── spark/
   │   ├── jobs/
   │   │   ├── transform_events.py    # Main Spark job
   │   │   ├── aggregate_daily.py     # Daily aggregation
   │   │   └── aggregate_hourly.py    # Hourly aggregation
   │   ├── utils/
   │   │   ├── schemas.py             # Spark schemas
   │   │   └── transformations.py     # Reusable transforms
   │   └── data_generator.py          # Generate sample events
   ├── clickhouse/
   │   ├── schema.sql                 # Table definitions
   │   └── queries/                   # Analytical query templates
   ├── api/
   │   ├── main.py                    # FastAPI proxy
   │   └── routes/
   │       ├── analytics.py
   │       └── drilldown.py
   ├── frontend/
   │   └── ...
   ├── docker-compose.yml
   └── Makefile
   ```
2. `spark/data_generator.py`:
   - Generate 100M+ events as Parquet files:
     - Event types: page_view, click, purchase, signup, search
     - Fields: event_id, user_id, event_type, timestamp, page_url, product_id, amount_cents, country, device, session_id
     - Realistic distributions: 80% page_views, 15% clicks, 3% purchases, etc.
   - Output: `data/raw/events_YYYYMMDD.parquet` (partitioned by date)
3. `spark/jobs/transform_events.py`:
   - Read raw Parquet files
   - Clean: deduplicate by event_id, filter invalid records, parse timestamps
   - Enrich: add derived fields (hour_of_day, day_of_week, is_weekend)
   - Sessionize: group events by user_id within 30-min windows
   - Write cleaned data as Parquet partitioned by date + event_type
4. `spark/jobs/aggregate_daily.py`:
   - Group by: date, event_type, country, device
   - Metrics: event_count, unique_users, total_revenue, avg_session_duration
   - Write to ClickHouse via JDBC or clickhouse-connect

## Phase 2: ClickHouse Schema & Queries
**Goal:** Design ClickHouse tables and write analytical queries.

### Tasks
1. `clickhouse/schema.sql`:
   ```sql
   CREATE TABLE events (
       event_id String, user_id String, event_type LowCardinality(String),
       timestamp DateTime64(3), page_url String, product_id Nullable(String),
       amount_cents Nullable(Int64), country LowCardinality(String),
       device LowCardinality(String), session_id String,
       hour_of_day UInt8, day_of_week UInt8
   ) ENGINE = MergeTree()
   PARTITION BY toYYYYMM(timestamp)
   ORDER BY (event_type, timestamp, user_id);

   CREATE TABLE daily_aggregates (
       date Date, event_type LowCardinality(String),
       country LowCardinality(String), device LowCardinality(String),
       event_count UInt64, unique_users UInt64,
       total_revenue_cents Int64, avg_session_seconds Float64
   ) ENGINE = SummingMergeTree()
   ORDER BY (date, event_type, country, device);

   CREATE MATERIALIZED VIEW hourly_events_mv TO hourly_events AS
   SELECT toStartOfHour(timestamp) as hour, event_type,
          count() as event_count, uniqExact(user_id) as unique_users
   FROM events GROUP BY hour, event_type;
   ```
2. `clickhouse/queries/`:
   - `funnel.sql` — conversion funnel: page_view → click → purchase rates
   - `retention.sql` — user retention cohort analysis
   - `top_pages.sql` — top pages by unique visitors with trend
   - `revenue_by_country.sql` — revenue breakdown with period comparison
3. Verify: insert sample data, run queries, validate sub-second response.

## Phase 3: FastAPI Query Proxy
**Goal:** REST API that translates dashboard requests to ClickHouse SQL.

### Tasks
1. `api/routes/analytics.py`:
   - `GET /api/analytics/overview?start=&end=` — total events, users, revenue, conversion rate
   - `GET /api/analytics/timeseries?metric=&granularity=&start=&end=` — time-bucketed metric (hourly/daily)
   - `GET /api/analytics/breakdown?dimension=&metric=&start=&end=` — group by dimension (country, device, event_type)
   - `GET /api/analytics/funnel?start=&end=` — conversion funnel stages
2. `api/routes/drilldown.py`:
   - `GET /api/drilldown/events?filters=` — raw event table with server-side pagination
   - `GET /api/drilldown/users/{user_id}` — user event timeline
3. Query builder: translate API params to parameterized ClickHouse SQL (prevent injection).
4. Caching: Redis cache with 1-min TTL for frequent queries.

## Phase 4: React Dashboard
**Goal:** Interactive analytics dashboard with filters and drill-downs.

### Tasks
1. Frontend pages:
   - `Dashboard.tsx` — KPI cards (total events, users, revenue, conversion), main timeseries chart
   - `Breakdown.tsx` — dimension selector (country/device/event_type), bar/pie charts
   - `Funnel.tsx` — conversion funnel visualization
   - `Explorer.tsx` — raw event table with column filters, pagination, export CSV
2. Components:
   - `TimeSeriesChart.tsx` — Recharts line chart with granularity toggle (hour/day/week)
   - `DateRangePicker.tsx` — preset ranges (today, 7d, 30d, custom)
   - `FilterBar.tsx` — country, device, event_type multi-select filters
   - `KPICard.tsx` — metric with period-over-period comparison
3. All charts respond to global date range and filter selections.
4. Docker Compose: Spark (standalone), ClickHouse, FastAPI, React (nginx), Redis.
