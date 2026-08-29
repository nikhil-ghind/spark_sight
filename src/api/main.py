"""FastAPI backend that proxies ClickHouse queries for the React dashboard."""
import os
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import clickhouse_connect

app = FastAPI(title="spark_sight")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def ch():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DB", "spark_sight"),
    )


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/hourly-counts")
def hourly_counts(event_type: str = None, limit: int = Query(default=168, le=1000)):
    client = ch()
    where = f"WHERE event_type = '{event_type}'" if event_type else ""
    rows = client.query(
        f"SELECT hour, event_type, event_count FROM hourly_event_counts {where} ORDER BY hour DESC LIMIT {limit}"
    ).result_rows
    return [{"hour": str(r[0]), "event_type": r[1], "event_count": r[2]} for r in rows]


@app.get("/api/daily-revenue")
def daily_revenue(days: int = Query(default=30, le=365)):
    client = ch()
    rows = client.query(
        f"SELECT date, total_revenue, transaction_count, avg_revenue "
        f"FROM daily_revenue ORDER BY date DESC LIMIT {days}"
    ).result_rows
    return [{"date": str(r[0]), "total_revenue": r[1], "transaction_count": r[2], "avg_revenue": r[3]} for r in rows]


@app.get("/api/retention")
def retention():
    client = ch()
    rows = client.query(
        "SELECT days_since_first, active_users FROM user_retention ORDER BY days_since_first ASC LIMIT 90"
    ).result_rows
    return [{"day": r[0], "active_users": r[1]} for r in rows]
