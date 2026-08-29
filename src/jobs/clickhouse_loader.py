"""Load Parquet aggregates into ClickHouse tables."""
import os
import clickhouse_connect
from pyspark.sql import SparkSession, DataFrame


def get_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DB", "spark_sight"),
    )


def load_parquet_to_clickhouse(spark: SparkSession, parquet_path: str, table: str):
    df = spark.read.parquet(parquet_path)
    rows = [list(row) for row in df.collect()]
    columns = df.columns
    client = get_client()
    client.insert(table, rows, column_names=columns)
    print(f"Loaded {len(rows)} rows into {table}")


def load_all(spark: SparkSession, output_dir: str):
    load_parquet_to_clickhouse(spark, f"{output_dir}/hourly_counts",  "hourly_event_counts")
    load_parquet_to_clickhouse(spark, f"{output_dir}/daily_revenue",  "daily_revenue")
    load_parquet_to_clickhouse(spark, f"{output_dir}/user_retention", "user_retention")
