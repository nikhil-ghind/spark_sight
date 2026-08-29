import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from src.jobs.event_transforms import hourly_event_counts, daily_revenue, user_retention, EVENT_SCHEMA


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def make_df(spark, rows):
    return spark.createDataFrame(rows, schema=EVENT_SCHEMA)


def test_hourly_event_counts(spark):
    rows = [
        ("e1", "u1", "click", None, datetime(2024, 1, 1, 10, 5), None),
        ("e2", "u2", "click", None, datetime(2024, 1, 1, 10, 30), None),
        ("e3", "u1", "view",  None, datetime(2024, 1, 1, 11, 0),  None),
    ]
    df = make_df(spark, rows)
    result = hourly_event_counts(df).collect()
    counts = {(str(r.hour), r.event_type): r.event_count for r in result}
    assert counts[("2024-01-01 10:00:00", "click")] == 2
    assert counts[("2024-01-01 11:00:00", "view")] == 1


def test_daily_revenue(spark):
    rows = [
        ("e1", "u1", "purchase", None, datetime(2024, 1, 1, 10), 100.0),
        ("e2", "u2", "purchase", None, datetime(2024, 1, 1, 15), 50.0),
        ("e3", "u1", "click",    None, datetime(2024, 1, 1, 12), None),
    ]
    df = make_df(spark, rows)
    result = daily_revenue(df).collect()
    assert len(result) == 1
    assert result[0].total_revenue == pytest.approx(150.0)
    assert result[0].transaction_count == 2


def test_user_retention(spark):
    rows = [
        ("e1", "u1", "view", None, datetime(2024, 1, 1), None),
        ("e2", "u1", "view", None, datetime(2024, 1, 3), None),
        ("e3", "u2", "view", None, datetime(2024, 1, 5), None),
        ("e4", "u2", "view", None, datetime(2024, 1, 5), None),
    ]
    df = make_df(spark, rows)
    result = {r.days_since_first: r.active_users for r in user_retention(df).collect()}
    assert result[0] == 2   # both users on their first day
    assert result[2] == 1   # u1 on day 2
