CREATE DATABASE IF NOT EXISTS spark_sight;

CREATE TABLE IF NOT EXISTS spark_sight.hourly_event_counts (
    hour        DateTime,
    event_type  String,
    event_count UInt64
) ENGINE = MergeTree()
ORDER BY (hour, event_type);

CREATE TABLE IF NOT EXISTS spark_sight.daily_revenue (
    date              Date,
    total_revenue     Float64,
    transaction_count UInt64,
    avg_revenue       Float64
) ENGINE = MergeTree()
ORDER BY date;

CREATE TABLE IF NOT EXISTS spark_sight.user_retention (
    days_since_first Int32,
    active_users     UInt64
) ENGINE = MergeTree()
ORDER BY days_since_first;
