"""PySpark batch transforms over raw events → ClickHouse-ready aggregates."""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType


EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),    False),
    StructField("user_id",     StringType(),    False),
    StructField("event_type",  StringType(),    False),
    StructField("properties",  StringType(),    True),   # JSON string
    StructField("ts",          TimestampType(), False),
    StructField("revenue",     DoubleType(),    True),
])


def read_events(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(EVENT_SCHEMA).parquet(path)


def hourly_event_counts(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("hour", F.date_trunc("hour", F.col("ts")))
          .groupBy("hour", "event_type")
          .agg(F.count("*").alias("event_count"))
          .orderBy("hour", "event_type")
    )


def daily_revenue(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("revenue").isNotNull())
          .withColumn("date", F.to_date("ts"))
          .groupBy("date")
          .agg(
              F.sum("revenue").alias("total_revenue"),
              F.count("*").alias("transaction_count"),
              F.avg("revenue").alias("avg_revenue"),
          )
          .orderBy("date")
    )


def user_retention(df: DataFrame) -> DataFrame:
    """Days-since-first-event distribution per user (cohort day-0 → day-N)."""
    first_seen = (
        df.groupBy("user_id")
          .agg(F.min("ts").alias("first_seen"))
    )
    return (
        df.join(first_seen, "user_id")
          .withColumn("days_since_first", F.datediff(F.to_date("ts"), F.to_date("first_seen")))
          .groupBy("days_since_first")
          .agg(F.countDistinct("user_id").alias("active_users"))
          .orderBy("days_since_first")
    )


def run(spark: SparkSession, input_path: str, output_dir: str):
    df = read_events(spark, input_path)
    df.cache()

    hourly_event_counts(df).write.mode("overwrite").parquet(f"{output_dir}/hourly_counts")
    daily_revenue(df).write.mode("overwrite").parquet(f"{output_dir}/daily_revenue")
    user_retention(df).write.mode("overwrite").parquet(f"{output_dir}/user_retention")

    df.unpersist()


if __name__ == "__main__":
    import sys
    spark = SparkSession.builder.appName("spark_sight").getOrCreate()
    run(spark, sys.argv[1], sys.argv[2])
    spark.stop()
