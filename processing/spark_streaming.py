"""
Stage 2 — Stream Processing
Apache Kafka → PySpark Structured Streaming → PostgreSQL + Parquet

Reads crypto-trades topic, computes VWAP and volume metrics
over tumbling windows of 1 min and 5 min.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, BooleanType
)

os.environ["JAVA_HOME"] = r"C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot"
hadoop_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".hadoop"))
os.environ["HADOOP_HOME"] = hadoop_path
os.environ["PATH"] = os.path.join(hadoop_path, "bin") + os.pathsep + os.environ.get("PATH", "")

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC", "crypto-trades")
POSTGRES_URL     = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST','localhost')}:5433/{os.getenv('POSTGRES_DB','crypto_pipeline')}"
POSTGRES_PROPS   = {"user": os.getenv("POSTGRES_USER","pipeline"), "password": os.getenv("POSTGRES_PASSWORD","changeme"), "driver": "org.postgresql.Driver"}
PARQUET_OUTPUT   = os.getenv("PARQUET_OUTPUT", "/tmp/crypto_raw")

# ── Schema ────────────────────────────────────────────────────────────────────
TRADE_SCHEMA = StructType([
    StructField("symbol",         StringType(),  True),
    StructField("price",          DoubleType(),  True),
    StructField("quantity",       DoubleType(),  True),
    StructField("trade_time",     LongType(),    True),
    StructField("trade_time_iso", StringType(),  True),
    StructField("is_buyer_maker", BooleanType(), True),
    StructField("trade_id",       LongType(),    True),
])


import shutil

def create_spark() -> SparkSession:
    # Clear local checkpoints for fresh start to avoid corruption errors
    shutil.rmtree("/tmp/checkpoint", ignore_errors=True)
    shutil.rmtree("/tmp/crypto_raw", ignore_errors=True)
    shutil.rmtree(r"C:\tmp\checkpoint", ignore_errors=True)
    shutil.rmtree(r"C:\tmp\crypto_raw", ignore_errors=True)

    return (
        SparkSession.builder
        .appName("CryptoStreamingPipeline")
        .master("local[2]")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.7.3"
        )
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw_stream):
    """Deserialize JSON bytes → typed DataFrame."""
    return (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_ts", (F.col("trade_time") / 1000).cast("timestamp"))
    )


def compute_window_metrics(events, window_duration: str, slide_duration: str = None):
    """
    Compute VWAP, volume, trade count per symbol per window.
    VWAP = sum(price * quantity) / sum(quantity)
    """
    w = F.window("event_ts", window_duration, slide_duration) if slide_duration else F.window("event_ts", window_duration)

    return (
        events
        .withWatermark("event_ts", "10 seconds")
        .groupBy("symbol", w)
        .agg(
            (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
            F.sum("quantity").alias("total_volume"),
            F.count("*").alias("trade_count"),
            F.first("price").alias("price_open"),
            F.last("price").alias("price_close"),
            F.sum(F.when(~F.col("is_buyer_maker"), F.col("quantity")).otherwise(0)).alias("buy_volume"),
        )
        .withColumn("price_change_pct",
            ((F.col("price_close") - F.col("price_open")) / F.col("price_open") * 100).cast("double")
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .withColumn("window_minutes", F.lit(window_duration))
        .drop("window")
    )


def write_to_postgres(batch_df, batch_id: int, table: str) -> None:
    if batch_df.isEmpty():
        return
    batch_df.write.jdbc(url=POSTGRES_URL, table=table, mode="append", properties=POSTGRES_PROPS)


def main() -> None:
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw     = read_kafka_stream(spark)
    events  = parse_events(raw)

    # ── Sink 1: Raw events → Parquet ─────────────────────────────────────────
    raw_query = (
        events.writeStream
        .format("parquet")
        .option("path", PARQUET_OUTPUT)
        .option("checkpointLocation", "/tmp/checkpoint/raw")
        .partitionBy("symbol")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ── Sink 2: 1-min window → PostgreSQL ────────────────────────────────────
    metrics_1min = compute_window_metrics(events, "1 minute")
    query_1min = (
        metrics_1min.writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "trade_metrics_1min"))
        .option("checkpointLocation", "/tmp/checkpoint/1min")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ── Sink 3: 5-min window → PostgreSQL ────────────────────────────────────
    metrics_5min = compute_window_metrics(events, "5 minutes")
    query_5min = (
        metrics_5min.writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "trade_metrics_5min"))
        .option("checkpointLocation", "/tmp/checkpoint/5min")
        .trigger(processingTime="60 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
