"""
Stage 2 - Stream Processing
Apache Kafka -> PySpark Structured Streaming -> PostgreSQL + Parquet.

Reads crypto-trades topic, computes VWAP and volume metrics over tumbling
windows of 1 minute and 5 minutes.
"""

import os
import shutil

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

if os.name == "nt":
    os.environ.setdefault("JAVA_HOME", r"C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot")
    hadoop_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".hadoop"))
    os.environ.setdefault("HADOOP_HOME", hadoop_path)
    os.environ["PATH"] = os.path.join(hadoop_path, "bin") + os.pathsep + os.environ.get("PATH", "")

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-trades")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_pipeline")
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPS = {
    "user": os.getenv("POSTGRES_USER", "pipeline"),
    "password": os.getenv("POSTGRES_PASSWORD", "changeme"),
    "driver": "org.postgresql.Driver",
}
PARQUET_OUTPUT = os.getenv("PARQUET_OUTPUT", "/tmp/crypto_raw")
AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT", "")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_DIR", os.getenv("SPARK_CHECKPOINT_ROOT", "/tmp/checkpoint"))
RESET_SPARK_STATE = os.getenv("RESET_SPARK_STATE", "false").lower() == "true"

TRADE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("trade_time", LongType(), True),
        StructField("trade_time_iso", StringType(), True),
        StructField("is_buyer_maker", BooleanType(), True),
        StructField("trade_id", LongType(), True),
    ]
)


def is_azure_output(path: str) -> bool:
    """Return whether the configured Parquet output targets Azure Blob/ADLS."""
    return path.startswith(("wasbs://", "abfss://"))


def get_spark_packages() -> str:
    """Return Spark package coordinates required by the active sinks."""
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.postgresql:postgresql:42.7.3",
    ]
    if is_azure_output(PARQUET_OUTPUT):
        packages.append("org.apache.hadoop:hadoop-azure:3.3.4")
    return ",".join(packages)


def apply_azure_storage_config(builder):
    """Configure Azure Blob/ADLS access through VM Managed Identity when requested."""
    if not is_azure_output(PARQUET_OUTPUT) or not AZURE_STORAGE_ACCOUNT:
        return builder

    for suffix in ("blob.core.windows.net", "dfs.core.windows.net"):
        account_host = f"{AZURE_STORAGE_ACCOUNT}.{suffix}"
        builder = (
            builder.config(f"spark.hadoop.fs.azure.account.auth.type.{account_host}", "OAuth")
            .config(
                f"spark.hadoop.fs.azure.account.oauth.provider.type.{account_host}",
                "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider",
            )
        )
    return builder


def create_spark() -> SparkSession:
    """Create a Spark session configured for local or Azure Parquet output."""
    if RESET_SPARK_STATE and not is_azure_output(PARQUET_OUTPUT):
        shutil.rmtree(CHECKPOINT_ROOT, ignore_errors=True)
        shutil.rmtree(PARQUET_OUTPUT, ignore_errors=True)
        shutil.rmtree(r"C:\tmp\checkpoint", ignore_errors=True)
        shutil.rmtree(r"C:\tmp\crypto_raw", ignore_errors=True)

    builder = (
        SparkSession.builder.appName("CryptoStreamingPipeline")
        .master("local[2]")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", get_spark_packages())
    )

    if os.name == "nt":
        builder = builder.config("spark.driver.host", "127.0.0.1").config(
            "spark.driver.bindAddress",
            "127.0.0.1",
        )

    return apply_azure_storage_config(builder).getOrCreate()


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Read raw Kafka messages from the configured crypto topic."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw_stream: DataFrame) -> DataFrame:
    """Deserialize and validate Kafka JSON bytes into typed trade events."""
    return (
        raw_stream.select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_ts", (F.col("trade_time") / 1000).cast("timestamp"))
        .filter(
            F.col("price").isNotNull()
            & (F.col("price") > 0)
            & F.col("quantity").isNotNull()
            & (F.col("quantity") > 0)
            & F.col("symbol").isNotNull()
            & F.col("event_ts").isNotNull()
            & F.col("trade_id").isNotNull()
        )
    )


def compute_window_metrics(
    events: DataFrame,
    window_duration: str,
    slide_duration: str | None = None,
) -> DataFrame:
    """Compute VWAP, volume, trade count and price change by symbol/window."""
    if slide_duration:
        window_col = F.window("event_ts", window_duration, slide_duration)
    else:
        window_col = F.window("event_ts", window_duration)

    return (
        events.withWatermark("event_ts", "10 seconds")
        .groupBy("symbol", window_col)
        .agg(
            (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
            F.sum("quantity").alias("total_volume"),
            F.count("*").alias("trade_count"),
            F.min_by("price", F.struct("event_ts", "trade_id")).alias("price_open"),
            F.max_by("price", F.struct("event_ts", "trade_id")).alias("price_close"),
            F.sum(
                F.when(~F.col("is_buyer_maker"), F.col("quantity")).otherwise(0)
            ).alias("buy_volume"),
        )
        .withColumn(
            "price_change_pct",
            (
                (F.col("price_close") - F.col("price_open"))
                / F.col("price_open")
                * 100
            ).cast("double"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("window_minutes", F.lit(window_duration))
        .drop("window")
    )


def write_to_postgres(batch_df: DataFrame, batch_id: int, table: str) -> None:
    """Append a non-empty streaming micro-batch to PostgreSQL."""
    del batch_id
    if batch_df.isEmpty():
        return
    batch_df.write.jdbc(url=POSTGRES_URL, table=table, mode="append", properties=POSTGRES_PROPS)


def main() -> None:
    """Start the streaming queries for raw Parquet and aggregated metrics."""
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_kafka_stream(spark)
    events = parse_events(raw)

    raw_query = (
        events.writeStream.format("parquet")
        .option("path", PARQUET_OUTPUT)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/raw")
        .partitionBy("symbol")
        .trigger(processingTime="30 seconds")
        .start()
    )

    metrics_1min = compute_window_metrics(events, "1 minute")
    query_1min = (
        metrics_1min.writeStream.foreachBatch(
            lambda df, bid: write_to_postgres(df, bid, "trade_metrics_1min")
        )
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/1min")
        .trigger(processingTime="30 seconds")
        .start()
    )

    metrics_5min = compute_window_metrics(events, "5 minutes")
    query_5min = (
        metrics_5min.writeStream.foreachBatch(
            lambda df, bid: write_to_postgres(df, bid, "trade_metrics_5min")
        )
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/5min")
        .trigger(processingTime="60 seconds")
        .start()
    )

    del raw_query, query_1min, query_5min
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
