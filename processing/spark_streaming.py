"""
Stage 2 - Stream Processing
Apache Kafka -> PySpark Structured Streaming -> PostgreSQL + Parquet.

Reads crypto-trades topic, computes VWAP and volume metrics over tumbling
windows of 1 minute and 5 minutes.
"""

import logging
import os
import shutil
from datetime import datetime, timedelta
from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
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

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logging.getLogger("py4j").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-trades")
KAFKA_MAX_OFFSETS_PER_TRIGGER = int(os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "250000"))
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "480m")
SPARK_DRIVER_JAVA_OPTIONS = os.getenv(
    "SPARK_DRIVER_JAVA_OPTIONS",
    (
        "-XX:+UseSerialGC "
        "-XX:MaxMetaspaceSize=160m "
        "-XX:ReservedCodeCacheSize=48m "
        "-XX:MaxDirectMemorySize=64m "
        "-Xss512k"
    ),
)
PARQUET_OUTPUT = os.getenv("PARQUET_OUTPUT", "/tmp/crypto_raw")
AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT", "")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_DIR", os.getenv("SPARK_CHECKPOINT_ROOT", "/tmp/checkpoint"))
METRICS_CHECKPOINT_VERSION = os.getenv("SPARK_METRICS_CHECKPOINT_VERSION", "v5")
RESET_SPARK_STATE = os.getenv("RESET_SPARK_STATE", "false").lower() == "true"
METRIC_TABLES = frozenset({"trade_metrics_1min", "trade_metrics_5min"})

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


class ProgressLogger(StreamingQueryListener):
    """Log lifecycle and throughput for every Structured Streaming query."""

    def onQueryStarted(self, event: Any) -> None:
        """Log query startup with its stable runtime identifier."""
        log.info("Streaming query started: id=%s name=%s", event.id, event.name)

    def onQueryProgress(self, event: Any) -> None:
        """Log batch input volume and processing throughput."""
        progress = event.progress
        log.info(
            "Streaming progress: query=%s batch=%s rows=%s rate=%.1f/s",
            progress.name or progress.id,
            progress.batchId,
            progress.numInputRows,
            progress.processedRowsPerSecond,
        )

    def onQueryIdle(self, event: Any) -> None:
        """Ignore idle notifications to avoid noisy logs."""

    def onQueryTerminated(self, event: Any) -> None:
        """Log unexpected or graceful streaming query termination."""
        if event.exception:
            log.error(
                "Streaming query terminated: id=%s exception=%s",
                event.id,
                event.exception,
            )
        else:
            log.warning("Streaming query terminated: id=%s", event.id)


def is_azure_output(path: str) -> bool:
    """Return whether the configured Parquet output targets Azure Blob/ADLS."""
    return path.startswith(("wasbs://", "abfss://"))


def get_spark_packages() -> str:
    """Return Spark package coordinates required by the active sinks."""
    packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"]
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
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.driver.extraJavaOptions", SPARK_DRIVER_JAVA_OPTIONS)
        .config("spark.memory.fraction", "0.4")
        .config("spark.sql.shuffle.partitions", "2")
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
        .option("maxOffsetsPerTrigger", KAFKA_MAX_OFFSETS_PER_TRIGGER)
        # KNOWN: offset gaps are acceptable for this restartable portfolio demo.
        # Set failOnDataLoss=true for a production workload that must fail on missing offsets.
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


def get_postgres_conn() -> connection:
    """Create a PostgreSQL connection from environment configuration."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "crypto_pipeline"),
        user=os.getenv("POSTGRES_USER", "pipeline"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def write_to_postgres(batch_df: DataFrame, table: str) -> None:
    """Upsert finalized metrics and derive completed 5-minute windows from 1-minute rows."""
    if table not in METRIC_TABLES:
        raise ValueError(f"Unsupported metrics table: {table}")
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    upsert_sql = sql.SQL(
        """
        INSERT INTO {}
          (window_start, window_end, symbol, vwap, total_volume,
           trade_count, price_open, price_close, buy_volume,
           price_change_pct, window_minutes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (window_start, symbol)
        DO UPDATE SET
          window_end       = EXCLUDED.window_end,
          vwap             = EXCLUDED.vwap,
          total_volume     = EXCLUDED.total_volume,
          trade_count      = EXCLUDED.trade_count,
          price_open       = EXCLUDED.price_open,
          price_close      = EXCLUDED.price_close,
          buy_volume       = EXCLUDED.buy_volume,
          price_change_pct = EXCLUDED.price_change_pct,
          window_minutes   = EXCLUDED.window_minutes
        """
    ).format(sql.Identifier(table))
    values = [
        (
            row.window_start,
            row.window_end,
            row.symbol,
            row.vwap,
            row.total_volume,
            row.trade_count,
            row.price_open,
            row.price_close,
            row.buy_volume,
            row.price_change_pct,
            row.window_minutes,
        )
        for row in rows
    ]

    conn = get_postgres_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(upsert_sql, values)
            refreshed_rows = 0
            if table == "trade_metrics_1min":
                bucket_starts = {
                    row.window_start.replace(
                        minute=(row.window_start.minute // 5) * 5,
                        second=0,
                        microsecond=0,
                    )
                    for row in rows
                }
                bucket_starts.update(
                    bucket_start - timedelta(minutes=5)
                    for bucket_start in tuple(bucket_starts)
                )
                refreshed_rows = refresh_five_minute_windows(cur, bucket_starts)
        conn.commit()
        log.info("Upserted %d rows into %s", len(rows), table)
        if table == "trade_metrics_1min":
            log.info("Refreshed %d completed 5-minute rows", refreshed_rows)
    finally:
        conn.close()


def refresh_five_minute_windows(cur: Any, bucket_starts: set[datetime]) -> int:
    """Recompute complete 5-minute rows from deterministic 1-minute metrics."""
    refresh_sql = """
        INSERT INTO trade_metrics_5min
          (window_start, window_end, symbol, vwap, total_volume,
           trade_count, price_open, price_close, buy_volume,
           price_change_pct, window_minutes)
        SELECT
          %s::timestamptz AS window_start,
          %s::timestamptz + INTERVAL '5 minutes' AS window_end,
          symbol,
          SUM(vwap * total_volume) / NULLIF(SUM(total_volume), 0) AS vwap,
          SUM(total_volume) AS total_volume,
          SUM(trade_count) AS trade_count,
          (ARRAY_AGG(price_open ORDER BY window_start ASC))[1] AS price_open,
          (ARRAY_AGG(price_close ORDER BY window_start DESC))[1] AS price_close,
          SUM(buy_volume) AS buy_volume,
          (
            (
              (ARRAY_AGG(price_close ORDER BY window_start DESC))[1]
              - (ARRAY_AGG(price_open ORDER BY window_start ASC))[1]
            )
            / NULLIF((ARRAY_AGG(price_open ORDER BY window_start ASC))[1], 0)
            * 100
          )::double precision AS price_change_pct,
          '5 minutes' AS window_minutes
        FROM trade_metrics_1min
        WHERE window_start >= %s::timestamptz
          AND window_start < %s::timestamptz + INTERVAL '5 minutes'
          AND NOW() >= %s::timestamptz + INTERVAL '5 minutes 10 seconds'
        GROUP BY symbol
        ON CONFLICT (window_start, symbol)
        DO UPDATE SET
          window_end       = EXCLUDED.window_end,
          vwap             = EXCLUDED.vwap,
          total_volume     = EXCLUDED.total_volume,
          trade_count      = EXCLUDED.trade_count,
          price_open       = EXCLUDED.price_open,
          price_close      = EXCLUDED.price_close,
          buy_volume       = EXCLUDED.buy_volume,
          price_change_pct = EXCLUDED.price_change_pct,
          window_minutes   = EXCLUDED.window_minutes
    """
    refreshed_rows = 0
    for bucket_start in sorted(bucket_starts):
        cur.execute(
            refresh_sql,
            (
                bucket_start,
                bucket_start,
                bucket_start,
                bucket_start,
                bucket_start,
            ),
        )
        refreshed_rows += cur.rowcount
    return refreshed_rows


def main() -> None:
    """Start one stateless Parquet query and one stateful metrics query."""
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressLogger())

    raw = read_kafka_stream(spark)
    events = parse_events(raw)

    metrics_1min = compute_window_metrics(events, "1 minute")
    query_1min = (
        metrics_1min.writeStream.foreachBatch(
            lambda df, _bid: write_to_postgres(df, "trade_metrics_1min")
        )
        .queryName("metrics_1min")
        # v5 isolates the memory-tuned Spark config from any malformed legacy checkpoint logs.
        .option(
            "checkpointLocation",
            f"{CHECKPOINT_ROOT}/1min-{METRICS_CHECKPOINT_VERSION}",
        )
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Start the stateful query first so its checkpoint captures two shuffle partitions.
    # The legacy raw checkpoint records four partitions but has no aggregation state.
    raw_query = (
        events.writeStream.format("parquet")
        .queryName("raw_parquet")
        .option("path", PARQUET_OUTPUT)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/raw-{METRICS_CHECKPOINT_VERSION}")
        .partitionBy("symbol")
        .trigger(processingTime="30 seconds")
        .start()
    )

    del raw_query, query_1min
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

# To verify after deploy:
# docker stats spark --no-stream  -> target under 85%; investigate sustained excess
# docker inspect spark --format '{{.RestartCount}}'  -> must be 0
# docker compose logs spark | grep -i "error\|exception"  -> must be empty
