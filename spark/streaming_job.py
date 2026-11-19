"""Spark Structured Streaming job.

Kafka (crypto.prices) -> validate (each rejection carries a reason)
  |-- valid ticks   -> S3 lake Parquet  (raw data lake, partitioned by date)
  |-- valid ticks   -> 1-minute tumbling window per symbol -> PostgreSQL raw.price_windows
  |-- invalid msgs  -> S3 dead_letter/ with cause + Kafka coordinates (never silently dropped)
  |-- failed sink   -> S3 dead_letter/sink_failures/ (quarantine before retrying)
"""
import logging
import os

from pyspark.sql import SparkSession, functions as F, types as T

log = logging.getLogger("spark.streaming")

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto.prices")
BUCKET = os.getenv("S3_BUCKET", "datalake")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://lake:8333")
CKPT = "/checkpoints"
TRIGGER = "30 seconds"
WATERMARK = "1 minute"

JDBC_URL = "jdbc:postgresql://{h}:{p}/{d}".format(
    h=os.getenv("WH_HOST", "warehouse"), p=os.getenv("WH_PORT", "5432"), d=os.getenv("WH_DB", "warehouse"))
JDBC_PROPS = {"user": os.getenv("WH_USER", "warehouse"),
              "password": os.getenv("WH_PASSWORD", "warehouse"),
              "driver": "org.postgresql.Driver"}

SCHEMA = T.StructType([
    T.StructField("symbol", T.StringType()),
    T.StructField("price", T.DoubleType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("source", T.StringType()),
])


def build_spark() -> SparkSession:
    return (SparkSession.builder.appName("crypto-streaming")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY", "lakeadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY", "lakeadmin123"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
            .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            # v1's commitJob re-merges stale _temporary task dirs left by earlier runs on
            # S3-compatible stores; v2 commits per task and does not need that merge.
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .getOrCreate())


def write_dead_letter(batch_df, batch_id):
    """Append rejected rows as Parquet without a streaming file-sink metadata log.

    The built-in Parquet stream sink keeps a `_spark_metadata` log keyed by batch id.
    Resetting the checkpoint (e.g. after changing the dead-letter schema in Phase 2)
    leaves that log behind with higher batch ids, so Spark silently skips every new
    batch and no dead letters are written at all. A plain batch write has no such
    log, so it cannot drift out of sync with the checkpoint.
    """
    if batch_df.isEmpty():
        return
    (batch_df.write.mode("append").partitionBy("rejected_date")
     .parquet(f"s3a://{BUCKET}/dead_letter"))


def write_windows_to_postgres(batch_df, batch_id):
    """Append one microbatch to the warehouse.

    At-least-once on purpose: dbt dedupes. If Postgres is unavailable the batch is copied to
    the lake first, then the exception is re-raised so Spark retries instead of losing data
    or wedging silently.
    """
    try:
        batch_df.write.jdbc(JDBC_URL, "raw.price_windows", mode="append", properties=JDBC_PROPS)
        log.info("batch %s written to Postgres", batch_id)
    except Exception:
        log.exception("batch %s failed to reach Postgres; quarantining to the lake", batch_id)
        try:
            batch_df.write.mode("append").parquet(
                f"s3a://{BUCKET}/dead_letter/sink_failures/batch_id={batch_id}")
        except Exception:
            log.exception("quarantine write also failed for batch %s", batch_id)
        raise


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")
           .option("failOnDataLoss", "false")
           .load())

    parsed = (raw.select(
                F.col("value").cast("string").alias("raw_value"),
                F.col("topic").alias("kafka_topic"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.col("timestamp").alias("kafka_timestamp"))
              .withColumn("t", F.from_json("raw_value", SCHEMA))
              .withColumn("event_ts", F.col("t.event_time").cast("timestamp"))
              .withColumn("error_reason",
                          F.when(F.col("raw_value").isNull() | (F.length("raw_value") == 0), "empty_value")
                           # permissive from_json never returns a null struct, so check for an
                           # actual JSON object before blaming a missing field
                           .when(~(F.trim("raw_value").startswith("{") & F.trim("raw_value").endswith("}")),
                                 "unparseable_json")
                           .when(F.col("t.symbol").isNull() | (F.length("t.symbol") == 0), "missing_symbol")
                           .when(F.col("t.price").isNull(), "missing_price")
                           .when(F.col("t.price") <= 0, "non_positive_price")
                           .when(F.col("event_ts").isNull(), "invalid_event_time")))

    ticks = (parsed.where(F.col("error_reason").isNull())
             .select(F.upper("t.symbol").alias("symbol"), F.col("t.price").alias("price"),
                     F.col("event_ts").alias("event_time"), F.col("t.source").alias("source")))

    dead = (parsed.where(F.col("error_reason").isNotNull())
            .select("raw_value", "error_reason", "kafka_topic", "kafka_partition",
                    "kafka_offset", "kafka_timestamp",
                    F.current_timestamp().alias("rejected_at"))
            .withColumn("rejected_date", F.to_date("rejected_at")))

    q_raw = (ticks.withColumn("event_date", F.to_date("event_time"))
             .writeStream.format("parquet")
             .option("path", f"s3a://{BUCKET}/raw/ticks")
             .option("checkpointLocation", f"{CKPT}/ticks")
             .partitionBy("event_date")
             .trigger(processingTime=TRIGGER).start())

    q_dead = (dead.writeStream.foreachBatch(write_dead_letter)
              .option("checkpointLocation", f"{CKPT}/dead_letter")
              .trigger(processingTime=TRIGGER).start())

    windows = (ticks.withWatermark("event_time", WATERMARK)
               .groupBy(F.window("event_time", "1 minute"), "symbol")
               .agg(F.avg("price").alias("avg_price"), F.min("price").alias("min_price"),
                    F.max("price").alias("max_price"), F.count("*").alias("tick_count"))
               .select("symbol", F.col("window.start").alias("window_start"),
                       F.col("window.end").alias("window_end"),
                       "avg_price", "min_price", "max_price", "tick_count",
                       F.current_timestamp().alias("ingested_at")))

    q_win = (windows.writeStream.outputMode("append")
             .foreachBatch(write_windows_to_postgres)
             .option("checkpointLocation", f"{CKPT}/windows")
             .trigger(processingTime=TRIGGER).start())

    log.info("streaming queries started: %s", [q.name for q in spark.streams.active])
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
