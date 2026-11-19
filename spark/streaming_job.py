"""Spark Structured Streaming job.

Kafka (crypto.prices) -> validate
  |-- valid ticks   -> S3 lake Parquet  (raw data lake, partitioned by date)
  |-- valid ticks   -> 1-minute tumbling window per symbol -> PostgreSQL raw.price_windows
  |-- invalid msgs  -> S3 dead_letter/ (never silently dropped)
"""
import os

from pyspark.sql import SparkSession, functions as F, types as T

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto.prices")
BUCKET = os.getenv("S3_BUCKET", "datalake")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://lake:8333")
CKPT = "/checkpoints"
TRIGGER = "30 seconds"

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
            .getOrCreate())


def write_windows_to_postgres(batch_df, batch_id):
    batch_df.write.jdbc(JDBC_URL, "raw.price_windows", mode="append", properties=JDBC_PROPS)


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")
           .option("failOnDataLoss", "false")
           .load()
           .select(F.col("value").cast("string").alias("value")))

    parsed = (raw.withColumn("t", F.from_json("value", SCHEMA))
              .withColumn("event_ts", F.col("t.event_time").cast("timestamp")))
    is_valid = (F.col("t.symbol").isNotNull() & (F.col("t.price") > 0) & F.col("event_ts").isNotNull())

    ticks = (parsed.filter(is_valid)
             .select(F.upper("t.symbol").alias("symbol"), F.col("t.price").alias("price"),
                     F.col("event_ts").alias("event_time"), F.col("t.source").alias("source")))

    dead = (parsed.filter(~is_valid)
            .select("value", F.current_timestamp().alias("rejected_at"))
            .withColumn("rejected_date", F.to_date("rejected_at")))

    q_raw = (ticks.withColumn("event_date", F.to_date("event_time"))
             .writeStream.format("parquet")
             .option("path", f"s3a://{BUCKET}/raw/ticks")
             .option("checkpointLocation", f"{CKPT}/ticks")
             .partitionBy("event_date")
             .trigger(processingTime=TRIGGER).start())

    q_dead = (dead.writeStream.format("parquet")
              .option("path", f"s3a://{BUCKET}/dead_letter")
              .option("checkpointLocation", f"{CKPT}/dead_letter")
              .partitionBy("rejected_date")
              .trigger(processingTime=TRIGGER).start())

    windows = (ticks.withWatermark("event_time", "1 minute")
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

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
