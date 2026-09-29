"""Milestone 2, step 2: read the Kafka topic with Spark Structured Streaming.

Every 10 seconds Spark asks Kafka for any new messages, handles them as one
small batch, and prints a summary. Nothing is saved yet; that starts in step 4.

Run from the project root (Kafka must be running):
    python spark/kafka_console.py
"""

import signal
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# The Kafka connector isn't part of PySpark, so Spark downloads it on the first run.
# Its version must match PySpark's (3.5.9) and Spark's Scala version (2.12).
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9"

spark = (
    SparkSession.builder
    .appName("kafka-console")
    .master("local[*]")
    .config("spark.jars.packages", KAFKA_PACKAGE)
    .config("spark.sql.shuffle.partitions", 4)  # Spark's default of 200 suits clusters; 4 is plenty on a laptop
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# A Kafka stream behaves like a table that keeps growing. Each row is one message:
# key and value (raw bytes) plus where it came from: topic, partition, offset, timestamp.
messages = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "taxi.trips.v1")
    .option("startingOffsets", "earliest")  # with no saved progress, start from the oldest message
    .option("maxOffsetsPerTrigger", 5000)   # take at most 5,000 messages per batch
    .load()
)


def show_batch(batch: DataFrame, batch_id: int) -> None:
    """Spark calls this once per micro-batch, passing the new messages as a normal DataFrame."""
    batch.persist()  # the batch is used three times below; keep it in memory instead of re-reading Kafka
    total = batch.count()
    print(f"\n=== Batch {batch_id}: {total:,} new messages ===")
    if total:
        (batch.groupBy("partition")
         .agg(F.count("*").alias("messages"),
              F.min("offset").alias("first_offset"),
              F.max("offset").alias("last_offset"))
         .orderBy("partition")
         .show())
        (batch.select(F.col("key").cast("string").alias("zone"), "partition", "offset",
                      F.get_json_object(F.col("value").cast("string"), "$.event_time").alias("event_time"))
         .show(3, truncate=False))
    batch.unpersist()


query = (
    messages.writeStream
    .foreachBatch(show_batch)
    .trigger(processingTime="10 seconds")
    .start()
)

# PySpark's own Ctrl+C handler cancels running Spark jobs mid-batch, which ends the stream with an
# error. Python's default handler just raises KeyboardInterrupt, so we can finish the batch and stop cleanly.
signal.signal(signal.SIGINT, signal.default_int_handler)

print("Streaming from Kafka. A summary prints every 10 seconds when new messages arrive. Press Ctrl+C to stop.")
try:
    while query.isActive:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopping: letting the current batch finish first...")
    while query.status["isTriggerActive"]:
        time.sleep(0.5)
    query.stop()
if query.exception():
    print("The stream failed:", query.exception())
spark.stop()