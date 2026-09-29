"""Milestone 2, step 1: check that Spark runs on this Mac and can read the taxi data.

Run from the project root:
    python spark/smoke_test.py
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("smoke-test")
    .master("local[*]")  # run Spark inside this one program, using every CPU core
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")  # hide Spark's routine INFO messages

trips = spark.read.parquet("data/yellow_tripdata_2025-01.parquet")
print(f"\nSpark {spark.version} read {trips.count():,} trips from the parquet file.\n")

print("Busiest pickup zones (compare with Step 5):")
trips.groupBy("PULocationID").count().orderBy(F.desc("count")).show(5)

# Keep Spark alive so its web UI can be opened. Ctrl+C stops it; stray Enter presses don't.
print(f"Spark UI: {spark.sparkContext.uiWebUrl}  (http://localhost:4040 works too)")
print("Spark is still running so you can look at the UI. Press Ctrl+C here to stop.")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopping Spark.")
spark.stop()