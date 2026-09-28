import re
import sys
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

path = sys.argv[1] if len(sys.argv) > 1 else "data/yellow_tripdata_2025-01.parquet"
match = re.search(r"(\d{4})-(\d{2})", path)
if not match:
    sys.exit("Pass a TLC file named like yellow_tripdata_YYYY-MM.parquet")
if not Path(path).exists():
    sys.exit(f"{path} not found. Download it into data/ first.")
table = pq.read_table(path)
year, month = int(match[1]), int(match[2])
month_start = datetime(year, month, 1)
month_end = datetime(year + month // 12, month % 12 + 1, 1)


def count(mask) -> int:
    """How many rows a true/false column marks as true (nulls count as false)."""
    return pc.sum(pc.fill_null(mask, False)).as_py() or 0


def pct(n: int) -> str:
    return f"{n:,} ({n / table.num_rows:.2%})"


print(f"\n== {path}: {table.num_rows:,} trips, {table.num_columns} columns")
for field in table.schema:
    print(f"   {field.name:<24} {str(field.type):<14} nulls: {table[field.name].null_count:,}")

# The dropoff time becomes each event's time, so its range matters most
dropoff = table["tpep_dropoff_datetime"]
pickup = table["tpep_pickup_datetime"]
start, end = pa.scalar(month_start, dropoff.type), pa.scalar(month_end, dropoff.type)
in_month = pc.and_(pc.greater_equal(dropoff, start), pc.less(dropoff, end))
print(f"\n== Dropoff times range from {pc.min(dropoff).as_py()} to {pc.max(dropoff).as_py()}")
print(f"   outside {month_start:%B %Y}: {pct(count(pc.invert(in_month)))}")

print("\n== Data quality (the producer passes these through; silver deals with them)")
print(f"   negative fare:          {pct(count(pc.less(table['fare_amount'], 0)))}")
print(f"   zero distance:          {pct(count(pc.equal(table['trip_distance'], 0)))}")
print(f"   dropoff before pickup:  {pct(count(pc.less(dropoff, pickup)))}")
print(f"   passenger_count 0:      {pct(count(pc.equal(table['passenger_count'], 0)))}")

# Trips per hour decides how fast the stream runs: at 60x, one hour of trips plays in one minute
hourly = pc.value_counts(pc.floor_temporal(pc.filter(dropoff, in_month), unit="hour")).field("counts")
print("\n== Trips per hour")
print(f"   quietest: {pc.min(hourly).as_py():,}   average: {pc.mean(hourly).as_py():,.0f}   busiest: {pc.max(hourly).as_py():,}")
print(f"   at 60x: {pc.min(hourly).as_py() / 60:,.0f} / {pc.mean(hourly).as_py() / 60:,.0f} / {pc.max(hourly).as_py() / 60:,.0f} events per second")

# The pickup zone becomes the Kafka key, so its spread decides how evenly partitions fill
zones = pc.value_counts(table["PULocationID"])
top = pa.table({"zone": zones.field("values"), "trips": zones.field("counts")}).sort_by([("trips", "descending")])
print(f"\n== Pickup zones: {top.num_rows} distinct. Top 10:")
for row in top.slice(0, 10).to_pylist():
    print(f"   zone {row['zone']:>3}: {pct(row['trips'])}")

print("\n== One trip, as it will become an event:")
for key, value in table.filter(in_month).slice(0, 1).to_pylist()[0].items():
    print(f"   {key}: {value}")