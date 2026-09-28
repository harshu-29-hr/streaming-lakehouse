"""Replay NYC TLC yellow-taxi trips into Kafka as a live event stream.

Built up over Steps 6 to 9. For now it loads one month of trips, turns each
trip into an event, and prints the first few so you can see what they look like.

Run from the project root:
    python producer/replay_producer.py
    python producer/replay_producer.py --file data/yellow_tripdata_2025-02.parquet
"""

import argparse
import json
import logging
import math
import re
import sys
import uuid
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

log = logging.getLogger("replay")

# TLC column -> (event field, type). We keep only what the pipeline needs, rename
# to snake_case, and force one type per field, because TLC files from different
# years disagree (for example, passenger_count is a float in older files).
COLUMNS = {
    "VendorID": ("vendor_id", pa.int64()),
    "tpep_pickup_datetime": ("pickup_time", pa.timestamp("us")),
    "tpep_dropoff_datetime": ("event_time", pa.timestamp("us")),
    "passenger_count": ("passenger_count", pa.int64()),
    "trip_distance": ("trip_distance", pa.float64()),
    "PULocationID": ("pu_location_id", pa.int64()),
    "DOLocationID": ("do_location_id", pa.int64()),
    "payment_type": ("payment_type", pa.int64()),
    "fare_amount": ("fare_amount", pa.float64()),
    "tip_amount": ("tip_amount", pa.float64()),
    "total_amount": ("total_amount", pa.float64()),
}

# Fields that identify a trip. The same trip always gets the same event_id,
# so if it's ever delivered twice, the pipeline can spot the duplicate.
ID_FIELDS = ("vendor_id", "pickup_time", "event_time", "pu_location_id", "do_location_id", "total_amount")
ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "streaming-lakehouse/taxi-trip")


def month_bounds(path: str, month: str | None) -> tuple[datetime, datetime]:
    """Start and end of the month the file covers (end is the 1st of the next month)."""
    match = re.search(r"(\d{4})-(\d{2})", month or path)
    if not match:
        sys.exit("Can't tell which month this file covers. Pass --month YYYY-MM.")
    year, mon = int(match[1]), int(match[2])
    return datetime(year, mon, 1), datetime(year + mon // 12, mon % 12 + 1, 1)


def load_trips(path: str, month: str | None, limit: int | None) -> pa.Table:
    # Parquet stores data by column, so reading 11 of 20 columns skips the rest entirely
    raw = pq.read_table(path, columns=list(COLUMNS))
    table = pa.table({field: pc.cast(raw[src], typ, safe=False) for src, (field, typ) in COLUMNS.items()})

    # Keep only trips that end inside the file's month, then sort into stream order
    start, end = month_bounds(path, month)
    ts = pa.timestamp("us")
    in_month = pc.and_(
        pc.greater_equal(table["event_time"], pa.scalar(start, ts)),
        pc.less(table["event_time"], pa.scalar(end, ts)),
    )
    total = table.num_rows
    table = table.filter(in_month).sort_by("event_time")
    dropped = total - table.num_rows
    if limit:
        table = table.slice(0, limit)

    log.info("Loaded %s trips for %s (dropped %s outside the month)",
             f"{table.num_rows:,}", start.strftime("%Y-%m"), f"{dropped:,}")
    return table


def to_event(row: dict) -> dict:
    """Turn one trip (a row) into one event, ready to send as JSON."""
    event = {
        "event_id": str(uuid.uuid5(ID_NAMESPACE, "|".join(str(row[f]) for f in ID_FIELDS))),
        "event_type": "trip_completed",
        "schema_version": 1,
    }
    for field, value in row.items():
        if isinstance(value, datetime):
            value = value.isoformat(timespec="seconds")  # JSON has no date type, so dates become text
        elif isinstance(value, float) and math.isnan(value):
            value = None  # NaN isn't valid JSON; null is
        event[field] = value
    return event


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--file", default="data/yellow_tripdata_2025-01.parquet",
                   help="NYC TLC yellow taxi parquet file (default: %(default)s)")
    p.add_argument("--month", help="YYYY-MM the file covers (default: read from the file name)")
    p.add_argument("--limit", type=int, help="stop after this many trips")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

    table = load_trips(args.file, args.month, args.limit)

    # Preview the first 3 events. Step 7 replaces this with the real replay loop.
    for row in table.slice(0, 3).to_pylist():
        print(json.dumps(to_event(row), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())