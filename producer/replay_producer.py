"""Replay NYC TLC yellow-taxi trips into Kafka as a live event stream.

Built up over Steps 6 to 9. For now it replays one month of trips in event-time
order at a chosen speed, printing each event instead of sending it to Kafka.

Run from the project root:
    python producer/replay_producer.py --speed 1 --limit 30
    python producer/replay_producer.py > /dev/null
"""

import argparse
import json
import logging
import math
import re
import sys
import time
import uuid
from datetime import datetime, timezone

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

LOG_EVERY_S = 10  # how often to print a progress line


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


class StdoutSink:
    """Where events go for now: printed as `key<TAB>json`. Step 8 adds a Kafka sink."""

    def send(self, key: str, value: bytes) -> None:
        sys.stdout.write(f"{key}\t{value.decode()}\n")

    def close(self) -> int:
        sys.stdout.flush()
        return 0  # number of failed sends


class Replayer:
    """Sends events on a replay clock: at speed 60, one minute of trips plays in one second."""

    def __init__(self, sink, speed: float):
        self.sink = sink
        self.speed = speed
        self.wall_start = time.monotonic()  # a clock that never jumps, unlike the time of day
        self.sent = 0
        self._last_log, self._last_sent = self.wall_start, 0

    def pace(self, t: float) -> None:
        """Wait until the event t seconds into the replay is due (no waiting at speed 0)."""
        if self.speed > 0:
            delay = self.wall_start + t / self.speed - time.monotonic()
            if delay > 0.001:
                time.sleep(delay)

    def emit(self, key: str, event: dict) -> None:
        # produced_at is the real time it was sent, so later we can measure end-to-end delay
        payload = {**event, "produced_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds")}
        self.sink.send(key, json.dumps(payload, separators=(",", ":")).encode())
        self.sent += 1

        now = time.monotonic()
        if now - self._last_log >= LOG_EVERY_S:
            rate = (self.sent - self._last_sent) / (now - self._last_log)
            log.info("sent=%s  rate=%.0f ev/s  event_time=%s", f"{self.sent:,}", rate, event["event_time"])
            self._last_log, self._last_sent = now, self.sent


def run(args: argparse.Namespace) -> int:
    table = load_trips(args.file, args.month, args.limit)
    if table.num_rows == 0:
        log.error("No trips to replay.")
        return 1

    sink = StdoutSink()
    replayer = Replayer(sink, args.speed)
    t0 = table["event_time"][0].as_py()
    log.info("Replaying at %s. Ctrl+C to stop.", f"{args.speed:g}x" if args.speed else "max speed")
    try:
        # Convert rows to Python in chunks of 50,000 rather than all 3.5 million at once
        for batch in table.to_batches(max_chunksize=50_000):
            for row in batch.to_pylist():
                t = (row["event_time"] - t0).total_seconds()  # how far into the replay this trip is
                replayer.pace(t)
                replayer.emit(str(row["pu_location_id"]), to_event(row))
    except KeyboardInterrupt:
        log.info("Interrupted.")

    failures = sink.close()
    log.info("Done: %s events sent, %s failures", f"{replayer.sent:,}", failures)
    return 1 if failures else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--file", default="data/yellow_tripdata_2025-01.parquet",
                   help="NYC TLC yellow taxi parquet file (default: %(default)s)")
    p.add_argument("--month", help="YYYY-MM the file covers (default: read from the file name)")
    p.add_argument("--limit", type=int, help="stop after this many trips")
    p.add_argument("--speed", type=float, default=60.0,
                   help="replay speed vs. real time; 60 = one minute of trips per second, "
                        "0 = as fast as possible (default: %(default)s)")
    args = p.parse_args(argv)
    if args.speed < 0:
        p.error("--speed must be 0 or more")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    return run(args)


if __name__ == "__main__":
    sys.exit(main())