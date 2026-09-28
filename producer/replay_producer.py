"""Replay NYC TLC yellow-taxi trips into Kafka as a live event stream.

It replays one month of trips in event-time order at a chosen speed and sends
each one to Kafka, keyed by pickup zone. Optional chaos knobs deliver some events
late (out of order) and some twice, so the pipeline downstream has real
problems to handle.

Run from the project root (Kafka must be running):
    python producer/replay_producer.py --limit 5000
    python producer/replay_producer.py --late-rate 0.05 --dup-rate 0.02
    python producer/replay_producer.py --dry-run --speed 1 --limit 30
"""

import argparse
import heapq
import itertools
import json
import logging
import math
import random
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

LOG_EVERY_S = 10     # how often to print a progress line
DUP_WINDOW_S = 300   # duplicates arrive up to 5 minutes (event time) after the original


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


class KafkaSink:
    """Sends events to a Kafka topic."""

    def __init__(self, bootstrap: str, topic: str):
        from confluent_kafka import Producer  # imported here so --dry-run works without Kafka

        self.topic = topic
        self.failed = 0
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": "taxi-replay-producer",
            "enable.idempotence": True,       # a retried send can't create a duplicate; implies acks=all
            "compression.type": "zstd",       # smaller messages on the wire
            "linger.ms": 20,                  # wait up to 20 ms so messages go out in batches
            "partitioner": "murmur2_random",  # hash keys the same way Kafka's own Java tools do
        })
        # Record timestamps stay at the real send time. The trip's event time lives in the
        # payload: stamping records with a January date would make Kafka's 7-day retention
        # treat them as already expired and delete them.

    def _on_delivery(self, err, msg) -> None:
        """Called once per message, when Kafka confirms it arrived (or gives up)."""
        if err is not None:
            self.failed += 1
            if self.failed <= 5:
                log.error("Delivery failed: %s", err)

    def send(self, key: str, value: bytes) -> None:
        while True:
            try:
                self.producer.produce(self.topic, key=key, value=value, on_delivery=self._on_delivery)
                break
            except BufferError:  # the local send queue is full: let it drain, then retry
                self.producer.poll(0.5)
        self.producer.poll(0)  # handle any delivery confirmations that have come back

    def close(self) -> int:
        undelivered = self.producer.flush(30)  # wait up to 30 s for messages still in flight
        if undelivered:
            log.error("%d messages still undelivered at shutdown", undelivered)
        return self.failed + undelivered


class StdoutSink:
    """--dry-run: print events as `key<TAB>json` instead of sending them."""

    def send(self, key: str, value: bytes) -> None:
        sys.stdout.write(f"{key}\t{value.decode()}\n")

    def close(self) -> int:
        sys.stdout.flush()
        return 0  # number of failed sends


class Replayer:
    """Sends events on a replay clock: at speed 60, one minute of trips plays in one second.
    Late and duplicate deliveries wait in a queue until the replay reaches their release time."""

    def __init__(self, sink, speed: float):
        self.sink = sink
        self.speed = speed
        self.wall_start = time.monotonic()  # a clock that never jumps, unlike the time of day
        self.pending = []                   # held events, always popped earliest release first
        self.seq = itertools.count()        # tie-breaker so two events due at the same time don't clash
        self.sent = self.late = self.dups = 0
        self._last_log, self._last_sent = self.wall_start, 0

    def pace(self, t: float) -> None:
        """Wait until the event t seconds into the replay is due (no waiting at speed 0)."""
        if self.speed > 0:
            delay = self.wall_start + t / self.speed - time.monotonic()
            if delay > 0.001:
                time.sleep(delay)

    def hold(self, release_at: float, key: str, event: dict) -> None:
        """Keep an event back until the replay reaches release_at (seconds into the replay)."""
        heapq.heappush(self.pending, (release_at, next(self.seq), key, event))

    def release_until(self, t: float) -> None:
        """Send every held event whose release time has come."""
        while self.pending and self.pending[0][0] <= t:
            release_at, _, key, event = heapq.heappop(self.pending)
            self.pace(release_at)
            self.emit(key, event)

    def emit(self, key: str, event: dict) -> None:
        # produced_at is the real time it was sent, so later we can measure end-to-end delay
        payload = {**event, "produced_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds")}
        self.sink.send(key, json.dumps(payload, separators=(",", ":")).encode())
        self.sent += 1

        now = time.monotonic()
        if now - self._last_log >= LOG_EVERY_S:
            rate = (self.sent - self._last_sent) / (now - self._last_log)
            log.info("sent=%s  rate=%.0f ev/s  event_time=%s  held=%d",
                     f"{self.sent:,}", rate, event["event_time"], len(self.pending))
            self._last_log, self._last_sent = now, self.sent


def run(args: argparse.Namespace) -> int:
    table = load_trips(args.file, args.month, args.limit)
    if table.num_rows == 0:
        log.error("No trips to replay.")
        return 1

    rng = random.Random(args.seed)  # same seed, same "random" choices: runs are repeatable
    late_max_s = args.late_max_minutes * 60
    sink = StdoutSink() if args.dry_run else KafkaSink(args.bootstrap, args.topic)
    replayer = Replayer(sink, args.speed)
    t0 = table["event_time"][0].as_py()
    target = "stdout" if args.dry_run else f"{args.bootstrap} / {args.topic}"
    speed = f"{args.speed:g}x" if args.speed else "max speed"
    log.info("Replaying to %s at %s (late %.1f%%, duplicates %.1f%%). Ctrl+C to stop.",
             target, speed, args.late_rate * 100, args.dup_rate * 100)
    try:
        # Convert rows to Python in chunks of 50,000 rather than all 3.5 million at once
        for batch in table.to_batches(max_chunksize=50_000):
            for row in batch.to_pylist():
                t = (row["event_time"] - t0).total_seconds()  # how far into the replay this trip is
                key = str(row["pu_location_id"])
                event = to_event(row)

                replayer.release_until(t)  # first send any held events that are now due
                replayer.pace(t)
                if rng.random() < args.late_rate:
                    replayer.late += 1
                    replayer.hold(t + rng.uniform(1, late_max_s), key, event)
                else:
                    replayer.emit(key, event)
                if rng.random() < args.dup_rate:
                    replayer.dups += 1
                    replayer.hold(t + rng.uniform(0, DUP_WINDOW_S), key, event)
        replayer.release_until(math.inf)  # the month is over: send everything still held
    except KeyboardInterrupt:
        log.info("Interrupted. %d held events were not sent.", len(replayer.pending))

    failures = sink.close()
    log.info("Done: %s events sent (%s held back as late, %s duplicates), %s failures",
             f"{replayer.sent:,}", f"{replayer.late:,}", f"{replayer.dups:,}", failures)
    return 1 if failures else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--file", default="data/yellow_tripdata_2025-01.parquet",
                   help="NYC TLC yellow taxi parquet file (default: %(default)s)")
    p.add_argument("--month", help="YYYY-MM the file covers (default: read from the file name)")
    p.add_argument("--limit", type=int, help="stop after this many trips")
    p.add_argument("--bootstrap", default="localhost:9092", help="Kafka address (default: %(default)s)")
    p.add_argument("--topic", default="taxi.trips.v1", help="Kafka topic (default: %(default)s)")
    p.add_argument("--dry-run", action="store_true", help="print events instead of sending them to Kafka")
    p.add_argument("--speed", type=float, default=60.0,
                   help="replay speed vs. real time; 60 = one minute of trips per second, "
                        "0 = as fast as possible (default: %(default)s)")
    p.add_argument("--late-rate", type=float, default=0.0, help="fraction of events delivered late, e.g. 0.05")
    p.add_argument("--late-max-minutes", type=float, default=15.0,
                   help="how late (in event-time minutes) a late event can be (default: %(default)s)")
    p.add_argument("--dup-rate", type=float, default=0.0, help="fraction of events delivered twice, e.g. 0.02")
    p.add_argument("--seed", type=int, default=42, help="random seed for the chaos knobs (default: %(default)s)")
    args = p.parse_args(argv)
    if args.speed < 0:
        p.error("--speed must be 0 or more")
    for flag in ("late_rate", "dup_rate"):
        if not 0 <= getattr(args, flag) <= 1:
            p.error(f"--{flag.replace('_', '-')} must be between 0 and 1")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    return run(args)


if __name__ == "__main__":
    sys.exit(main())