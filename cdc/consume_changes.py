"""Kafka consumer: change events -> warehouse landing table.

This is the piece where most CDC pipelines quietly go wrong, so every decision
in here is deliberate.

DELIVERY SEMANTICS
------------------
Offsets are committed only AFTER the batch has been durably written to the
warehouse. That ordering is the whole design:

    read -> write -> commit      at-least-once. A crash between write and
                                 commit replays the batch. You may see a
                                 message twice, never zero times.

    read -> commit -> write      at-most-once. A crash between commit and
                                 write loses the batch silently, forever, with
                                 no error anywhere. This is the default in
                                 every Kafka client, because auto-commit is on
                                 by default. It is the single most common
                                 data-loss bug in streaming pipelines.

We take at-least-once, which means duplicates are guaranteed to happen
eventually. So the sink has to be idempotent, and it is: every event carries an
event_uid derived from (topic, partition, offset), which Kafka guarantees is
unique and stable, and the load is a MERGE on that key. Replaying a batch
writes nothing new.

TWO KINDS OF FAILURE, TWO DIFFERENT RESPONSES
---------------------------------------------
    A bad message  - unparseable JSON, missing envelope fields. Retrying will
                     never help; it will fail identically forever and block the
                     partition. Send it to the dead-letter topic with the
                     reason attached, and move on.

    A bad sink     - warehouse unreachable, credentials expired, network blip.
                     The message is fine. Retrying WILL help. Back off and try
                     again, and if it still fails, stop the consumer rather
                     than dead-lettering perfectly good data.

Conflating these two is how you end up with a dead-letter queue full of
valid records because Snowflake was down for four minutes.

Run:
    python cdc/consume_changes.py --target duckdb        # local, no creds
    python cdc/consume_changes.py --target snowflake
    python cdc/consume_changes.py --target duckdb --once # drain and exit
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
GROUP_ID = os.getenv("CDC_GROUP_ID", "cdc-warehouse-loader")
TOPICS = ["ordersdb.public.customers", "ordersdb.public.orders"]
DLQ_TOPIC = "cdc.dlq"

BATCH_MAX_ROWS = int(os.getenv("CDC_BATCH_ROWS", "200"))
BATCH_MAX_SECONDS = float(os.getenv("CDC_BATCH_SECONDS", "2"))
# --once waits this many idle seconds for a partition assignment before
# concluding there is genuinely nothing to do.
ONCE_GIVE_UP_POLLS = 90

MAX_SINK_ATTEMPTS = 5
BACKOFF_BASE_SECONDS = 1.0


class BadEvent(Exception):
    """The message itself is wrong. Retrying cannot fix it."""


@dataclass
class ChangeEvent:
    event_uid: str
    source_table: str
    op: str
    lsn: int | None
    source_ts_ms: int | None
    record_key: str
    before_image: str | None
    after_image: str | None
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int

    def as_row(self) -> tuple:
        return (
            self.event_uid, self.source_table, self.op, self.lsn,
            self.source_ts_ms, self.record_key, self.before_image,
            self.after_image, self.kafka_topic, self.kafka_partition,
            self.kafka_offset,
        )


COLUMNS = [
    "EVENT_UID", "SOURCE_TABLE", "OP", "LSN", "SOURCE_TS_MS", "RECORD_KEY",
    "BEFORE_IMAGE", "AFTER_IMAGE", "KAFKA_TOPIC", "KAFKA_PARTITION",
    "KAFKA_OFFSET",
]


def parse(msg) -> ChangeEvent:
    """Turn a Kafka message into a warehouse row, or raise BadEvent.

    The before/after images are kept as raw JSON TEXT, not parsed into columns.
    That is on purpose: the landing table's job is to record exactly what
    arrived, byte for byte. Interpreting it is dbt's job, downstream, where a
    mistake can be fixed by rerunning a model instead of by replaying Kafka.
    """
    raw = msg.value()
    if raw is None:
        # A tombstone: Debezium's null-valued marker for a deleted key. We set
        # tombstones.on.delete=false, so this should not appear - but if the
        # config ever changes, silently crashing here would be worse than
        # saying so.
        raise BadEvent("null message value (tombstone)")

    try:
        envelope = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BadEvent(f"value is not valid JSON: {exc}") from exc

    if not isinstance(envelope, dict):
        raise BadEvent(f"value is {type(envelope).__name__}, expected object")

    op = envelope.get("op")
    if op not in {"c", "u", "d", "r"}:
        raise BadEvent(f"unknown or missing op: {op!r}")

    source = envelope.get("source") or {}
    if not source.get("table"):
        raise BadEvent("envelope has no source.table")

    before, after = envelope.get("before"), envelope.get("after")
    if op in {"c", "u", "r"} and after is None:
        raise BadEvent(f"op={op} with no after image")
    if op == "d" and before is None:
        raise BadEvent("op=d with no before image (is REPLICA IDENTITY set?)")

    key = msg.key()
    return ChangeEvent(
        # topic+partition+offset is Kafka's own unique, stable address for this
        # message. Using it as the dedupe key means a replayed batch merges to
        # nothing, without needing the payload to contain anything unique.
        event_uid=f"{msg.topic()}:{msg.partition()}:{msg.offset()}",
        source_table=source["table"],
        op=op,
        lsn=source.get("lsn"),
        source_ts_ms=source.get("ts_ms"),
        record_key=key.decode() if key else "{}",
        before_image=json.dumps(before) if before is not None else None,
        after_image=json.dumps(after) if after is not None else None,
        kafka_topic=msg.topic(),
        kafka_partition=msg.partition(),
        kafka_offset=msg.offset(),
    )


# --------------------------------------------------------------------------
# Sinks. Both expose write(rows) and both are idempotent on EVENT_UID.
# --------------------------------------------------------------------------

class DuckDBSink:
    """Local file warehouse. No credentials, no network, no cost.

    Worth having even though Snowflake is the real target: it lets you test the
    consumer's failure behaviour dozens of times without touching a warehouse,
    and it makes the chaos test runnable on a plane.
    """

    def __init__(self, path: str):
        import duckdb

        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.con = duckdb.connect(path)
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS cdc_events (
                event_uid       VARCHAR PRIMARY KEY,
                source_table    VARCHAR,
                op              VARCHAR,
                lsn             BIGINT,
                source_ts_ms    BIGINT,
                record_key      VARCHAR,
                before_image    VARCHAR,
                after_image     VARCHAR,
                kafka_topic     VARCHAR,
                kafka_partition INTEGER,
                kafka_offset    BIGINT,
                _consumed_at    TIMESTAMP DEFAULT current_timestamp
            )
            """
        )

    def write(self, rows: list[tuple]) -> None:
        self.con.executemany(
            f"INSERT INTO cdc_events ({','.join(c.lower() for c in COLUMNS)}) "
            f"VALUES ({','.join('?' * len(COLUMNS))}) "
            f"ON CONFLICT (event_uid) DO NOTHING",
            rows,
        )

    def count(self) -> int:
        return self.con.execute("SELECT count(*) FROM cdc_events").fetchone()[0]

    def close(self) -> None:
        self.con.close()


class SnowflakeSink:
    """Batch insert into a scratch table, then MERGE into the landing table.

    Why not INSERT straight into the target? Because INSERT is not idempotent,
    and at-least-once delivery means we will re-send batches. MERGE on
    EVENT_UID makes a replay a no-op. The scratch table exists because a MERGE
    needs a source relation, and because it lets one round trip carry the whole
    batch instead of one statement per row.
    """

    def __init__(self):
        import snowflake.connector

        self.con = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database=os.getenv("SNOWFLAKE_DATABASE", "NYC_TRIPS"),
            schema="RAW",
        )
        cur = self.con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS CDC_EVENTS (
                EVENT_UID       STRING NOT NULL,
                SOURCE_TABLE    STRING,
                OP              STRING,
                LSN             NUMBER,
                SOURCE_TS_MS    NUMBER,
                RECORD_KEY      STRING,
                BEFORE_IMAGE    STRING,
                AFTER_IMAGE     STRING,
                KAFKA_TOPIC     STRING,
                KAFKA_PARTITION NUMBER,
                KAFKA_OFFSET    NUMBER,
                _CONSUMED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
        )
        cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS CDC_EVENTS_STAGE LIKE CDC_EVENTS")
        cur.close()

    def write(self, rows: list[tuple]) -> None:
        cur = self.con.cursor()
        try:
            cur.execute("TRUNCATE TABLE CDC_EVENTS_STAGE")
            cur.executemany(
                f"INSERT INTO CDC_EVENTS_STAGE ({','.join(COLUMNS)}) "
                f"VALUES ({','.join(['%s'] * len(COLUMNS))})",
                rows,
            )
            cur.execute(
                f"""
                MERGE INTO CDC_EVENTS t
                USING CDC_EVENTS_STAGE s ON t.EVENT_UID = s.EVENT_UID
                WHEN NOT MATCHED THEN INSERT ({','.join(COLUMNS)})
                VALUES ({','.join('s.' + c for c in COLUMNS)})
                """
            )
            self.con.commit()
        finally:
            cur.close()

    def count(self) -> int:
        cur = self.con.cursor()
        try:
            return cur.execute("SELECT count(*) FROM CDC_EVENTS").fetchone()[0]
        finally:
            cur.close()

    def close(self) -> None:
        self.con.close()


# --------------------------------------------------------------------------

def write_with_retry(sink, rows: list[tuple]) -> None:
    """Exponential backoff, then give up loudly.

    Giving up means raising, which stops the consumer without committing
    offsets. On restart the batch is re-read and re-attempted. That is the
    correct behaviour: an unavailable warehouse is a reason to stop, not a
    reason to throw data away.
    """
    for attempt in range(1, MAX_SINK_ATTEMPTS + 1):
        try:
            sink.write(rows)
            return
        except Exception as exc:
            if attempt == MAX_SINK_ATTEMPTS:
                print(f"  sink failed {attempt}x, giving up: {exc}", file=sys.stderr)
                raise
            delay = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            print(f"  sink attempt {attempt} failed ({exc}); retry in {delay:.0f}s",
                  file=sys.stderr)
            time.sleep(delay)


def to_dlq(producer: Producer, msg, reason: str) -> None:
    """Park an unprocessable message with enough context to debug it later.

    The original bytes are preserved. A dead-letter record you cannot replay is
    just a log line with extra steps.
    """
    payload = {
        "reason": reason,
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "key": msg.key().decode(errors="replace") if msg.key() else None,
        "value": msg.value().decode(errors="replace") if msg.value() else None,
        "failed_at": time.time(),
    }
    producer.produce(DLQ_TOPIC, json.dumps(payload).encode())
    producer.flush(10)


def run(target: str, once: bool) -> None:
    sink = DuckDBSink(os.getenv("CDC_DUCKDB", "warehouse/cdc.duckdb")) \
        if target == "duckdb" else SnowflakeSink()

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        # The two settings that make this at-least-once instead of at-most-once.
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        # When a consumer dies without saying goodbye - SIGKILL, OOM, power cut
        # - the group coordinator keeps its partitions RESERVED until the
        # member's session expires. It does not reassign them. librdkafka
        # defaults that timeout to 45 seconds, which means a single-consumer
        # pipeline is stalled for 45s after a hard crash even if you restart
        # it immediately.
        #
        # 6s (the broker's default minimum) fails over quickly. The trade is
        # real: set it too low and a consumer merely pausing - a long GC, a
        # slow warehouse write - gets evicted and triggers a rebalance it did
        # not need. Heartbeats must be well under the timeout, hence 2s.
        "session.timeout.ms": int(os.getenv("CDC_SESSION_TIMEOUT_MS", "6000")),
        "heartbeat.interval.ms": 2000,
        # A batch that takes longer than this between polls looks dead. Our
        # unit of work is one micro-batch, so this bounds the warehouse write.
        "max.poll.interval.ms": 300000,
    })
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe(TOPICS)

    running = True

    def stop(signum, frame):
        nonlocal running
        print("\nshutting down after current batch...")
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    def drained() -> bool:
        """Have we actually consumed everything, or is it just quiet?

        "No message for three polls" is not the same as "caught up". It is
        also what you see while waiting for a dead member's session to expire,
        or during a rebalance, or on a slow broker. Asking the broker where
        each partition ends and comparing it to our own position is the only
        answer that means anything.
        """
        assignment = consumer.assignment()
        if not assignment:
            return False          # not assigned yet - silence proves nothing
        for tp in assignment:
            _, end = consumer.get_watermark_offsets(tp, timeout=5, cached=False)
            pos = consumer.position([tp])[0].offset
            if pos is None or pos < 0:
                committed = consumer.committed([tp], timeout=5)[0].offset
                pos = committed if committed and committed >= 0 else 0
            if pos < end:
                return False
        return True

    total_written = total_dlq = 0
    batch: list[tuple] = []
    batch_started = time.monotonic()
    idle_polls = 0

    def flush() -> None:
        nonlocal batch, batch_started, total_written
        if batch:
            write_with_retry(sink, batch)
            # Only now is it safe to tell Kafka we are done with these.
            consumer.commit(asynchronous=False)
            total_written += len(batch)
            print(f"  committed {len(batch)} events (total {total_written})")
            batch = []
        batch_started = time.monotonic()

    try:
        while running:
            msg = consumer.poll(1.0)

            if msg is None:
                idle_polls += 1
                if batch and time.monotonic() - batch_started >= BATCH_MAX_SECONDS:
                    flush()
                if once and not batch and idle_polls >= 3 and drained():
                    break
                if once and idle_polls >= ONCE_GIVE_UP_POLLS:
                    print("  no assignment after "
                          f"{ONCE_GIVE_UP_POLLS}s - giving up", file=sys.stderr)
                    break
                continue

            idle_polls = 0
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            try:
                batch.append(parse(msg).as_row())
            except BadEvent as exc:
                total_dlq += 1
                print(f"  DLQ {msg.topic()}:{msg.partition()}:{msg.offset()} - {exc}",
                      file=sys.stderr)
                to_dlq(producer, msg, str(exc))
                # A dead-lettered message is handled, so it must not block the
                # partition. It will be committed with the surrounding batch.

            if len(batch) >= BATCH_MAX_ROWS or \
               time.monotonic() - batch_started >= BATCH_MAX_SECONDS:
                flush()

        flush()
    finally:
        consumer.close()
        producer.flush(10)
        print(f"\nwritten: {total_written}   dead-lettered: {total_dlq}")
        try:
            print(f"rows in landing table: {sink.count():,}")
        finally:
            sink.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", choices=["duckdb", "snowflake"], default="duckdb")
    ap.add_argument("--once", action="store_true",
                    help="drain what is there and exit instead of tailing")
    args = ap.parse_args()
    run(args.target, args.once)


if __name__ == "__main__":
    main()
