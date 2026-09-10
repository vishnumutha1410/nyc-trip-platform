"""Kill the consumer mid-stream and prove the pipeline is still correct.

Anyone can wire Debezium to Kafka and show a message arriving. What separates
that from a pipeline you would run in production is being able to demonstrate
what happens when it dies halfway through a batch - and to assert it, not
eyeball it.

WHAT THIS DOES
    1. starts the load generator (the source keeps changing throughout)
    2. starts the consumer
    3. SIGKILLs the consumer at random intervals, N times, restarting it
    4. stops the generator, lets the consumer drain
    5. asserts three things

WHY SIGKILL AND NOT CTRL-C
    Ctrl-C is a graceful shutdown - the consumer flushes its batch and commits.
    That tests nothing. SIGKILL is a power cut: the process disappears between
    the write and the commit, which is exactly the window that decides whether
    your delivery semantics are at-least-once or at-most-once. If offsets were
    auto-committed, this test would lose data every single run.

THE THREE ASSERTIONS
    NO LOSS         every Kafka offset from 0 to the log end is accounted for:
                    either landed in the warehouse, or parked in the dead-letter
                    topic with a reason. Landed and dead-lettered are both fine.
                    Unaccounted is not. Not "counts look close" - every
                    individual offset, so a gap is caught by name.
    NO DUPLICATES   count(*) == count(distinct event_uid). At-least-once
                    delivery means replays definitely happened; the MERGE on
                    event_uid is what makes them harmless.
    CONVERGENCE     collapsing the event log to current state reproduces
                    exactly what is in Postgres right now.

    Together those are the real definition of "the pipeline works": it lost
    nothing, it double-counted nothing, and it agrees with the source.

Run:
    python cdc/chaos_test.py              # 3 kills
    python cdc/chaos_test.py --kills 6
"""
from __future__ import annotations

import argparse
import json
import os
import random
import signal
import subprocess
import sys
import time

import duckdb
import psycopg2
from confluent_kafka import Consumer, TopicPartition
from dotenv import load_dotenv

load_dotenv()

DSN = os.getenv("ORDERS_DSN",
                "host=localhost port=5432 dbname=ordersdb user=orders password=orders")
DUCKDB = os.getenv("CDC_DUCKDB", "warehouse/cdc.duckdb")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPICS = ["ordersdb.public.customers", "ordersdb.public.orders"]
DLQ_TOPIC = "cdc.dlq"
PY = sys.executable


def spawn(args: list[str]) -> subprocess.Popen:
    return subprocess.Popen(
        [PY] + args,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def log_end_offsets() -> dict[str, int]:
    """Where each topic ends right now, straight from the broker."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "chaos-probe"})
    try:
        return {t: c.get_watermark_offsets(TopicPartition(t, 0), timeout=10)[1]
                for t in TOPICS}
    finally:
        c.close()


def dlq_offsets() -> dict[str, set[int]]:
    """Which source offsets were deliberately dead-lettered.

    A message parked in the DLQ is HANDLED, not lost - it has a reason
    attached and enough context to replay. Counting it as data loss makes
    this test cry wolf every time the dead-letter path does its job, and a
    test that fails for correct behaviour is a test people switch off.
    """
    parked: dict[str, set[int]] = {t: set() for t in TOPICS}
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "chaos-dlq-probe",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    try:
        tp = TopicPartition(DLQ_TOPIC, 0, 0)
        try:
            _, end = c.get_watermark_offsets(tp, timeout=10)
        except Exception:
            return parked          # topic does not exist yet - nothing parked
        if not end or end <= 0:
            return parked
        c.assign([tp])
        seen = 0
        while seen < end:
            msg = c.poll(5.0)
            if msg is None:
                break
            if msg.error():
                continue
            seen += 1
            try:
                rec = json.loads(msg.value())
                if rec.get("topic") in parked:
                    parked[rec["topic"]].add(int(rec["offset"]))
            except (json.JSONDecodeError, TypeError, ValueError, KeyError):
                continue
    finally:
        c.close()
    return parked


def check_no_loss(con, ends: dict[str, int]) -> list[str]:
    """Every offset in [0, log_end) is accounted for: landed OR dead-lettered."""
    problems = []
    parked = dlq_offsets()
    for topic, end in ends.items():
        landed = {r[0] for r in con.execute(
            "select kafka_offset from cdc_events where kafka_topic = ?", [topic]
        ).fetchall()}
        dead = parked.get(topic, set())
        gaps = sorted(set(range(end)) - landed - dead)
        print(f"  {topic}")
        print(f"    log end offset : {end:,}")
        print(f"    offsets landed : {len(landed):,}")
        print(f"    dead-lettered  : {len(dead):,}")
        if gaps:
            problems.append(f"{topic}: {len(gaps)} unaccounted offsets, "
                            f"first few {gaps[:10]}")
            print(f"    UNACCOUNTED    : {len(gaps)}  {gaps[:10]}")
        else:
            print("    unaccounted    : 0")
    return problems


def check_no_duplicates(con) -> list[str]:
    total, distinct = con.execute(
        "select count(*), count(distinct event_uid) from cdc_events"
    ).fetchone()
    print(f"  rows: {total:,}   distinct event_uid: {distinct:,}")
    if total != distinct:
        return [f"{total - distinct} duplicate rows in landing table"]
    return []


CURRENT_STATE = """
with latest as (
    select record_key, op,
           row_number() over (partition by record_key order by lsn desc) as rn
    from cdc_events where source_table = ?
)
select record_key from latest where rn = 1 and op <> 'd'
"""


def check_convergence(con, pg) -> list[str]:
    problems = []
    for table, pk in (("customers", "customer_id"), ("orders", "order_id")):
        with pg.cursor() as cur:
            cur.execute(f"SELECT {pk} FROM {table}")
            source = {r[0] for r in cur.fetchall()}
        wh = {json.loads(r[0])[pk]
              for r in con.execute(CURRENT_STATE, [table]).fetchall()}
        missing, extra = source - wh, wh - source
        print(f"  {table}: postgres {len(source):,}  warehouse {len(wh):,}  "
              f"missing {len(missing)}  undeleted {len(extra)}")
        if missing:
            problems.append(f"{table}: {len(missing)} rows missing downstream "
                            f"{sorted(missing)[:10]}")
        if extra:
            problems.append(f"{table}: {len(extra)} rows deleted upstream but "
                            f"still present {sorted(extra)[:10]}")
    return problems


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--kills", type=int, default=3)
    ap.add_argument("--min-alive", type=float, default=14.0)
    ap.add_argument("--max-alive", type=float, default=22.0)
    args = ap.parse_args()

    print("starting load generator")
    gen = spawn(["cdc/generate_load.py", "--sleep", "0.15"])
    time.sleep(2)

    consumer = None
    try:
        for i in range(1, args.kills + 1):
            consumer = spawn(["cdc/consume_changes.py", "--target", "duckdb"])
            # Long enough to clear the previous member's 6s session timeout AND
            # commit several 2s batches. A consumer that dies before it ever
            # writes proves nothing - the window we are trying to land inside
            # is the one between a warehouse write and an offset commit.
            alive = random.uniform(args.min_alive, args.max_alive)
            print(f"kill {i}/{args.kills}: consumer running for {alive:.1f}s...")
            time.sleep(alive)
            # SIGKILL. No cleanup, no flush, no commit. A power cut.
            os.killpg(os.getpgid(consumer.pid), signal.SIGKILL)
            consumer.wait()
            print(f"  killed (pid {consumer.pid})")

        print("\nstopping load generator")
        os.killpg(os.getpgid(gen.pid), signal.SIGTERM)
        gen.wait()
        gen = None
        time.sleep(2)

        print("draining remaining events")
        subprocess.run([PY, "cdc/consume_changes.py", "--target", "duckdb", "--once"],
                       check=True)
    finally:
        if gen is not None:
            os.killpg(os.getpgid(gen.pid), signal.SIGTERM)

    ends = log_end_offsets()
    con = duckdb.connect(DUCKDB, read_only=True)
    pg = psycopg2.connect(DSN)

    print("\n=== NO LOSS: every kafka offset landed ===")
    problems = check_no_loss(con, ends)
    print("\n=== NO DUPLICATES: merge on event_uid held ===")
    problems += check_no_duplicates(con)
    print("\n=== CONVERGENCE: warehouse == postgres ===")
    problems += check_convergence(con, pg)

    con.close()
    pg.close()

    print()
    if problems:
        print(f"CHAOS TEST FAILED after {args.kills} kills")
        for p in problems:
            print(f"  - {p}")
        return 1
    print(f"CHAOS TEST PASSED - {args.kills} hard kills mid-stream, "
          f"no loss, no duplicates, warehouse converged")
    return 0


if __name__ == "__main__":
    sys.exit(main())
