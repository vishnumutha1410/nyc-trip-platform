"""Prove the warehouse agrees with the source database.

A pipeline that runs without errors is not a pipeline that is correct. Every
defect worth catching in phase 1 was found the same way: by computing the same
number two different ways and comparing. This does that for CDC.

The warehouse holds an append-only log of change events, not rows. To compare
it against Postgres you first have to collapse the log back into current state:
for each primary key, take the event with the highest LSN, and if that event is
a delete, the row does not exist.

    LSN, not timestamp. Two changes committed in the same millisecond share a
    ts_ms, and wall clocks are not monotonic. The log sequence number is the
    database's own total order over commits, and it is the only safe tiebreaker.

Run the load generator, run the consumer, stop both, then:

    python cdc/reconcile.py
"""
from __future__ import annotations

import json
import os
import sys

import duckdb
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DSN = os.getenv("ORDERS_DSN",
                "host=localhost port=5432 dbname=ordersdb user=orders password=orders")
DUCKDB = os.getenv("CDC_DUCKDB", "warehouse/cdc.duckdb")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
GROUP_ID = os.getenv("CDC_GROUP_ID", "cdc-warehouse-loader")
TOPICS = ["ordersdb.public.customers", "ordersdb.public.orders"]

# Collapse the event log to current state. qualify picks the latest event per
# key; the where clause drops keys whose latest event was a delete.
CURRENT_STATE = """
with latest as (
    select
        source_table,
        record_key,
        op,
        after_image,
        row_number() over (
            partition by source_table, record_key order by lsn desc
        ) as rn
    from cdc_events
    where source_table = ?
)
select record_key, after_image
from latest
where rn = 1 and op <> 'd'
"""


def warehouse_state(con, table: str) -> dict:
    rows = con.execute(CURRENT_STATE, [table]).fetchall()
    return {k: v for k, v in rows}


def assert_caught_up() -> None:
    """Refuse to compare while the consumer is behind.

    A check that can go red for reasons unrelated to correctness gets ignored
    the third time someone sees it fail. Consumer lag is exactly that: compare
    a live source against a lagging target and you get a confident MISMATCH
    that means nothing. Better to say "I cannot answer that yet" than to answer
    it wrongly.
    """
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP_ID,
                  "enable.auto.commit": False})
    try:
        tps = [TopicPartition(t, 0) for t in TOPICS]
        committed = c.committed(tps, timeout=10)
        behind = []
        for tp in committed:
            end = c.get_watermark_offsets(TopicPartition(tp.topic, 0), timeout=10)[1]
            pos = tp.offset if tp.offset >= 0 else 0
            if end - pos > 0:
                behind.append(f"{tp.topic}: {end - pos} events behind")
        if behind:
            print("consumer is not caught up - reconciliation would be meaningless:")
            for b in behind:
                print(f"  {b}")
            print("\nstop the load generator, run `make -f Makefile.cdc "
                  "cdc-consume-once`, then try again.")
            sys.exit(2)
    finally:
        c.close()


def main() -> int:
    if not os.path.exists(DUCKDB):
        sys.exit(f"{DUCKDB} does not exist - run the consumer first.")

    assert_caught_up()

    con = duckdb.connect(DUCKDB, read_only=True)
    pg = psycopg2.connect(DSN)

    failures = 0
    for table, pk in (("customers", "customer_id"), ("orders", "order_id")):
        with pg.cursor() as cur:
            cur.execute(f"SELECT {pk} FROM {table}")
            source_keys = {row[0] for row in cur.fetchall()}

        wh = warehouse_state(con, table)
        # record_key is Debezium's key JSON, e.g. {"customer_id": 1}
        wh_keys = {json.loads(k)[pk] for k in wh}

        missing = source_keys - wh_keys      # in Postgres, absent downstream
        extra = wh_keys - source_keys        # deleted upstream, still downstream

        total_events = con.execute(
            "select count(*) from cdc_events where source_table = ?", [table]
        ).fetchone()[0]
        distinct_uids = con.execute(
            "select count(distinct event_uid) from cdc_events where source_table = ?",
            [table],
        ).fetchone()[0]

        status = "OK" if not missing and not extra else "MISMATCH"
        if status == "MISMATCH":
            failures += 1

        print(f"\n{table}")
        print(f"  events landed        : {total_events:,}")
        print(f"  distinct event_uids  : {distinct_uids:,}"
              f"{'   <- duplicates present!' if distinct_uids != total_events else ''}")
        print(f"  rows in postgres     : {len(source_keys):,}")
        print(f"  rows in warehouse    : {len(wh_keys):,}")
        print(f"  missing downstream   : {len(missing)}"
              f"{'  ' + str(sorted(missing)[:10]) if missing else ''}")
        print(f"  not deleted downstream: {len(extra)}"
              f"{'  ' + str(sorted(extra)[:10]) if extra else ''}")
        print(f"  -> {status}")

    con.close()
    pg.close()

    print()
    if failures:
        print("RECONCILIATION FAILED")
        return 1
    print("RECONCILIATION PASSED - warehouse current state matches postgres")
    return 0


if __name__ == "__main__":
    sys.exit(main())
