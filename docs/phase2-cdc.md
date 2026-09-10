# Phase 2 — Change Data Capture

Phase 1 was batch: whole files land in S3, Spark rewrites them, Snowflake loads
them, dbt models them. Everything was reprocessable because everything was
re-readable.

Phase 2 is the opposite problem. There is an operational Postgres database that
nobody will let you touch. Rows change in it constantly. Nobody records what
they used to be. Your job is to build a warehouse view of that database that is
correct, current, and keeps history — without adding a single column to their
schema and without running `SELECT *` against their production box every hour.

---

## Why not just poll?

The pipeline almost everyone builds first is:

```sql
SELECT * FROM orders WHERE updated_at > :last_run
```

It is easy, it works in a demo, and it is wrong in four separate ways.

| Problem | What actually happens |
|---|---|
| Deletes are invisible | A deleted row has no `updated_at` to be greater than. It just stops appearing, and your warehouse keeps it forever. |
| Intermediate states are lost | If an order goes `pending → paid → shipped` between two polls, you see `shipped` and the middle states never existed as far as you know. |
| It needs a column you don't own | `updated_at` only exists if the application team maintains it on every write path. One `UPDATE` that forgets it and the row is invisible to you forever. |
| It hammers the source | Every poll is a real query against the production database, and the interval is a straight trade between freshness and load. |

CDC fixes all four by reading the database's own write-ahead log — the thing
Postgres already writes for crash recovery and replication. Every insert,
update and delete is in there, in commit order, with no cooperation from the
application at all. You are not querying the database; you are reading a log it
was writing anyway.

---

## The stack, and what each piece is actually for

**Postgres 16** — the source. The only unusual thing about it is
`wal_level=logical`. At the default (`replica`) the WAL contains enough to
restore a byte-identical copy of the database, but not enough to reconstruct
*row-level changes* in a readable form. `logical` adds that. Without this one
setting there is no CDC.

**Debezium** — the reader. It connects to Postgres as a replication client,
creates a *replication slot*, and turns WAL entries into JSON change events.

> Why Debezium instead of writing your own? You could read the WAL yourself with
> `psycopg2`'s replication support in about 200 lines. What you would then spend
> two months on is: taking a consistent initial snapshot while the stream keeps
> moving, resuming from an exact LSN after a crash, handling DDL changes,
> managing slot lifecycle so the WAL does not fill the disk. Debezium is those
> two months. This is the argument to make when an interviewer asks why you
> reached for it.

**Kafka Connect** — the process that runs Debezium. Debezium is a Connect
*plugin*, not a standalone program. Connect handles the parts that are boring
and hard: restarting failed tasks, storing connector configs, and — the
important one — persisting offsets so a restarted connector resumes where it
stopped rather than replaying from the beginning.

**Redpanda** — the log Debezium writes to. It speaks the Kafka API, so every
Kafka client, tool and piece of documentation applies unchanged.

> Why Redpanda instead of Kafka? Purely operational. Kafka wants a broker plus
> either ZooKeeper or KRaft configuration, and comfortably 2GB of JVM heap.
> Redpanda is a single C++ binary with no JVM and no coordination service, and
> runs happily in 1GB. Same protocol, quarter of the memory. On an 8GB laptop
> already running Spark, that is the difference between the stack booting and
> the stack swapping. Everything you learn here is Kafka knowledge.

**Redpanda Console** — a web UI on `localhost:8080` for browsing topics and
messages. Not part of the pipeline; it is how you *see* the pipeline, which
matters enormously when you are learning what a change event looks like.

---

## Two decisions in the config worth defending

**`REPLICA IDENTITY FULL`** (in `cdc/init/01_schema.sql`)

By default, an `UPDATE` writes only the primary key of the old row into the
WAL. So Debezium can tell you the row changed and what it changed *to*, but the
`before` image is mostly nulls:

```
before: { "order_id": 41, "status": null, "amount": null }
after:  { "order_id": 41, "status": "paid", "amount": 88.10 }
```

`FULL` logs the entire old row instead. It costs WAL volume — that is why it is
not the default, and why a real team would argue about it — but "what was the
value before this change?" is precisely the question SCD Type 2 history is
built from. You cannot build it from nulls.

**No `ExtractNewRecordState` transform**

Debezium ships a single-message transform that flattens each event down to just
the `after` row, which makes messages look like ordinary table rows and is very
tempting. It also throws away the `before` image — the thing you just paid WAL
volume for. This project keeps the full envelope:

```json
{
  "before": { ... } | null,
  "after":  { ... } | null,
  "source": { "lsn": 24304592, "ts_ms": 1757455112000, "table": "orders", ... },
  "op": "c" | "u" | "d" | "r",
  "ts_ms": 1757455112094
}
```

`op` is the operation: `c`reate, `u`pdate, `d`elete, and `r` for a row that
came from the initial snapshot rather than the live stream. `source.lsn` is the
log sequence number — the position in the WAL where this change was committed.

**LSN is the ordering key, not the timestamp.** Two changes committed in the
same millisecond have the same `ts_ms` but different LSNs, and clocks are not
monotonic anyway. Any time this pipeline needs to know which of two versions of
a row is newer, it uses the LSN. That single sentence is the answer to a common
interview question about CDC ordering.

---

## Running it

Everything below is from `~/nyc-trip-platform` with the venv active.

### 1. Start the stack

```bash
make -f Makefile.cdc cdc-up
```

Four containers come up. The first pull is a few hundred MB. When it finishes
you should be able to open <http://localhost:8080> and see an empty topic list.

Check them:

```bash
docker compose ps
```

All four should say `running`; postgres and redpanda should say `healthy`.

### 2. Confirm logical replication is actually on

```bash
docker exec -it cdc-postgres psql -U orders -d ordersdb -c "SHOW wal_level;"
```

Must print `logical`. If it prints `replica`, the `command:` block in
`docker-compose.yml` did not take effect and nothing after this will work.

While you are here, look at the publication that the init script created:

```bash
docker exec -it cdc-postgres psql -U orders -d ordersdb -c \
  "SELECT * FROM pg_publication_tables;"
```

Two rows: `customers` and `orders`.

### 3. Register the connector

```bash
make -f Makefile.cdc cdc-register
```

This POSTs `cdc/connectors/postgres-orders.json` to the Connect REST API.
Then:

```bash
make -f Makefile.cdc cdc-status
```

You want `"state": "RUNNING"` for both the connector and its task. If a task is
`FAILED`, the JSON contains the full Java stack trace — read the first
`Caused by:` line, it is almost always the real cause.

### 4. Watch the snapshot arrive

`snapshot.mode: initial` means Debezium's first act is to read every existing
row of both tables and emit it as an event with `op: "r"`. Ten customers were
seeded, so:

```bash
make -f Makefile.cdc cdc-watch-customers
```

Ten messages, each with `before: null` and `op: "r"`. This is the bootstrap
that makes CDC complete rather than "everything from now on" — without it your
warehouse would only ever know about rows that happened to change after you
plugged in.

Leave that consumer running.

### 5. Make a change and watch it appear

New terminal:

```bash
docker exec -it cdc-postgres psql -U orders -d ordersdb -c \
  "UPDATE customers SET tier = 'platinum' WHERE customer_id = 1;"
```

Within a second a new message shows up in the consumer, with `op: "u"`, a
populated `before` showing the old tier, and an `after` showing `platinum`.

**That is the milestone.** A change made in a database, captured from its
write-ahead log by a process that database knows nothing about, and delivered
to a durable log a second later — with the previous value intact.

### 6. Turn on the traffic

```bash
pip install psycopg2-binary
make -f Makefile.cdc cdc-load
```

Roughly two changes a second: new orders, status transitions, customer tier and
city changes, and the occasional delete. Leave it running and watch the topics
fill.

### 7. Look at the replication slot

```bash
make -f Makefile.cdc cdc-slot
```

`restart_lsn` is how far Debezium has confirmed it has processed.
`retained_wal` is how much WAL Postgres is holding on to on that slot's behalf.

Now stop Connect and watch that number grow:

```bash
docker compose stop connect
# wait a minute, run cdc-slot again
```

This is the sharp edge of CDC and worth understanding before you ever put one
in production: **an inactive replication slot pins the WAL forever.** Postgres
will not recycle log segments a slot still needs, so a connector that has been
down since Friday can fill the source database's disk by Monday and take
production with it. Deleting a Debezium connector does *not* drop its slot; you
have to do that yourself:

```sql
SELECT pg_drop_replication_slot('dbz_orders_slot');
```

Start it again and watch the retained WAL collapse back:

```bash
docker compose start connect
```

---

## What's next

The stream exists. It is not yet a warehouse. Still to build:

- **A consumer** that reads the topics and lands events in Snowflake, with
  bounded retry, a dead-letter queue for messages it cannot handle, and offsets
  committed only *after* a successful write — which makes it at-least-once, and
  therefore makes idempotency downstream non-negotiable.
- **SCD Type 2 dimensions** in dbt, built from the `before`/`after` pairs, with
  `valid_from` / `valid_to` / `is_current` and LSN as the tiebreaker.
- **A chaos test**: kill the consumer mid-stream, restart it, and prove with a
  reconciliation query that no change was lost and no row was double-counted.
  That test is the actual portfolio piece. Anyone can wire Debezium up; being
  able to demonstrate the failure behaviour is the part that reads as
  experience.
