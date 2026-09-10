"""Continuous write traffic against the orders database.

This is the "application" - the thing that has no idea a data pipeline exists.
It inserts orders, walks them through a lifecycle, mutates customers, and
occasionally deletes. Everything downstream has to discover all of that from
the WAL alone.

The mix is deliberate:

  INSERT order        a brand new fact
  UPDATE order status pending -> paid -> shipped -> delivered
  UPDATE customer     tier upgrade or a move to a new city   <- SCD Type 2 fuel
  DELETE order        cancelled; proves you handle deletes, which is where
                      most homemade "incremental load on updated_at" pipelines
                      quietly go wrong: a deleted row has no updated_at to see.

Run:  python cdc/generate_load.py            (forever, ~2 changes/sec)
      python cdc/generate_load.py --n 50     (50 changes then stop)
      python cdc/generate_load.py --burst    (as fast as it will go)
"""
from __future__ import annotations

import argparse
import os
import random
import sys
import time

import psycopg2
import psycopg2.extras

DSN = os.getenv(
    "ORDERS_DSN",
    "host=localhost port=5432 dbname=ordersdb user=orders password=orders",
)

STATUS_FLOW = {
    "pending": "paid",
    "paid": "shipped",
    "shipped": "delivered",
}
TIERS = ["bronze", "silver", "gold", "platinum"]
CITIES = ["Brooklyn", "Queens", "Manhattan", "Bronx", "Jersey City",
          "Hoboken", "Staten Island", "Yonkers"]


def insert_order(cur) -> str:
    cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
    customer_id = cur.fetchone()[0]
    amount = round(random.uniform(8.0, 340.0), 2)
    cur.execute(
        "INSERT INTO orders (customer_id, status, amount) "
        "VALUES (%s, 'pending', %s) RETURNING order_id",
        (customer_id, amount),
    )
    return f"INSERT order {cur.fetchone()[0]} (${amount})"


def advance_order(cur) -> str | None:
    """Move one order one step along its lifecycle."""
    cur.execute(
        "SELECT order_id, status FROM orders "
        "WHERE status <> 'delivered' ORDER BY random() LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        return None
    order_id, status = row
    nxt = STATUS_FLOW[status]
    cur.execute(
        "UPDATE orders SET status = %s, updated_at = now() WHERE order_id = %s",
        (nxt, order_id),
    )
    return f"UPDATE order {order_id}: {status} -> {nxt}"


def mutate_customer(cur) -> str:
    """The change that makes SCD Type 2 worth building.

    A customer's tier and city are slowly changing attributes. If you only ever
    keep the current value, every historical order silently gets re-attributed
    to the customer's new tier, and last quarter's "gold tier revenue" changes
    every time you rerun the report.
    """
    cur.execute("SELECT customer_id, tier, city FROM customers ORDER BY random() LIMIT 1")
    customer_id, tier, city = cur.fetchone()
    if random.random() < 0.5:
        new = random.choice([t for t in TIERS if t != tier])
        cur.execute(
            "UPDATE customers SET tier = %s, updated_at = now() WHERE customer_id = %s",
            (new, customer_id),
        )
        return f"UPDATE customer {customer_id}: tier {tier} -> {new}"
    new = random.choice([c for c in CITIES if c != city])
    cur.execute(
        "UPDATE customers SET city = %s, updated_at = now() WHERE customer_id = %s",
        (new, customer_id),
    )
    return f"UPDATE customer {customer_id}: city {city} -> {new}"


def delete_order(cur) -> str | None:
    cur.execute(
        "SELECT order_id FROM orders WHERE status = 'pending' ORDER BY random() LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        return None
    cur.execute("DELETE FROM orders WHERE order_id = %s", (row[0],))
    return f"DELETE order {row[0]}"


# (weight, function). Inserts dominate; deletes are rare but never zero.
ACTIONS = [
    (45, insert_order),
    (35, advance_order),
    (15, mutate_customer),
    (5,  delete_order),
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=0, help="stop after N changes (0 = forever)")
    ap.add_argument("--burst", action="store_true", help="no sleep between changes")
    ap.add_argument("--sleep", type=float, default=0.5, help="seconds between changes")
    args = ap.parse_args()

    conn = psycopg2.connect(DSN)
    conn.autocommit = True  # every statement is its own transaction, so every
                            # change reaches the WAL immediately
    weights = [w for w, _ in ACTIONS]
    fns = [f for _, f in ACTIONS]

    made = 0
    try:
        with conn.cursor() as cur:
            while args.n == 0 or made < args.n:
                fn = random.choices(fns, weights=weights, k=1)[0]
                msg = fn(cur)
                if msg is None:
                    continue
                made += 1
                print(f"[{made:>6}] {msg}", flush=True)
                if not args.burst:
                    time.sleep(args.sleep)
    except KeyboardInterrupt:
        print(f"\nstopped after {made} changes")
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
