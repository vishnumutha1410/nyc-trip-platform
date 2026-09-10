"""Look at a raw TLC file using the same quality rules the Spark job applies.

The rules come from spark/jobs/rules.py - the exact strings Spark uses. So the
quarantine rate printed here is the real one, not an estimate.
"""
import sys

import duckdb

from spark.jobs.rules import failure_reason_expression, valid_row_expression

RAW = sys.argv[1] if len(sys.argv) > 1 else "data/raw/yellow_tripdata_2024-01.parquet"

con = duckdb.connect()
con.execute(f"""
    CREATE VIEW trips AS
    SELECT
        tpep_pickup_datetime  AS pickup_datetime,
        tpep_dropoff_datetime AS dropoff_datetime,
        passenger_count,
        trip_distance,
        "PULocationID"        AS pickup_location_id,
        "DOLocationID"        AS dropoff_location_id,
        fare_amount,
        total_amount
    FROM read_parquet('{RAW}')
""")

total = con.execute("SELECT count(*) FROM trips").fetchone()[0]
lo, hi = con.execute(
    "SELECT min(pickup_datetime), max(pickup_datetime) FROM trips"
).fetchone()
good = con.execute(
    f"SELECT count(*) FROM trips WHERE {valid_row_expression()}"
).fetchone()[0]
bad = total - good

print(f"\nfile          : {RAW}")
print(f"rows          : {total:,}")
print(f"pickup range  : {lo}  ->  {hi}")
print(f"clean         : {good:,}")
print(f"quarantined   : {bad:,}  ({bad / total:.2%})\n")

print("why rows fail:")
for reason, n in con.execute(f"""
    SELECT {failure_reason_expression()} AS reason, count(*) AS n
    FROM trips
    WHERE NOT ({valid_row_expression()})
    GROUP BY 1 ORDER BY n DESC
""").fetchall():
    print(f"  {reason:<24} {n:>9,}")

print("\npickup months present in this file:")
for month, n in con.execute("""
    SELECT date_trunc('month', pickup_datetime) AS m, count(*) AS n
    FROM trips GROUP BY 1 ORDER BY n DESC LIMIT 8
""").fetchall():
    print(f"  {str(month)[:7]}  {n:>10,}")
