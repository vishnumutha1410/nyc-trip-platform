"""Execute the Snowflake COPY INTO from Python so `make load` works end to end."""
from __future__ import annotations

import os
import pathlib
import sys

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

SQL_FILE = pathlib.Path("snowflake/03_load_curated.sql")


def statements(sql: str):
    """Split a script into statements, ignoring comment-only fragments."""
    for raw in sql.split(";"):
        stmt = "\n".join(
            line for line in raw.splitlines() if not line.strip().startswith("--")
        ).strip()
        if stmt:
            yield stmt


def main() -> None:
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        sys.exit(f"missing env vars: {', '.join(missing)}")

    con = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.getenv("SNOWFLAKE_ROLE", "NYC_PLATFORM_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "NYC_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "NYC_PLATFORM"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    )
    try:
        cur = con.cursor()
        for stmt in statements(SQL_FILE.read_text()):
            print(f"\n> {stmt.splitlines()[0][:90]}...")
            cur.execute(stmt)
            rows = cur.fetchall()
            if rows:
                for row in rows[:5]:
                    print(f"  {row}")
    finally:
        con.close()
    print("\nLoaded. Next: make dbt")


if __name__ == "__main__":
    main()
