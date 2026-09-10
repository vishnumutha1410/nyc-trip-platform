"""PySpark curation job: raw TLC Parquet -> conformed, cleaned, partitioned.

Four things happen here, and each one is a decision worth defending:

1. SCHEMA CONFORMANCE. The TLC schema changed over the years - airport_fee
   arrived in 2022, cbd_congestion_fee in 2025, and casing is inconsistent
   (VendorID vs vendorid). We normalise every column to snake_case and add
   missing columns as typed nulls, so five years of files become one table.

2. ROW-LOCAL QUALITY RULES. Ten predicates from spark/jobs/rules.py, shared
   verbatim with the DuckDB test suite so the documented rule is provably the
   shipped rule.

3. SOURCE-MONTH ALIGNMENT. A check the row-local rules structurally cannot do.
   A trip dated 2026-06 is perfectly plausible on its own; it is only wrong
   because it arrived in a file of 2024-01 data. The source month lives in the
   raw path's Hive partition, so only the job can see it.

4. QUARANTINE, NOT DROP. Rows failing any check are written to a separate
   dataset with the reason attached. Dropping them silently is how you end up
   unable to explain why your row count fell.
"""
from __future__ import annotations

import os
import sys

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from spark.jobs.rules import failure_reason_expression

load_dotenv()

# Canonical snake_case names -> the many spellings the TLC has used.
COLUMN_MAP = {
    "vendor_id": ["VendorID", "vendorid", "vendor_id"],
    "pickup_datetime": ["tpep_pickup_datetime", "Trip_Pickup_DateTime"],
    "dropoff_datetime": ["tpep_dropoff_datetime", "Trip_Dropoff_DateTime"],
    "passenger_count": ["passenger_count", "Passenger_Count"],
    "trip_distance": ["trip_distance", "Trip_Distance"],
    "rate_code_id": ["RatecodeID", "ratecodeid", "rate_code_id"],
    "store_and_fwd_flag": ["store_and_fwd_flag"],
    "pickup_location_id": ["PULocationID", "pulocationid"],
    "dropoff_location_id": ["DOLocationID", "dolocationid"],
    "payment_type": ["payment_type", "Payment_Type"],
    "fare_amount": ["fare_amount", "Fare_Amt"],
    "extra": ["extra"],
    "mta_tax": ["mta_tax"],
    "tip_amount": ["tip_amount", "Tip_Amt"],
    "tolls_amount": ["tolls_amount", "Tolls_Amt"],
    "improvement_surcharge": ["improvement_surcharge"],
    "total_amount": ["total_amount", "Total_Amt"],
    "congestion_surcharge": ["congestion_surcharge"],
    "airport_fee": ["airport_fee", "Airport_fee"],
    "cbd_congestion_fee": ["cbd_congestion_fee"],
}

# Columns that simply do not exist in older files. Added as typed nulls.
OPTIONAL_NUMERIC = ["congestion_surcharge", "airport_fee", "cbd_congestion_fee"]

# How far outside its source month a pickup may legitimately fall.
#
# Not zero: a trip that starts at 23:58 on the last day of a month is genuinely
# reported in that month's file but belongs in the next month's partition, and
# TLC files routinely contain a handful of these. Three days is comfortably
# past any real spill while still catching rows that are years adrift.
SOURCE_MONTH_TOLERANCE_DAYS = 3


def build_spark() -> SparkSession:
    """Local Spark tuned for an 8GB laptop.

    spark.local.dir matters more than it looks: the default is /tmp, which on
    WSL and in many container images is a small tmpfs backed by RAM. A shuffle
    of any size fills it and the job dies with "No space left on device" while
    the real disk sits empty.
    """
    mem = os.getenv("SPARK_DRIVER_MEMORY", "3g")
    return (
        SparkSession.builder.appName("curate_trips")
        .master("local[*]")
        .config("spark.driver.memory", mem)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Spark defaults to INT96 timestamps, a deprecated format that
        # Snowflake misreads. TIMESTAMP_MICROS is the modern standard.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.local.dir", os.path.expanduser(os.getenv("SPARK_LOCAL_DIR", "~/spark-tmp")))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .getOrCreate()
    )


def conform_schema(df: DataFrame) -> DataFrame:
    """Rename to snake_case, add missing optional columns, keep source partition.

    `year` and `month` come from the raw path's Hive partitioning
    (raw/yellow/year=2024/month=01/). They are carried through as
    source_year / source_month so the alignment check can use them, then
    dropped before the write.
    """
    existing = {c.lower(): c for c in df.columns}
    selects = []
    for canonical, aliases in COLUMN_MAP.items():
        found = next((existing[a.lower()] for a in aliases if a.lower() in existing), None)
        if found:
            selects.append(F.col(f"`{found}`").alias(canonical))
        elif canonical in OPTIONAL_NUMERIC:
            selects.append(F.lit(None).cast(DoubleType()).alias(canonical))
        else:
            raise ValueError(
                f"required column {canonical!r} not found. "
                f"file has: {sorted(df.columns)}"
            )

    for part_col in ("year", "month"):
        if part_col in existing:
            selects.append(F.col(existing[part_col]).alias(f"source_{part_col}"))
        else:
            raise ValueError(
                f"partition column {part_col!r} missing - is the raw path "
                f"partitioned as year=YYYY/month=MM?"
            )

    return df.select(*selects)


def source_month_window(df: DataFrame) -> tuple:
    """First and last valid pickup date for the file each row came from."""
    month_start = F.to_date(
        F.concat_ws(
            "-",
            F.col("source_year").cast("string"),
            F.lpad(F.col("source_month").cast("string"), 2, "0"),
            F.lit("01"),
        )
    )
    month_end = F.last_day(month_start)
    return (
        F.date_sub(month_start, SOURCE_MONTH_TOLERANCE_DAYS),
        F.date_add(month_end, SOURCE_MONTH_TOLERANCE_DAYS),
    )


def split_valid_and_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Partition rows into clean and quarantined, keeping the failure reason.

    The reason column is the single source of truth: a row is valid exactly
    when no check named it. That is simpler than maintaining a separate
    "is valid" predicate that has to be kept in step with the reasons.
    """
    lo, hi = source_month_window(df)
    pickup_date = F.to_date(F.col("pickup_datetime"))

    flagged = df.withColumn(
        "_quality_failure",
        F.when(
            pickup_date.isNull() | (pickup_date < lo) | (pickup_date > hi),
            F.lit("pickup_outside_source_month"),
        ).otherwise(F.expr(failure_reason_expression())),
    )

    drop_cols = ["_quality_failure", "source_year", "source_month"]
    valid = flagged.filter(F.col("_quality_failure").isNull()).drop(*drop_cols)
    quarantine = flagged.filter(F.col("_quality_failure").isNotNull())
    return valid, quarantine


def add_partitions_and_derived(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("pickup_year", F.year("pickup_datetime"))
          .withColumn("pickup_month", F.month("pickup_datetime"))
          .withColumn("pickup_date", F.to_date("pickup_datetime"))
          .withColumn(
              "trip_duration_seconds",
              F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime"),
          )
    )


def main() -> None:
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        sys.exit("S3_BUCKET is not set.")

    raw_path = f"s3a://{bucket}/raw/yellow/"
    curated_path = f"s3a://{bucket}/curated/trips/"
    quarantine_path = f"s3a://{bucket}/quarantine/trips/"

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"reading {raw_path}")
    raw = spark.read.option("mergeSchema", "true").parquet(raw_path)
    total_in = raw.count()

    conformed = conform_schema(raw)
    valid, quarantine = split_valid_and_quarantine(conformed)
    valid = add_partitions_and_derived(valid)

    n_valid = valid.count()
    n_quarantine = quarantine.count()

    # repartition by the same columns we partition on, so each output
    # partition is written by exactly one task = one file instead of N.
    (valid.repartition("pickup_year", "pickup_month")
          .write.mode("overwrite")
          .partitionBy("pickup_year", "pickup_month")
          .parquet(curated_path))
    if n_quarantine:
        quarantine.write.mode("overwrite").parquet(quarantine_path)

    print("\n--- curation summary ---")
    print(f"rows in          : {total_in:,}")
    print(f"rows curated     : {n_valid:,}")
    print(f"rows quarantined : {n_quarantine:,} ({n_quarantine / total_in:.2%})")
    print("\nquarantine reasons:")
    (quarantine.groupBy("_quality_failure").count()
               .orderBy(F.desc("count")).show(truncate=False))
    # Read the distribution back from what we just wrote rather than calling
    # another action on `valid`. Spark re-runs the whole DAG for every action
    # on an uncached DataFrame, so printing this from `valid` meant a fourth
    # full pass over 41M rows. Parquet keeps row counts in the file footers,
    # so counting the written output is nearly free.
    print("output partitions:")
    (spark.read.parquet(curated_path)
          .groupBy("pickup_year", "pickup_month").count()
          .orderBy("pickup_year", "pickup_month").show(50, truncate=False))
    print(f"\ncurated -> {curated_path}")
    spark.stop()


if __name__ == "__main__":
    main()
