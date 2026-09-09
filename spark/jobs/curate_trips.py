"""PySpark curation job: raw TLC Parquet -> conformed, cleaned, partitioned.

Three things happen here, and each one is a decision worth defending:

1. SCHEMA CONFORMANCE. The TLC schema changed over the years - airport_fee
   arrived in 2022, cbd_congestion_fee in 2025, and casing is inconsistent
   (VendorID vs vendorid). We normalise every column to snake_case and add
   missing columns as typed nulls, so five years of files become one table.

2. QUARANTINE, NOT DROP. Rows failing a quality rule are written to a separate
   quarantine dataset with the reason attached. Dropping them silently is how
   you end up unable to explain why your row count fell.

3. PARTITIONING. Output is partitioned by pickup year and month, which is how
   every downstream query filters. Aim for files in the hundreds of MB.
"""
from __future__ import annotations

import os
import sys

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from spark.jobs.rules import failure_reason_expression, valid_row_expression

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


def build_spark() -> SparkSession:
    """Local Spark tuned for an 8GB laptop.

    adaptive query execution on: lets Spark coalesce shuffle partitions at
    runtime instead of blindly using 200, which is what produces the
    small-file problem on a laptop-sized dataset.
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
    """Rename to snake_case and add missing optional columns as typed nulls."""
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
    return df.select(*selects)


def split_valid_and_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Partition rows into clean and quarantined, keeping the reason."""
    flagged = df.withColumn("_quality_failure", F.expr(failure_reason_expression()))
    valid = flagged.filter(F.expr(valid_row_expression())).drop("_quality_failure")
    quarantine = flagged.filter(~F.expr(valid_row_expression()))
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

    (valid.repartition("pickup_year", "pickup_month").write.mode("overwrite")
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
    print(f"\ncurated -> {curated_path}")
    spark.stop()


if __name__ == "__main__":
    main()
