"""Download NYC TLC yellow-taxi Parquet files and land them in S3 as-is.

The raw layer is deliberately untouched: same bytes the TLC published, in a
partitioned prefix. If a downstream job is wrong you re-run it against raw
rather than re-downloading, and you can always prove what the source said.
"""
from __future__ import annotations

import os
import pathlib
import sys

import boto3
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
LOCAL_DIR = pathlib.Path("data/raw")
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def months() -> list[str]:
    raw = os.getenv("TLC_MONTHS", "2024-01")
    return [m.strip() for m in raw.split(",") if m.strip()]


def download_month(month: str) -> pathlib.Path:
    """Fetch one monthly file. Skips if already present locally."""
    name = f"yellow_tripdata_{month}.parquet"
    dest = LOCAL_DIR / name
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        print(f"  {name} already downloaded ({dest.stat().st_size / 1e6:.1f} MB)")
        return dest

    url = f"{BASE_URL}/{name}"
    print(f"  downloading {url}")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with dest.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    print(f"  saved {dest} ({dest.stat().st_size / 1e6:.1f} MB)")
    return dest


def download_zone_lookup() -> pathlib.Path:
    dest = pathlib.Path("dbt/seeds/taxi_zone_lookup.csv")
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        print("  zone lookup already present")
        return dest
    print(f"  downloading {ZONE_LOOKUP_URL}")
    r = requests.get(ZONE_LOOKUP_URL, timeout=60)
    r.raise_for_status()
    dest.write_bytes(r.content)
    print(f"  saved {dest}")
    return dest


def upload(path: pathlib.Path, month: str) -> str:
    """Upload to s3://bucket/raw/yellow/year=YYYY/month=MM/<file>."""
    bucket = os.environ["S3_BUCKET"]
    year, mm = month.split("-")
    key = f"raw/yellow/year={year}/month={mm}/{path.name}"
    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
    s3.upload_file(str(path), bucket, key)
    uri = f"s3://{bucket}/{key}"
    print(f"  uploaded -> {uri}")
    return uri


def main() -> None:
    if not os.getenv("S3_BUCKET"):
        sys.exit("S3_BUCKET is not set. Copy .env.example to .env and fill it in.")

    print("Zone lookup:")
    download_zone_lookup()

    for month in months():
        print(f"\n{month}:")
        local = download_month(month)
        upload(local, month)

    print("\nRaw layer loaded. Next: make curate")


if __name__ == "__main__":
    main()
