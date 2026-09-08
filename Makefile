.PHONY: help setup download curate load dbt test lint all

help:
	@echo "make setup     - install python deps"
	@echo "make download  - fetch TLC months and upload to S3 raw"
	@echo "make curate    - run the PySpark curation job"
	@echo "make load      - COPY curated parquet into Snowflake"
	@echo "make dbt       - build the star schema"
	@echo "make test      - run the test suite"
	@echo "make all       - download -> curate -> load -> dbt"

setup:
	pip install -r requirements.txt

download:
	python -m ingest.download_tlc

curate:
	python -m spark.jobs.curate_trips

load:
	python -m warehouse.load_to_snowflake

dbt:
	cd dbt && dbt deps && dbt build

test:
	pytest -v

lint:
	ruff check .

all: download curate load dbt
