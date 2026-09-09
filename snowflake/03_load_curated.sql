-- Load curated Parquet from S3 into RAW.
--
-- Timestamps are converted EXPLICITLY rather than by inference. Spark writes
-- them as INT64 microseconds since epoch; MATCH_BY_COLUMN_NAME reads that as
-- seconds and produces years like 39006190 with no error. Verified against the
-- real data: the misread was exactly 1,000,000x.
--
-- TO_TIMESTAMP_NTZ(<value>, 6) states the scale, so nothing has to guess.
--
-- PATTERN excludes Spark's empty _SUCCESS marker, which COPY would otherwise
-- try to parse as Parquet and fail on. ON_ERROR stays ABORT_STATEMENT so a
-- genuinely corrupt file still stops the load loudly.

USE ROLE NYC_PLATFORM_ROLE;
USE WAREHOUSE NYC_WH;
USE DATABASE NYC_PLATFORM;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS RAW_TRIPS (
    vendor_id              NUMBER,
    pickup_datetime        TIMESTAMP_NTZ,
    dropoff_datetime       TIMESTAMP_NTZ,
    passenger_count        NUMBER,
    trip_distance          FLOAT,
    rate_code_id           NUMBER,
    store_and_fwd_flag     VARCHAR,
    pickup_location_id     NUMBER,
    dropoff_location_id    NUMBER,
    payment_type           NUMBER,
    fare_amount            FLOAT,
    extra                  FLOAT,
    mta_tax                FLOAT,
    tip_amount             FLOAT,
    tolls_amount           FLOAT,
    improvement_surcharge  FLOAT,
    total_amount           FLOAT,
    congestion_surcharge   FLOAT,
    airport_fee            FLOAT,
    cbd_congestion_fee     FLOAT,
    pickup_date            DATE,
    trip_duration_seconds  NUMBER,
    _loaded_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO RAW_TRIPS (
    vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
    rate_code_id, store_and_fwd_flag, pickup_location_id, dropoff_location_id,
    payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee,
    cbd_congestion_fee, pickup_date, trip_duration_seconds
)
FROM (
    SELECT
        $1:vendor_id::NUMBER,
        TO_TIMESTAMP_NTZ($1:pickup_datetime::NUMBER, 6),
        TO_TIMESTAMP_NTZ($1:dropoff_datetime::NUMBER, 6),
        $1:passenger_count::NUMBER,
        $1:trip_distance::FLOAT,
        $1:rate_code_id::NUMBER,
        $1:store_and_fwd_flag::VARCHAR,
        $1:pickup_location_id::NUMBER,
        $1:dropoff_location_id::NUMBER,
        $1:payment_type::NUMBER,
        $1:fare_amount::FLOAT,
        $1:extra::FLOAT,
        $1:mta_tax::FLOAT,
        $1:tip_amount::FLOAT,
        $1:tolls_amount::FLOAT,
        $1:improvement_surcharge::FLOAT,
        $1:total_amount::FLOAT,
        $1:congestion_surcharge::FLOAT,
        $1:airport_fee::FLOAT,
        $1:cbd_congestion_fee::FLOAT,
        $1:pickup_date::DATE,
        $1:trip_duration_seconds::NUMBER
    FROM @CURATED_STAGE
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*[.]parquet'
ON_ERROR = ABORT_STATEMENT;

-- Correctness check: recompute duration from the stored timestamps and compare
-- against what Spark calculated before the write. Zero means they round-tripped.
SELECT
    count(*)             AS rows_loaded,
    min(pickup_datetime) AS earliest,
    max(pickup_datetime) AS latest,
    count_if(DATEDIFF(second, pickup_datetime, dropoff_datetime)
             <> trip_duration_seconds) AS duration_mismatches
FROM RAW_TRIPS;
