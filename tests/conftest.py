import datetime as dt
import random

import duckdb
import pytest


@pytest.fixture
def con():
    c = duckdb.connect(":memory:")
    yield c
    c.close()


def _row(i, *, fare=12.5, dist=2.4, pu=None, do=None, pu_zone=142, do_zone=236,
         passengers=1, total=None, payment=1, flag="N"):
    pu = pu or dt.datetime(2024, 1, 15, 8, 0, 0) + dt.timedelta(minutes=i)
    do = do or pu + dt.timedelta(minutes=14)
    return (
        2, pu, do, passengers, dist, 1, flag, pu_zone, do_zone, payment,
        fare, 0.5, 0.5, 2.0, 0.0, 0.3,
        total if total is not None else round(fare + 3.3, 2),
        2.5, 0.0, 0.0,
        pu.date(), int((do - pu).total_seconds()), pu.year, pu.month,
        dt.datetime(2024, 4, 1, 0, 0, 0),
    )


RAW_TRIPS_DDL = """
    create table raw_trips (
        vendor_id integer, pickup_datetime timestamp, dropoff_datetime timestamp,
        passenger_count integer, trip_distance double, rate_code_id integer,
        store_and_fwd_flag varchar, pickup_location_id integer,
        dropoff_location_id integer, payment_type integer,
        fare_amount double, extra double, mta_tax double, tip_amount double,
        tolls_amount double, improvement_surcharge double, total_amount double,
        congestion_surcharge double, airport_fee double, cbd_congestion_fee double,
        pickup_date date, trip_duration_seconds bigint,
        pickup_year integer, pickup_month integer, _loaded_at timestamp
    )
"""


@pytest.fixture
def raw_trips(con):
    """50 clean trips across two zones, plus the zone lookup."""
    con.execute(RAW_TRIPS_DDL)
    random.seed(7)
    rows = [_row(i, pu_zone=random.choice([142, 236, 161]),
                 do_zone=random.choice([142, 236, 161])) for i in range(50)]
    con.executemany(
        f"insert into raw_trips values ({', '.join(['?'] * 25)})", rows
    )
    con.execute("""
        create table taxi_zone_lookup as
        select * from (values
            (142, 'Manhattan', 'Lincoln Square East', 'Yellow Zone'),
            (236, 'Manhattan', 'Upper East Side North', 'Yellow Zone'),
            (161, 'Manhattan', 'Midtown Center', 'Yellow Zone')
        ) as t("LocationID", "Borough", "Zone", "service_zone")
    """)
    return con


@pytest.fixture
def make_row():
    return _row
