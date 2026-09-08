"""Execute the dbt model SQL in DuckDB and assert the star schema behaves.

Catches the mistakes that actually happen in dimensional modelling: a join
that fans out and inflates your measures, a broken grain, a foreign key with
no match in its dimension.
"""
from tests.dbt_render import model, render


def build_star(con):
    """Materialise stg_trips, dim_zone and fct_trips from the real model files."""
    stg = render(model("stg_trips"), {"raw_trips": "raw_trips"})
    con.execute(f"create table stg_trips as {stg}")

    dim = render(model("dim_zone"), {"taxi_zone_lookup": "taxi_zone_lookup"})
    con.execute(f"create table dim_zone as {dim}")

    fct = render(model("fct_trips"), {"stg_trips": "stg_trips"})
    con.execute(f"create table fct_trips as {fct}")
    return con


def test_staging_preserves_row_count(raw_trips):
    build_star(raw_trips)
    src, stg = raw_trips.execute(
        "select (select count(*) from raw_trips), (select count(*) from stg_trips)"
    ).fetchone()
    assert src == stg, f"staging changed the row count: {src} -> {stg}"


def test_fact_grain_is_one_row_per_trip(raw_trips):
    build_star(raw_trips)
    rows, keys = raw_trips.execute(
        "select count(*), count(distinct trip_key) from fct_trips"
    ).fetchone()
    assert rows == keys, f"{rows - keys} duplicate trip_keys - the grain is broken"


def test_fact_does_not_lose_rows_to_the_dimension_join(raw_trips):
    """The silent-join-loss check, as a test rather than a hope."""
    build_star(raw_trips)
    joined = raw_trips.execute("""
        select count(*) from fct_trips f
        join dim_zone z on f.pickup_zone_key = z.zone_id
    """).fetchone()[0]
    total = raw_trips.execute("select count(*) from fct_trips").fetchone()[0]
    assert joined == total, (
        f"joining to dim_zone dropped {total - joined} of {total} trips"
    )


def test_joining_dimensions_does_not_inflate_measures(raw_trips):
    """Fan-out is the classic star schema bug: revenue silently multiplies."""
    build_star(raw_trips)
    base = raw_trips.execute(
        "select round(sum(total_amount), 2) from fct_trips"
    ).fetchone()[0]
    joined = raw_trips.execute("""
        select round(sum(f.total_amount), 2)
        from fct_trips f
        join dim_zone pu on f.pickup_zone_key = pu.zone_id
        join dim_zone dz on f.dropoff_zone_key = dz.zone_id
    """).fetchone()[0]
    assert base == joined, (
        f"revenue changed when joining dimensions: {base} -> {joined} (fan-out)"
    )


def test_every_foreign_key_resolves(raw_trips):
    build_star(raw_trips)
    orphans = raw_trips.execute("""
        select count(*) from fct_trips f
        left join dim_zone z on f.pickup_zone_key = z.zone_id
        where z.zone_id is null
    """).fetchone()[0]
    assert orphans == 0, f"{orphans} trips reference a zone that does not exist"


def test_derived_ratios_are_null_not_infinite(raw_trips, make_row):
    """A zero-distance trip must yield NULL fare_per_mile, never a divide error."""
    raw_trips.executemany(
        f"insert into raw_trips values ({', '.join(['?'] * 25)})",
        [make_row(950, dist=0.0)],
    )
    build_star(raw_trips)
    bad = raw_trips.execute("""
        select count(*) from fct_trips
        where trip_distance = 0 and fare_per_mile is not null
    """).fetchone()[0]
    assert bad == 0, "divide-by-zero produced a value instead of NULL"


def test_surrogate_key_is_deterministic(raw_trips):
    build_star(raw_trips)
    first = raw_trips.execute(
        "select trip_key from stg_trips order by pickup_at limit 1"
    ).fetchone()[0]
    stg = render(model("stg_trips"), {"raw_trips": "raw_trips"})
    raw_trips.execute(f"create or replace table stg_trips_again as {stg}")
    second = raw_trips.execute(
        "select trip_key from stg_trips_again order by pickup_at limit 1"
    ).fetchone()[0]
    assert first == second, "surrogate keys change between runs - not deterministic"
