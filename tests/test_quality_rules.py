"""The quality rules are SQL strings shared by Spark and these tests.

Testing them here means the rule you documented is provably the rule that
runs - there is no second implementation to drift out of sync.
"""
import datetime as dt

import pytest

from spark.jobs.rules import (
    QUALITY_RULES,
    failure_reason_expression,
    valid_row_expression,
)

BAD_ROWS = {
    "fare_not_negative":    dict(fare=-5.0),
    "distance_positive":    dict(dist=0.0),
    "distance_plausible":   dict(dist=450.0),
    "passenger_count_sane": dict(passengers=44),
}


def _insert(con, make_row, i, **kw):
    con.executemany(
        f"insert into raw_trips values ({', '.join(['?'] * 25)})",
        [make_row(i, **kw)],
    )


def test_clean_rows_all_pass(raw_trips):
    n = raw_trips.execute(
        f"select count(*) from raw_trips where {valid_row_expression()}"
    ).fetchone()[0]
    assert n == 50, f"{50 - n} of 50 clean rows were wrongly quarantined"


@pytest.mark.parametrize("rule,kwargs", list(BAD_ROWS.items()))
def test_each_bad_row_is_caught_by_the_right_rule(raw_trips, make_row, rule, kwargs):
    _insert(raw_trips, make_row, 900, **kwargs)
    reason = raw_trips.execute(f"""
        select {failure_reason_expression()}
        from raw_trips
        order by pickup_datetime desc
        limit 1
    """).fetchone()[0]
    assert reason == rule, f"expected rule {rule!r} to fire, got {reason!r}"


def test_dropoff_before_pickup_is_caught(raw_trips, make_row):
    pu = dt.datetime(2024, 1, 20, 10, 0, 0)
    _insert(raw_trips, make_row, 901, pu=pu, do=pu - dt.timedelta(minutes=5))
    bad = raw_trips.execute(
        f"select count(*) from raw_trips where not ({valid_row_expression()})"
    ).fetchone()[0]
    assert bad == 1, "a trip that ended before it started was not quarantined"


def test_every_rule_has_a_reason_branch():
    expr = failure_reason_expression()
    for name, _ in QUALITY_RULES:
        assert f"'{name}'" in expr, f"rule {name} has no branch in the reason CASE"


def test_valid_expression_covers_every_rule():
    expr = valid_row_expression()
    for _, pred in QUALITY_RULES:
        assert pred in expr, f"predicate missing from the valid-row filter: {pred}"
