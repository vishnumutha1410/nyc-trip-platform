"""Data quality rules for TLC trip records.

These are plain SQL expression strings on purpose. Spark applies them via
Spark SQL, and the test suite runs the identical strings against DuckDB with
synthetic rows. One definition, two engines, no drift between the rule you
documented and the rule you shipped.
"""

# A row failing ANY of these is quarantined rather than silently dropped.
# Each entry: (rule_name, sql_predicate_that_is_TRUE_when_the_row_is_VALID)
QUALITY_RULES = [
    ("fare_not_negative",     "fare_amount >= 0"),
    ("total_not_negative",    "total_amount >= 0"),
    ("distance_positive",     "trip_distance > 0"),
    ("distance_plausible",    "trip_distance < 300"),
    ("pickup_before_dropoff", "pickup_datetime < dropoff_datetime"),
    ("duration_plausible",    "datediff('second', pickup_datetime, dropoff_datetime) < 43200"),
    ("passenger_count_sane",  "passenger_count IS NULL OR passenger_count BETWEEN 0 AND 9"),
    ("zones_present",         "pickup_location_id IS NOT NULL AND dropoff_location_id IS NOT NULL"),
    ("pickup_after_2009",     "pickup_datetime >= TIMESTAMP '2009-01-01'"),
    ("pickup_not_in_future",  "pickup_datetime <= current_timestamp"),
]


def valid_row_expression() -> str:
    """A single SQL predicate that is TRUE only for fully valid rows."""
    return " AND ".join(f"({pred})" for _, pred in QUALITY_RULES)


def failure_reason_expression() -> str:
    """SQL CASE returning the name of the FIRST rule a row violates, else NULL.

    Keeping the reason on the quarantined row is what makes the quarantine
    table useful - otherwise you have a pile of bad rows and no idea why.
    """
    whens = "\n".join(
        f"    WHEN NOT ({pred}) THEN '{name}'" for name, pred in QUALITY_RULES
    )
    return f"CASE\n{whens}\n    ELSE NULL\nEND"
