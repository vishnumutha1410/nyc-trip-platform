"""Render a dbt model's SQL well enough to run it in DuckDB.

This is not a dbt implementation. It resolves the handful of Jinja
constructs this project actually uses so the model logic can be executed
and asserted on locally, with no warehouse and no credentials.

Why bother: a round trip to Snowflake to find out you fat-fingered a join
takes a minute and costs credits. This takes 30ms.
"""
from __future__ import annotations

import pathlib
import re

MODELS = pathlib.Path(__file__).resolve().parents[1] / "dbt" / "models"


def _surrogate_key(match: re.Match) -> str:
    """dbt_utils.generate_surrogate_key([a, b]) -> md5(concat_ws(...))."""
    cols = re.findall(r"'([^']+)'", match.group(1))
    casted = ", ".join(f"coalesce(cast({c} as varchar), '')" for c in cols)
    return f"md5(concat_ws('-', {casted}))"


def render(model_path: str | pathlib.Path, relations: dict[str, str]) -> str:
    """Return executable SQL for a model.

    `relations` maps the name inside ref()/source() to the DuckDB table name.
    """
    sql = pathlib.Path(model_path).read_text()

    # {{ config(...) }} blocks are metadata, not SQL
    sql = re.sub(r"\{\{\s*config\([^}]*\)\s*\}\}", "", sql, flags=re.S)

    # {{ dbt_utils.generate_surrogate_key([...]) }}
    sql = re.sub(
        r"\{\{\s*dbt_utils\.generate_surrogate_key\(\s*(\[[^\]]*\])\s*\)\s*\}\}",
        _surrogate_key, sql, flags=re.S,
    )

    # {{ ref('x') }}  and  {{ source('a', 'b') }}
    def _ref(m: re.Match) -> str:
        name = m.group(1)
        if name not in relations:
            raise KeyError(f"no test relation registered for {name!r}")
        return relations[name]

    sql = re.sub(r"\{\{\s*ref\(\s*'([^']+)'\s*\)\s*\}\}", _ref, sql)
    sql = re.sub(
        r"\{\{\s*source\(\s*'[^']+'\s*,\s*'([^']+)'\s*\)\s*\}\}", _ref, sql
    )
    return sql


def model(name: str) -> pathlib.Path:
    hits = list(MODELS.rglob(f"{name}.sql"))
    if not hits:
        raise FileNotFoundError(f"model {name}.sql not found under {MODELS}")
    return hits[0]
