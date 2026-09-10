"""Unit tests for the change-event parser. No Kafka, no warehouse, ~0.1s.

The point of these is not coverage theatre. Every case here is a real message
shape the parser will actually meet, and the two "bad" cases are the ones that
decide whether a malformed record lands in the DLQ or takes the consumer down.

    python -m pytest tests/test_cdc_parse.py -q
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cdc.consume_changes import BadEvent, parse  # noqa: E402


class FakeMessage:
    """Stands in for confluent_kafka.Message, which cannot be constructed."""

    def __init__(self, value, key=b'{"customer_id":1}', topic="ordersdb.public.customers",
                 partition=0, offset=10):
        self._value = value if value is None or isinstance(value, bytes) \
            else json.dumps(value).encode()
        self._key, self._topic = key, topic
        self._partition, self._offset = partition, offset

    def value(self): return self._value
    def key(self): return self._key
    def topic(self): return self._topic
    def partition(self): return self._partition
    def offset(self): return self._offset


CUSTOMER = {"customer_id": 1, "email": "ava.reed@example.com", "tier": "bronze"}
SOURCE = {"table": "customers", "schema": "public", "lsn": 26718672,
          "ts_ms": 1789059697025, "snapshot": "false"}


def envelope(op, before=None, after=None, source=None):
    return {"before": before, "after": after, "source": source or SOURCE,
            "op": op, "ts_ms": 1789059697583}


def test_snapshot_read_has_no_before():
    ev = parse(FakeMessage(envelope("r", after=CUSTOMER)))
    assert ev.op == "r"
    assert ev.before_image is None
    assert json.loads(ev.after_image)["tier"] == "bronze"


def test_update_keeps_both_images():
    after = dict(CUSTOMER, tier="platinum")
    ev = parse(FakeMessage(envelope("u", before=CUSTOMER, after=after)))
    assert json.loads(ev.before_image)["tier"] == "bronze"
    assert json.loads(ev.after_image)["tier"] == "platinum"


def test_delete_keeps_before_and_has_no_after():
    ev = parse(FakeMessage(envelope("d", before=CUSTOMER)))
    assert ev.op == "d"
    assert ev.after_image is None
    assert json.loads(ev.before_image)["customer_id"] == 1


def test_event_uid_is_kafka_coordinates():
    """The dedupe key must come from Kafka, not the payload.

    Two different changes to the same row at the same millisecond would collide
    on any payload-derived key. topic:partition:offset cannot collide.
    """
    ev = parse(FakeMessage(envelope("u", before=CUSTOMER, after=CUSTOMER),
                           partition=2, offset=999))
    assert ev.event_uid == "ordersdb.public.customers:2:999"


def test_lsn_is_carried_through():
    """LSN is the ordering key for SCD Type 2 - losing it here is unrecoverable."""
    ev = parse(FakeMessage(envelope("u", before=CUSTOMER, after=CUSTOMER)))
    assert ev.lsn == 26718672


def test_noop_update_is_still_a_valid_event():
    """Postgres emits a WAL record even when an UPDATE changes nothing.

    The parser must accept it. Suppressing it is a modelling decision that
    belongs downstream, where it can be seen and tested - not silently here.
    """
    ev = parse(FakeMessage(envelope("u", before=CUSTOMER, after=dict(CUSTOMER))))
    assert ev.before_image == ev.after_image


def test_malformed_json_is_a_bad_event():
    with pytest.raises(BadEvent, match="not valid JSON"):
        parse(FakeMessage(b"{not json"))


def test_unknown_op_is_a_bad_event():
    with pytest.raises(BadEvent, match="unknown or missing op"):
        parse(FakeMessage(envelope("x", after=CUSTOMER)))


def test_delete_without_before_is_a_bad_event():
    """Exactly what you see if REPLICA IDENTITY was left at DEFAULT."""
    with pytest.raises(BadEvent, match="REPLICA IDENTITY"):
        parse(FakeMessage(envelope("d")))


def test_tombstone_is_a_bad_event_not_a_crash():
    with pytest.raises(BadEvent, match="tombstone"):
        parse(FakeMessage(None))


def test_missing_source_table_is_a_bad_event():
    with pytest.raises(BadEvent, match="source.table"):
        parse(FakeMessage(envelope("c", after=CUSTOMER, source={"lsn": 1})))
