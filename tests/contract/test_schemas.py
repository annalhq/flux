"""Avro contracts: schemas parse, decimals round-trip exactly, and evolve BACKWARD-compatibly."""

import io
import json
from decimal import Decimal
from pathlib import Path

import fastavro
import pytest

ROOT = Path(__file__).resolve().parents[2]
SCHEMAS = ROOT / "producers" / "schemas"
FIXTURES = Path(__file__).parent / "fixtures"

SAMPLES = {
    "binance_trade.avsc": {
        "event_type": "trade",
        "event_time": 1780000000123,
        "symbol": "BTCUSDT",
        "trade_id": 4242,
        "price": Decimal("9999999999.99999999"),
        "quantity": Decimal("0.00000001"),
        "trade_time": 1780000000120,
        "is_buyer_maker": False,
        "ingestion_time": 1780000000200,
    },
    "coinbase_trade.avsc": {
        "trade_id": "987654",
        "product_id": "ETH-USD",
        "price": Decimal("3501.27000000"),
        "size": Decimal("0.42130000"),
        "side": "BUY",
        "event_time": 1780000000000,
        "ingestion_time": 1780000000050,
    },
}


def load(path: Path) -> dict:
    return fastavro.parse_schema(json.loads(path.read_text()))


def roundtrip(writer: dict, reader: dict, record: dict) -> dict:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, writer, record)
    buf.seek(0)
    return fastavro.schemaless_reader(buf, writer, reader)


@pytest.mark.parametrize("name", sorted(SAMPLES))
def test_schema_parses_with_timestamp_logical_types(name):
    raw = json.loads((SCHEMAS / name).read_text())
    load(SCHEMAS / name)
    time_fields = [f for f in raw["fields"] if f["name"].endswith("_time")]
    assert time_fields
    for f in time_fields:
        assert f["type"] == {"type": "long", "logicalType": "timestamp-millis"}, f["name"]


@pytest.mark.parametrize("name", sorted(SAMPLES))
def test_decimal_roundtrip_is_exact(name):
    schema = load(SCHEMAS / name)
    out = roundtrip(schema, schema, SAMPLES[name])
    for key, value in SAMPLES[name].items():
        if isinstance(value, Decimal):
            assert out[key] == value and out[key].as_tuple().exponent == -8


@pytest.mark.parametrize("name", sorted(SAMPLES))
def test_backward_compatible_with_previous_version(name):
    """New schema (reader) must read data written with the committed previous version."""
    previous, current = load(FIXTURES / name), load(SCHEMAS / name)
    out = roundtrip(previous, current, SAMPLES[name])
    for field in json.loads((SCHEMAS / name).read_text())["fields"]:
        assert field["name"] in out


def test_dlq_envelope_schema_is_valid_json():
    schema = json.loads((SCHEMAS / "dlq_envelope.json").read_text())
    assert set(schema["required"]) == set(schema["properties"])
