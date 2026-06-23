from decimal import Decimal

import pytest
from flux_producers.parsing import (
    ParseError,
    binance_asset,
    coinbase_asset,
    iso_to_millis,
    parse_binance,
    parse_coinbase,
    to_decimal,
)

BINANCE = {
    "stream": "btcusdt@trade",
    "data": {
        "e": "trade",
        "E": 1780000000123,
        "s": "BTCUSDT",
        "t": 4242,
        "p": "100123.45000000",
        "q": "0.00150000",
        "T": 1780000000120,
        "m": True,
        "M": True,
    },
}


def coinbase(event_type="update", **trade_overrides):
    trade = {
        "trade_id": "987654",
        "product_id": "ETH-USD",
        "price": "3501.27",
        "size": "0.4213",
        "side": "SELL",
        "time": "2026-05-28T12:00:00.123456Z",
    }
    trade.update(trade_overrides)
    return {"channel": "market_trades", "events": [{"type": event_type, "trades": [trade]}]}


def test_binance_field_mapping():
    record = parse_binance(BINANCE, ingestion_ms=1780000000200)
    assert record == {
        "event_type": "trade",
        "event_time": 1780000000123,
        "symbol": "BTCUSDT",
        "trade_id": 4242,
        "price": Decimal("100123.45000000"),
        "quantity": Decimal("0.00150000"),
        "trade_time": 1780000000120,
        "is_buyer_maker": True,
        "ingestion_time": 1780000000200,
    }


def test_binance_unwrapped_message_and_non_trade_events():
    assert parse_binance(BINANCE["data"], 0)["trade_id"] == 4242
    assert parse_binance({"stream": "x", "data": {"e": "aggTrade"}}, 0) is None
    assert parse_binance({"result": None, "id": 1}, 0) is None


def test_binance_ignores_undocumented_m_flag():
    msg = {"stream": "s", "data": {**BINANCE["data"], "M": False}}
    assert parse_binance(msg, 0) is not None


@pytest.mark.parametrize(
    "mutation",
    [
        {"p": "abc"},
        {"t": "4242"},
        {"m": "true"},
        {"E": True},
        {"q": "-1"},
        {"p": "1.123456789"},
    ],
)
def test_binance_malformed(mutation):
    with pytest.raises(ParseError):
        parse_binance({"stream": "s", "data": {**BINANCE["data"], **mutation}}, 0)


def test_binance_missing_field():
    data = dict(BINANCE["data"])
    del data["T"]
    with pytest.raises(ParseError, match="'T'"):
        parse_binance({"stream": "s", "data": data}, 0)


def test_coinbase_field_mapping():
    [record] = parse_coinbase(coinbase(), ingestion_ms=5)
    assert record == {
        "trade_id": "987654",
        "product_id": "ETH-USD",
        "price": Decimal("3501.27000000"),
        "size": Decimal("0.42130000"),
        "side": "SELL",
        "event_time": iso_to_millis("2026-05-28T12:00:00.123Z"),
        "ingestion_time": 5,
    }


def test_coinbase_snapshot_is_skipped():
    assert parse_coinbase(coinbase(event_type="snapshot"), 0) == []


def test_coinbase_other_channels_ignored():
    assert parse_coinbase({"channel": "heartbeats", "events": []}, 0) == []
    assert parse_coinbase({"channel": "subscriptions", "events": [{}]}, 0) == []


def test_coinbase_multiple_trades_and_events():
    msg = coinbase()
    msg["events"][0]["trades"].append({**msg["events"][0]["trades"][0], "trade_id": "987655"})
    msg["events"].append(coinbase(event_type="snapshot")["events"][0])
    assert [r["trade_id"] for r in parse_coinbase(msg, 0)] == ["987654", "987655"]


@pytest.mark.parametrize(
    "overrides", [{"side": "HOLD"}, {"time": "yesterday"}, {"price": None}, {"size": "1e400"}]
)
def test_coinbase_malformed(overrides):
    with pytest.raises(ParseError):
        parse_coinbase(coinbase(**overrides), 0)


def test_coinbase_lowercase_side_normalized():
    assert parse_coinbase(coinbase(side="buy"), 0)[0]["side"] == "BUY"


def test_decimal_exactness():
    assert to_decimal("0.1", "x") + to_decimal("0.2", "x") == Decimal("0.3")
    assert to_decimal("1234567890.12345678", "x") == Decimal("1234567890.12345678")
    assert to_decimal("1234567890.12345678", "x").as_tuple().exponent == -8
    with pytest.raises(ParseError):
        to_decimal("12345678901.12345678", "x")
    with pytest.raises(ParseError):
        to_decimal("NaN", "x")


@pytest.mark.parametrize(
    "text, expected",
    [
        ("1970-01-01T00:00:00Z", 0),
        ("1970-01-01T00:00:01.5Z", 1500),
        ("1970-01-01T00:00:01.999999999Z", 1999),
        ("1970-01-01T01:00:00+01:00", 0),
    ],
)
def test_iso_to_millis(text, expected):
    assert iso_to_millis(text) == expected


def test_asset_split():
    assert binance_asset("SOLUSDT") == "SOL"
    assert coinbase_asset("BTC-USD") == "BTC"
