"""Pure mapping from exchange WebSocket messages to Avro record dicts."""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

DECIMAL_SCALE = 8
DECIMAL_PRECISION = 18
_QUANTUM = Decimal(1).scaleb(-DECIMAL_SCALE)
_ISO_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})$")


class ParseError(ValueError):
    """Raised when an exchange message cannot be mapped to a record."""


def to_decimal(value: Any, field: str) -> Decimal:
    """Exact decimal at scale 8. Rejects values that would need rounding or overflow precision 18."""
    try:
        d = Decimal(str(value))
    except (InvalidOperation, TypeError) as e:
        raise ParseError(f"{field}: not a decimal: {value!r}") from e
    if not d.is_finite() or d < 0:
        raise ParseError(f"{field}: invalid value {value!r}")
    try:
        q = d.quantize(_QUANTUM)
    except InvalidOperation as e:
        raise ParseError(f"{field}: exceeds precision {DECIMAL_PRECISION}: {value!r}") from e
    if q != d:
        raise ParseError(f"{field}: more than {DECIMAL_SCALE} decimal places: {value!r}")
    if len(q.as_tuple().digits) > DECIMAL_PRECISION:
        raise ParseError(f"{field}: exceeds precision {DECIMAL_PRECISION}: {value!r}")
    return q


def iso_to_millis(value: Any) -> int:
    """ISO-8601 timestamp (any fractional precision) to epoch milliseconds, truncating below 1 ms."""
    if not isinstance(value, str):
        raise ParseError(f"time: expected string, got {value!r}")
    m = _ISO_RE.match(value)
    if not m:
        raise ParseError(f"time: not ISO-8601: {value!r}")
    base, frac, tz = m.groups()
    offset = "+00:00" if tz == "Z" else tz
    dt = datetime.fromisoformat(base + offset)
    millis = int((frac or "0")[:3].ljust(3, "0"))
    return int(dt.timestamp()) * 1000 + millis


def binance_asset(symbol: str) -> str:
    return symbol.removesuffix("USDT")


def coinbase_asset(product_id: str) -> str:
    return product_id.split("-", 1)[0]


def _get(msg: dict, key: str, kind: type | tuple[type, ...]) -> Any:
    if key not in msg:
        raise ParseError(f"missing field {key!r}")
    value = msg[key]
    if isinstance(value, bool) and kind is not bool:
        raise ParseError(f"field {key!r}: unexpected boolean")
    if not isinstance(value, kind):
        raise ParseError(f"field {key!r}: expected {kind}, got {type(value).__name__}")
    return value


def parse_binance(msg: Any, ingestion_ms: int) -> dict | None:
    """Combined-stream trade message to a BinanceTrade record; None for non-trade events."""
    if not isinstance(msg, dict):
        raise ParseError("message is not an object")
    data = msg.get("data", msg) if "stream" in msg else msg
    if not isinstance(data, dict):
        raise ParseError("'data' is not an object")
    if data.get("e") != "trade":
        return None
    return {
        "event_type": "trade",
        "event_time": _get(data, "E", int),
        "symbol": _get(data, "s", str),
        "trade_id": _get(data, "t", int),
        "price": to_decimal(_get(data, "p", (str, int, float)), "price"),
        "quantity": to_decimal(_get(data, "q", (str, int, float)), "quantity"),
        "trade_time": _get(data, "T", int),
        "is_buyer_maker": _get(data, "m", bool),
        "ingestion_time": ingestion_ms,
    }


def parse_coinbase(msg: Any, ingestion_ms: int) -> list[dict]:
    """market_trades message to CoinbaseTrade records. Snapshot events replay history and are skipped."""
    if not isinstance(msg, dict):
        raise ParseError("message is not an object")
    if msg.get("channel") != "market_trades":
        return []
    events = _get(msg, "events", list)
    records: list[dict] = []
    for event in events:
        if not isinstance(event, dict):
            raise ParseError("event is not an object")
        if event.get("type") != "update":
            continue
        for trade in _get(event, "trades", list):
            if not isinstance(trade, dict):
                raise ParseError("trade is not an object")
            side = _get(trade, "side", str).upper()
            if side not in ("BUY", "SELL"):
                raise ParseError(f"side: unexpected value {side!r}")
            records.append(
                {
                    "trade_id": str(_get(trade, "trade_id", (str, int))),
                    "product_id": _get(trade, "product_id", str),
                    "price": to_decimal(_get(trade, "price", (str, int, float)), "price"),
                    "size": to_decimal(_get(trade, "size", (str, int, float)), "size"),
                    "side": side,
                    "event_time": iso_to_millis(_get(trade, "time", str)),
                    "ingestion_time": ingestion_ms,
                }
            )
    return records
