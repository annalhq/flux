"""Raw message -> parse -> produce, with parse failures dead-lettered."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Protocol

from flux_producers.kafka_io import STREAMS
from flux_producers.parsing import ParseError, parse_binance, parse_coinbase


class Sink(Protocol):
    def produce(self, source: str, exchange: str, record: dict) -> None: ...

    def dead_letter(self, source, exchange, stage, topic, key, payload, error) -> None: ...

    def observe_lag(self, exchange: str, event_ms: int, ingest_ms: int) -> None: ...


def _records(exchange: str, msg: object, ingest_ms: int) -> list[dict]:
    if exchange == "binance":
        record = parse_binance(msg, ingest_ms)
        return [record] if record else []
    return parse_coinbase(msg, ingest_ms)


def _event_ms(exchange: str, record: dict) -> int:
    return record["trade_time"] if exchange == "binance" else record["event_time"]


class Ingestor:
    def __init__(
        self, sink: Sink, source: str, observe_lag: bool, clock: Callable[[], float] = time.time
    ) -> None:
        self._sink = sink
        self._source = source
        self._observe_lag = observe_lag
        self._clock = clock

    def handle_text(self, exchange: str, text: str | bytes) -> None:
        try:
            msg = json.loads(text)
        except ValueError as e:
            raw = text.decode(errors="replace") if isinstance(text, bytes) else text
            self._sink.dead_letter(self._source, exchange, "parse", STREAMS[exchange].topic, None, raw, e)
            return
        self.handle(exchange, msg)

    def handle(self, exchange: str, msg: object) -> None:
        ingest_ms = int(self._clock() * 1000)
        try:
            records = _records(exchange, msg, ingest_ms)
        except ParseError as e:
            payload = json.dumps(msg, default=str)
            self._sink.dead_letter(self._source, exchange, "parse", STREAMS[exchange].topic, None, payload, e)
            return
        for record in records:
            if self._observe_lag:
                self._sink.observe_lag(exchange, _event_ms(exchange, record), ingest_ms)
            self._sink.produce(self._source, exchange, record)
