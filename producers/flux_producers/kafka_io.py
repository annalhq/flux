"""Kafka producer with Schema Registry serialization, explicit partitioning and a DLQ."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial

from confluent_kafka import Consumer, KafkaError, Message, Producer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from flux_producers import metrics
from flux_producers.config import Settings
from flux_producers.parsing import binance_asset, coinbase_asset

log = logging.getLogger(__name__)

DLQ_TOPIC = "dlq.producers"
# Pinned so each asset owns one partition of the 3-partition raw topics; murmur2 would
# put two of the three default keys on the same partition. Other assets fall back to hashing.
ASSET_PARTITIONS = {"BTC": 0, "ETH": 1, "SOL": 2}
BUFFER_RETRIES = 3


@dataclass(frozen=True)
class Stream:
    exchange: str
    topic: str
    schema_file: str
    asset_of: Callable[[dict], str]
    event_ms_of: Callable[[dict], int]


STREAMS = {
    "binance": Stream(
        "binance",
        "raw.binance.trades",
        "binance_trade.avsc",
        lambda r: binance_asset(r["symbol"]),
        lambda r: r["trade_time"],
    ),
    "coinbase": Stream(
        "coinbase",
        "raw.coinbase.trades",
        "coinbase_trade.avsc",
        lambda r: coinbase_asset(r["product_id"]),
        lambda r: r["event_time"],
    ),
}


class TradeProducer:
    def __init__(self, settings: Settings) -> None:
        self._version = settings.producer_version
        self._bootstrap = settings.kafka_bootstrap
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "client.id": "flux-producers",
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 5,
                "batch.size": 65536,
                "compression.type": "lz4",
            }
        )
        # Separate producer so a full trade queue cannot block dead-lettering.
        self._dlq = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "client.id": "flux-producers-dlq",
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "zstd",
            }
        )
        registry = SchemaRegistryClient({"url": settings.schema_registry_url})
        # Schemas are registered by the bootstrap step; producers never evolve them implicitly.
        self._serializers = {
            name: AvroSerializer(
                registry,
                (settings.schema_dir / stream.schema_file).read_text(),
                conf={"auto.register.schemas": False},
            )
            for name, stream in STREAMS.items()
        }

    def produce(self, source: str, exchange: str, record: dict) -> None:
        stream = STREAMS[exchange]
        asset = stream.asset_of(record)
        try:
            value = self._serializers[exchange](
                record, SerializationContext(stream.topic, MessageField.VALUE)
            )
        except Exception as e:
            self.dead_letter(
                source, exchange, "serialize", stream.topic, asset, json.dumps(record, default=str), e
            )
            return
        self._send(source, exchange, stream.topic, asset, value, stream.event_ms_of(record))

    def _send(self, source: str, exchange: str, topic: str, asset: str, value: bytes, event_ms: int) -> None:
        kwargs: dict = {
            "topic": topic,
            "key": asset.encode(),
            "value": value,
            "timestamp": event_ms,
            "on_delivery": partial(self._on_delivery, source, exchange, asset, topic, value),
        }
        if (partition := ASSET_PARTITIONS.get(asset)) is not None:
            kwargs["partition"] = partition
        for attempt in range(BUFFER_RETRIES + 1):
            try:
                self._producer.produce(**kwargs)
                break
            except BufferError as e:
                if attempt == BUFFER_RETRIES:
                    self.dead_letter(source, exchange, "deliver", topic, asset, value, e)
                    return
                self._producer.poll(0.5)
        self._producer.poll(0)

    def _on_delivery(
        self,
        source: str,
        exchange: str,
        asset: str,
        topic: str,
        value: bytes,
        err: KafkaError | None,
        _msg: Message,
    ) -> None:
        if err is not None:
            metrics.DELIVERY_ERRORS.labels(exchange).inc()
            self.dead_letter(source, exchange, "deliver", topic, asset, value, err)
            return
        metrics.MESSAGES.labels(exchange, asset).inc()
        metrics.LAST_EVENT.labels(exchange).set(time.time())

    def dead_letter(
        self,
        source: str,
        exchange: str,
        stage: str,
        topic: str,
        key: str | None,
        payload: str | bytes,
        error: BaseException | KafkaError,
    ) -> None:
        metrics.DLQ.labels(exchange, stage).inc()
        is_bytes = isinstance(payload, bytes)
        envelope = {
            "source": source,
            "stage": stage,
            "target_topic": topic,
            "key": key,
            "payload": base64.b64encode(payload).decode() if is_bytes else payload,
            "payload_encoding": "base64" if is_bytes else "text",
            "error": f"{type(error).__name__}: {error}",
            "failed_at_ms": int(time.time() * 1000),
            "producer_version": self._version,
        }
        log.warning("dead-lettering %s/%s message for %s: %s", source, stage, topic, envelope["error"])
        try:
            self._dlq.produce(DLQ_TOPIC, key=source.encode(), value=json.dumps(envelope).encode())
            self._dlq.poll(0)
        except BufferError:
            log.error("DLQ queue full; dropped %s/%s message for %s", source, stage, topic)

    def latest_event_ms(self, tail: int = 50, timeout: float = 10.0) -> int | None:
        """Newest message timestamp (event time) across the raw topics, or None if they are empty."""
        consumer = Consumer(
            {
                "bootstrap.servers": self._bootstrap,
                "group.id": "flux-producers-resume",
                "enable.auto.commit": False,
            }
        )
        try:
            assignments = []
            for stream in STREAMS.values():
                partitions = (
                    consumer.list_topics(stream.topic, timeout=timeout).topics[stream.topic].partitions
                )
                for p in partitions:
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(stream.topic, p), timeout=timeout
                    )
                    if high > low:
                        assignments.append((TopicPartition(stream.topic, p, max(low, high - tail)), high))
            if not assignments:
                return None
            consumer.assign([tp for tp, _ in assignments])
            remaining = {(tp.topic, tp.partition): high for tp, high in assignments}
            latest, deadline = None, time.monotonic() + timeout
            while remaining and time.monotonic() < deadline:
                msg = consumer.poll(1.0)
                if msg is None or msg.error():
                    continue
                latest = max(latest or 0, msg.timestamp()[1])
                if msg.offset() + 1 >= remaining.get((msg.topic(), msg.partition()), 0):
                    remaining.pop((msg.topic(), msg.partition()), None)
            return latest
        finally:
            consumer.close()

    @staticmethod
    def observe_lag(exchange: str, event_ms: int, ingest_ms: int) -> None:
        metrics.EVENT_LAG.labels(exchange).observe(max(0.0, (ingest_ms - event_ms) / 1000))

    async def poll_forever(self, interval: float = 0.1) -> None:
        while True:
            self._producer.poll(0)
            self._dlq.poll(0)
            await asyncio.sleep(interval)

    def flush(self, timeout: float = 30.0) -> int:
        """Flush both producers; returns the number of messages still undelivered."""
        remaining = self._producer.flush(timeout)
        return remaining + self._dlq.flush(5)
