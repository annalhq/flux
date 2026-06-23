"""Idempotently prepare the platform: Kafka topics, Avro subjects, ClickHouse schema.

Safe to re-run. Missing topics are created, drifted topic configs are reset to the declared
values, schemas are registered (re-registering an identical schema is a no-op), and DDL uses
IF NOT EXISTS / OR REPLACE throughout.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
)
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "producers" / "schemas"
DDL_DIR = ROOT / "infra" / "clickhouse" / "ddl"

HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS


@dataclass(frozen=True)
class Topic:
    name: str
    partitions: int
    retention_ms: int
    compression: str

    def configs(self) -> dict[str, str]:
        return {
            "retention.ms": str(self.retention_ms),
            "compression.type": self.compression,
            "min.insync.replicas": "2",
            "cleanup.policy": "delete",
        }


TOPICS = (
    Topic("raw.binance.trades", 3, 48 * HOUR_MS, "lz4"),
    Topic("raw.coinbase.trades", 3, 48 * HOUR_MS, "lz4"),
    Topic("dlq.producers", 1, 7 * DAY_MS, "zstd"),
    Topic("agg.bars-1m", 3, 7 * DAY_MS, "lz4"),
    Topic("agg.spreads-5s", 3, 7 * DAY_MS, "lz4"),
    Topic("signals.whales", 1, 7 * DAY_MS, "lz4"),
    Topic("signals.double-bottom", 1, 7 * DAY_MS, "lz4"),
)
REPLICATION_FACTOR = 3

SUBJECTS = {
    "raw.binance.trades-value": "binance_trade.avsc",
    "raw.coinbase.trades-value": "coinbase_trade.avsc",
}
COMPATIBILITY = "BACKWARD"


def log(step: str, message: str) -> None:
    print(f"[{step:<10}] {message}", flush=True)


def http(method: str, url: str, body: str | dict | None = None, headers: dict | None = None) -> str:
    data = None
    headers = dict(headers or {})
    if isinstance(body, dict):
        data = json.dumps(body).encode()
        headers.setdefault("Content-Type", "application/vnd.schemaregistry.v1+json")
    elif isinstance(body, str):
        data = body.encode()
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {url} -> {e.code}: {e.read().decode().strip()}") from e


def wait_for(name: str, probe, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            probe()
            log("wait", f"{name} ready")
            return
        except Exception as e:
            if time.monotonic() > deadline:
                raise SystemExit(f"{name} not reachable after {timeout:.0f} s: {e}") from e
            time.sleep(2)


def ensure_topics(admin: AdminClient) -> None:
    existing = admin.list_topics(timeout=10).topics
    missing = [t for t in TOPICS if t.name not in existing]
    if missing:
        futures = admin.create_topics(
            [NewTopic(t.name, t.partitions, REPLICATION_FACTOR, config=t.configs()) for t in missing],
            operation_timeout=30,
        )
        for name, future in futures.items():
            try:
                future.result()
                log("topics", f"created {name}")
            except KafkaException as e:
                if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise
    existing = admin.list_topics(timeout=10).topics
    for t in TOPICS:
        if t not in missing:
            actual = len(existing[t.name].partitions)
            if actual != t.partitions:
                log("topics", f"WARNING {t.name} has {actual} partitions, expected {t.partitions}")
    _converge_configs(admin, [t for t in TOPICS if t not in missing])


def _converge_configs(admin: AdminClient, topics: list[Topic]) -> None:
    if not topics:
        return
    resources = [ConfigResource(ConfigResource.Type.TOPIC, t.name) for t in topics]
    described = admin.describe_configs(resources)
    for t, resource in zip(topics, resources, strict=True):
        current = {k: v.value for k, v in described[resource].result().items()}
        drift = {k: v for k, v in t.configs().items() if current.get(k) != v}
        if not drift:
            log("topics", f"{t.name} up to date")
            continue
        entries = [ConfigEntry(k, v, incremental_operation=AlterConfigOpType.SET) for k, v in drift.items()]
        fix = ConfigResource(ConfigResource.Type.TOPIC, t.name, incremental_configs=entries)
        admin.incremental_alter_configs([fix])[fix].result()
        log("topics", f"{t.name} reset {', '.join(sorted(drift))}")


def ensure_schemas(registry: str) -> None:
    for subject, filename in SUBJECTS.items():
        http("PUT", f"{registry}/config/{subject}", {"compatibility": COMPATIBILITY})
        schema = (SCHEMA_DIR / filename).read_text()
        result = json.loads(http("POST", f"{registry}/subjects/{subject}/versions", {"schema": schema}))
        log("schemas", f"{subject} id={result['id']} compatibility={COMPATIBILITY}")


def split_statements(sql: str) -> list[str]:
    statements = []
    for chunk in re.split(r";[ \t]*(?:--[^\n]*)?(?:\n|$)", sql):
        code = "\n".join(line for line in chunk.splitlines() if not line.strip().startswith("--"))
        if code.strip():
            statements.append(chunk.strip())
    return statements


def ensure_clickhouse(url: str, user: str, password: str) -> None:
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    headers = {"Authorization": f"Basic {token}"}
    for path in sorted(DDL_DIR.glob("*.sql")):
        statements = split_statements(path.read_text())
        for statement in statements:
            http("POST", url, statement, headers)
        log("clickhouse", f"applied {path.name} ({len(statements)} statements)")


def main() -> int:
    env = {**dotenv_values(ROOT / ".env"), **os.environ}
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--kafka", default=env.get("KAFKA_BOOTSTRAP_HOST", "localhost:29092"))
    parser.add_argument("--schema-registry", default="http://localhost:8081")
    parser.add_argument("--clickhouse", default="http://localhost:8123")
    parser.add_argument("--nessie", default="http://localhost:19120")
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()

    admin = AdminClient({"bootstrap.servers": args.kafka})
    wait_for("kafka", lambda: admin.list_topics(timeout=5), args.timeout)
    wait_for("schema-registry", lambda: http("GET", f"{args.schema_registry}/subjects"), args.timeout)
    wait_for("clickhouse", lambda: http("GET", f"{args.clickhouse}/ping"), args.timeout)
    wait_for("nessie", lambda: http("GET", f"{args.nessie}/api/v2/config"), args.timeout)

    ensure_topics(admin)
    ensure_schemas(args.schema_registry)
    ensure_clickhouse(args.clickhouse, env.get("CLICKHOUSE_USER", "flux"), env.get("CLICKHOUSE_PASSWORD", ""))
    log("done", "bootstrap complete")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, KafkaException) as e:
        sys.exit(f"bootstrap failed: {e}")
