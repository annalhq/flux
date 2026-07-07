"""Read access to the Iceberg lake through Nessie's Iceberg REST catalog."""

from __future__ import annotations

import json
import os
import urllib.request
from collections.abc import Mapping
from datetime import datetime, timedelta

import duckdb
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.expressions import And, GreaterThanOrEqual, In, LessThan
from pyiceberg.table import Table

from flux_recon.compare import BatchWindow, WindowKey

TRADES_TABLE = "market.trades"


def catalog(env: Mapping[str, str] = os.environ) -> Catalog:
    return load_catalog(
        "lake",
        type="rest",
        uri=env.get("ICEBERG_REST_URI", "http://nessie:19120/iceberg"),
        warehouse=env.get("ICEBERG_WAREHOUSE", "warehouse"),
        **{
            "s3.endpoint": env.get("S3_ENDPOINT", "http://minio:9000"),
            "s3.access-key-id": env["MINIO_ROOT_USER"],
            "s3.secret-access-key": env["MINIO_ROOT_PASSWORD"],
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def trades(env: Mapping[str, str] = os.environ) -> Table:
    return catalog(env).load_table(TRADES_TABLE)


def nessie_commits(env: Mapping[str, str] = os.environ, branch: str = "main", limit: int = 10_000) -> int:
    """Commits on a Nessie branch. Nessie keeps table history as commits, not Iceberg snapshots."""
    base = env.get("ICEBERG_REST_URI", "http://nessie:19120/iceberg").removesuffix("/iceberg")
    url = f"{base}/api/v2/trees/{branch}/history?max-records={limit}"
    with urllib.request.urlopen(url, timeout=10) as resp:
        return len(json.load(resp)["logEntries"])


def _dates(start: datetime, end: datetime) -> list[str]:
    """Partition values (event_date) covering [start, end)."""
    days, day = [], start.date()
    while day <= (end - timedelta(microseconds=1)).date():
        days.append(day.isoformat())
        day += timedelta(days=1)
    return days


def _time_filter(start: datetime, end: datetime):
    # The event_date predicate prunes partitions; the event_ts predicate is the exact range.
    return And(
        In("event_date", _dates(start, end)),
        GreaterThanOrEqual("event_ts", start.isoformat()),
        LessThan("event_ts", end.isoformat()),
    )


def window_aggregates(table: Table, start: datetime, end: datetime) -> dict[WindowKey, BatchWindow]:
    """Per (exchange, asset, 1-minute window) measures for trades with event_ts in [start, end)."""
    arrow = table.scan(
        row_filter=_time_filter(start, end),
        selected_fields=("exchange", "asset", "trade_id", "price", "quantity", "event_ts"),
    ).to_arrow()
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.register("trades", arrow)
    # Widen before multiplying: DECIMAL(18,8) * DECIMAL(18,8) stays at width 18 in DuckDB and
    # overflows; DECIMAL(38,8) operands give DECIMAL(38,16), the type the serving store keeps.
    rows = con.execute(
        """
        SELECT exchange, asset,
               epoch_ms(time_bucket(INTERVAL 1 MINUTE, event_ts)) AS window_start_ms,
               count(*), count(DISTINCT trade_id),
               sum(CAST(quantity AS DECIMAL(38, 8))),
               sum(CAST(price AS DECIMAL(38, 8)) * CAST(quantity AS DECIMAL(38, 8)))
        FROM trades
        GROUP BY ALL
        """
    ).fetchall()
    return {
        WindowKey(ex, asset, int(ws)): BatchWindow(int(n), int(distinct), volume, notional)
        for ex, asset, ws, n, distinct, volume, notional in rows
    }


def progressed_pairs(table: Table, after: datetime, horizon: timedelta = timedelta(minutes=10)) -> set[tuple]:
    """(exchange, asset) pairs with at least one committed trade in [after, after + horizon).

    Each raw partition is ordered and an Iceberg commit covers a consistent cut of all partitions,
    so one committed trade past `after` proves everything before it in that partition is committed.
    """
    arrow = table.scan(
        row_filter=_time_filter(after, after + horizon),
        selected_fields=("exchange", "asset"),
        limit=100_000,
    ).to_arrow()
    return set(zip(arrow["exchange"].to_pylist(), arrow["asset"].to_pylist(), strict=True))
