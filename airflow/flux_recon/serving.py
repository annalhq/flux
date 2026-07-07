"""ClickHouse access: streaming aggregates in, reconciliation results out."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from flux_recon.compare import RunSummary, StreamWindow, WindowKey, WindowResult


def client(env: Mapping[str, str] = os.environ) -> Client:
    return clickhouse_connect.get_client(
        host=env.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(env.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=env.get("CLICKHOUSE_USER", "flux"),
        password=env.get("CLICKHOUSE_PASSWORD", ""),
        database="flux",
    )


def window_aggregates(ch: Client, start: datetime, end: datetime) -> dict[WindowKey, StreamWindow]:
    rows = ch.query(
        """
        SELECT exchange, asset, toUnixTimestamp64Milli(window_start), trade_count, volume, notional
        FROM flux.bars_1m FINAL
        WHERE window_start >= {start:DateTime64(3, 'UTC')} AND window_start < {end:DateTime64(3, 'UTC')}
        """,
        parameters={"start": start, "end": end},
    ).result_rows
    return {
        WindowKey(ex, asset, int(ws)): StreamWindow(int(n), volume, notional)
        for ex, asset, ws, n, volume, notional in rows
    }


def progressed_pairs(ch: Client, after: datetime) -> set[tuple]:
    """(exchange, asset) pairs whose bars reach `after`: every window before it is closed and ingested."""
    rows = ch.query(
        """
        SELECT exchange, asset FROM flux.bars_1m
        GROUP BY exchange, asset
        HAVING max(window_end) >= {after:DateTime64(3, 'UTC')}
        """,
        parameters={"after": after},
    ).result_rows
    return {(ex, asset) for ex, asset in rows}


def write_results(
    ch: Client,
    start: datetime,
    end: datetime,
    results: Sequence[WindowResult],
    summary: RunSummary,
    run_id: str,
) -> None:
    checked_at = datetime.now(UTC)
    zero = 0
    window_rows = [
        [
            start,
            r.key.exchange,
            r.key.asset,
            datetime.fromtimestamp(r.key.window_start_ms / 1000, UTC),
            r.stream.count if r.stream else zero,
            r.batch.count if r.batch else zero,
            r.batch.distinct_ids if r.batch else zero,
            r.stream.volume if r.stream else zero,
            r.batch.volume if r.batch else zero,
            r.stream.notional if r.stream else zero,
            r.batch.notional if r.batch else zero,
            r.status,
            run_id,
            checked_at,
        ]
        for r in results
    ]
    if window_rows:
        ch.insert(
            "flux.recon_windows",
            window_rows,
            column_names=[
                "interval_start", "exchange", "asset", "window_start",
                "stream_count", "batch_count", "batch_distinct_ids",
                "stream_volume", "batch_volume", "stream_notional", "batch_notional",
                "status", "dag_run_id", "checked_at",
            ],
        )  # fmt: skip
    ch.insert(
        "flux.recon_runs",
        [
            [
                start, end,
                summary.windows_total, summary.windows_match, summary.windows_late,
                summary.windows_missing, summary.windows_exceed, summary.windows_mismatch,
                summary.stream_events, summary.batch_events, summary.duplicate_events,
                summary.completeness_pct, summary.status, run_id, checked_at,
            ]
        ],
        column_names=[
            "interval_start", "interval_end",
            "windows_total", "windows_match", "windows_late",
            "windows_missing", "windows_exceed", "windows_mismatch",
            "stream_events", "batch_events", "duplicate_events",
            "completeness_pct", "status", "dag_run_id", "checked_at",
        ],
    )  # fmt: skip
