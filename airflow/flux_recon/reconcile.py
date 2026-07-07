"""Reconcile one closed interval: lake (batch) against bars_1m (stream)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import asdict
from datetime import datetime, timedelta

from flux_recon import compare, lake, serving

# Late events are stamped up to ~30 s early; one extra minute of lake progress covers them.
LAKE_MARGIN = timedelta(minutes=1)


def parse_instant(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError(f"timestamp must include a UTC offset: {value!r}")
    return dt


def freshness(start: datetime, end: datetime, env: Mapping[str, str] = os.environ) -> tuple[bool, str]:
    """True once both sides provably contain everything for [start, end).

    Stream: every (exchange, asset) seen in the interval has bars up to `end`.
    Lake: each of those pairs has a committed trade at or after `end + LAKE_MARGIN`.
    """
    ch = serving.client(env)
    expected = {(k.exchange, k.asset) for k in serving.window_aggregates(ch, start, end)}
    if not expected:
        return False, "no bars in the interval yet"
    stream_done = serving.progressed_pairs(ch, end)
    lake_done = lake.progressed_pairs(lake.trades(env), end + LAKE_MARGIN)
    lagging = sorted(
        [f"stream {ex}/{asset}" for ex, asset in expected - stream_done]
        + [f"lake {ex}/{asset}" for ex, asset in expected - lake_done]
    )
    if lagging:
        return False, "waiting for " + ", ".join(lagging)
    return True, f"{len(expected)} (exchange, asset) pairs complete on both sides"


def reconcile(start: datetime, end: datetime, run_id: str, env: Mapping[str, str] = os.environ) -> dict:
    ch = serving.client(env)
    batch = lake.window_aggregates(lake.trades(env), start, end)
    stream = serving.window_aggregates(ch, start, end)
    results = compare.compare_windows(stream, batch)
    summary = compare.summarize(results)
    serving.write_results(ch, start, end, results, summary, run_id)
    return {"interval_start": start.isoformat(), "interval_end": end.isoformat(), **asdict(summary)}
