"""Exact window-level comparison of streaming aggregates against the lake. Pure, no I/O."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import NamedTuple

MATCH = "MATCH"
LATE_DROPS = "LATE_DROPS"
MISSING_STREAM = "MISSING_STREAM"
MISSING_BATCH = "MISSING_BATCH"
STREAM_EXCEEDS_BATCH = "STREAM_EXCEEDS_BATCH"
VALUE_MISMATCH = "VALUE_MISMATCH"

RUN_OK = "OK"
RUN_LATE_ONLY = "LATE_ONLY"
RUN_FAILED = "FAILED"
RUN_NO_DATA = "NO_DATA"

DEFAULT_MIN_COMPLETENESS_PCT = 99.9


class WindowKey(NamedTuple):
    exchange: str
    asset: str
    window_start_ms: int


@dataclass(frozen=True)
class StreamWindow:
    count: int
    volume: Decimal
    notional: Decimal


@dataclass(frozen=True)
class BatchWindow:
    count: int
    distinct_ids: int
    volume: Decimal
    notional: Decimal


@dataclass(frozen=True)
class WindowResult:
    key: WindowKey
    stream: StreamWindow | None
    batch: BatchWindow | None
    status: str


@dataclass(frozen=True)
class RunSummary:
    windows_total: int
    windows_match: int
    windows_late: int
    windows_missing: int
    windows_exceed: int
    windows_mismatch: int
    stream_events: int
    batch_events: int
    duplicate_events: int
    completeness_pct: float
    status: str


def classify(stream: StreamWindow | None, batch: BatchWindow | None) -> str:
    """Status of one window.

    The stream can only lose events (late arrivals behind the watermark), so it must never hold
    more than the lake. Equal counts with different sums cannot be explained by late data.
    """
    if stream is None and batch is None:
        raise ValueError("a window must exist on at least one side")
    if stream is None:
        return MISSING_STREAM
    if batch is None:
        return MISSING_BATCH
    if (stream.count, stream.volume, stream.notional) == (batch.count, batch.volume, batch.notional):
        return MATCH
    if stream.count > batch.count or stream.volume > batch.volume or stream.notional > batch.notional:
        return STREAM_EXCEEDS_BATCH
    if stream.count < batch.count:
        return LATE_DROPS
    return VALUE_MISMATCH


def compare_windows(
    stream: Mapping[WindowKey, StreamWindow], batch: Mapping[WindowKey, BatchWindow]
) -> list[WindowResult]:
    """Full outer join on (exchange, asset, window_start), sorted by key."""
    return [
        WindowResult(key, stream.get(key), batch.get(key), classify(stream.get(key), batch.get(key)))
        for key in sorted(set(stream) | set(batch))
    ]


def summarize(
    results: Iterable[WindowResult], min_completeness_pct: float = DEFAULT_MIN_COMPLETENESS_PCT
) -> RunSummary:
    results = list(results)
    counts = {status: 0 for status in (MATCH, LATE_DROPS, MISSING_STREAM, MISSING_BATCH, STREAM_EXCEEDS_BATCH,
                                       VALUE_MISMATCH)}  # fmt: skip
    for r in results:
        counts[r.status] += 1
    stream_events = sum(r.stream.count for r in results if r.stream)
    batch_events = sum(r.batch.count for r in results if r.batch)
    duplicates = sum(r.batch.count - r.batch.distinct_ids for r in results if r.batch)
    completeness = 100.0 * stream_events / batch_events if batch_events else (100.0 if results else 0.0)

    structural = (
        counts[MISSING_STREAM] + counts[MISSING_BATCH] + counts[STREAM_EXCEEDS_BATCH] + counts[VALUE_MISMATCH]
    )
    if not results:
        status = RUN_NO_DATA
    elif structural or completeness < min_completeness_pct:
        status = RUN_FAILED
    elif counts[LATE_DROPS]:
        status = RUN_LATE_ONLY
    else:
        status = RUN_OK

    return RunSummary(
        windows_total=len(results),
        windows_match=counts[MATCH],
        windows_late=counts[LATE_DROPS],
        windows_missing=counts[MISSING_STREAM] + counts[MISSING_BATCH],
        windows_exceed=counts[STREAM_EXCEEDS_BATCH],
        windows_mismatch=counts[VALUE_MISMATCH],
        stream_events=stream_events,
        batch_events=batch_events,
        duplicate_events=duplicates,
        completeness_pct=completeness,
        status=status,
    )
