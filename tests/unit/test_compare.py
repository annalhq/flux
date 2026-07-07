from decimal import Decimal as D

import pytest

from flux_recon.compare import (
    LATE_DROPS,
    MATCH,
    MISSING_BATCH,
    MISSING_STREAM,
    STREAM_EXCEEDS_BATCH,
    VALUE_MISMATCH,
    BatchWindow,
    StreamWindow,
    WindowKey,
    classify,
    compare_windows,
    summarize,
)

K1 = WindowKey("binance", "BTC", 1_780_000_020_000)
K2 = WindowKey("coinbase", "ETH", 1_780_000_080_000)


def stream(count=10, volume="1.5", notional="150000.25"):
    return StreamWindow(count, D(volume), D(notional))


def batch(count=10, distinct=None, volume="1.5", notional="150000.25"):
    return BatchWindow(count, count if distinct is None else distinct, D(volume), D(notional))


@pytest.mark.parametrize(
    "s, b, expected",
    [
        (stream(), batch(), MATCH),
        (stream(volume="1.50000000"), batch(volume="1.5"), MATCH),
        (stream(count=9, volume="1.4", notional="140000"), batch(), LATE_DROPS),
        (None, batch(), MISSING_STREAM),
        (stream(), None, MISSING_BATCH),
        (stream(count=11, volume="1.6", notional="160000"), batch(), STREAM_EXCEEDS_BATCH),
        (stream(count=10, volume="1.6"), batch(), STREAM_EXCEEDS_BATCH),
        (stream(count=10, notional="150000.26"), batch(), STREAM_EXCEEDS_BATCH),
        (stream(count=9, volume="1.6"), batch(), STREAM_EXCEEDS_BATCH),
        (stream(count=10, volume="1.4"), batch(), VALUE_MISMATCH),
        (stream(count=10, notional="1"), batch(), VALUE_MISMATCH),
    ],
)
def test_classify(s, b, expected):
    assert classify(s, b) == expected


def test_classify_rejects_empty_window():
    with pytest.raises(ValueError):
        classify(None, None)


def test_compare_full_outer_join_sorted():
    results = compare_windows({K2: stream(), K1: stream(count=9, volume="1", notional="1")}, {K1: batch()})
    assert [(r.key, r.status) for r in results] == [(K1, LATE_DROPS), (K2, MISSING_BATCH)]


def test_summary_ok_when_everything_matches():
    s = summarize(compare_windows({K1: stream(), K2: stream()}, {K1: batch(), K2: batch()}))
    assert (s.status, s.windows_total, s.windows_match, s.completeness_pct) == ("OK", 2, 2, 100.0)


def test_summary_late_only_above_threshold():
    s = summarize(
        compare_windows(
            {K1: stream(count=9999, volume="1", notional="1"), K2: stream()},
            {K1: batch(count=10000), K2: batch()},
        )
    )
    assert s.status == "LATE_ONLY"
    assert s.windows_late == 1
    assert s.stream_events == 10009 and s.batch_events == 10010
    assert s.completeness_pct == pytest.approx(100 * 10009 / 10010)


def test_summary_fails_below_completeness_threshold():
    s = summarize(compare_windows({K1: stream(count=900, volume="1", notional="1")}, {K1: batch(count=1000)}))
    assert s.status == "FAILED" and s.windows_late == 1


@pytest.mark.parametrize(
    "streams, batches, field",
    [
        ({}, {K1: batch()}, "windows_missing"),
        ({K1: stream()}, {}, "windows_missing"),
        ({K1: stream(count=11)}, {K1: batch()}, "windows_exceed"),
        ({K1: stream(volume="1")}, {K1: batch()}, "windows_mismatch"),
    ],
)
def test_summary_fails_on_structural_problems(streams, batches, field):
    s = summarize(compare_windows(streams, batches))
    assert s.status == "FAILED" and getattr(s, field) == 1


def test_summary_no_data():
    s = summarize([])
    assert s.status == "NO_DATA" and s.windows_total == 0 and s.completeness_pct == 0.0


def test_duplicates_counted_from_batch_distinct_ids():
    s = summarize(compare_windows({K1: stream(count=12)}, {K1: batch(count=12, distinct=10)}))
    assert s.duplicate_events == 2 and s.status == "OK"


def test_completeness_threshold_is_configurable():
    results = compare_windows({K1: stream(count=95, volume="1", notional="1")}, {K1: batch(count=100)})
    assert summarize(results, min_completeness_pct=90).status == "LATE_ONLY"
    assert summarize(results).status == "FAILED"
