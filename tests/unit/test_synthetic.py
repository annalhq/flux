from collections import defaultdict
from decimal import Decimal

import pytest
from flux_producers.config import SynthSettings
from flux_producers.parsing import parse_binance, parse_coinbase
from flux_producers.synthetic import (
    DOUBLE_BOTTOM_START_MINUTE,
    LATE_SHIFT_MS,
    WHALE_INTERVAL_S,
    SyntheticMarket,
    aligned_start_ms,
)

START = aligned_start_ms(1_780_000_000_000, 120)
ASSETS = ("BTC", "ETH", "SOL")


def simulate(seconds, assets=ASSETS, **overrides):
    settings = SynthSettings(**{"late_ratio": 0.0, "dup_ratio": 0.0, **overrides})
    market = SyntheticMarket(settings, assets, START)
    messages = []
    for _ in range(seconds):
        messages.extend(market.next_second())
    return messages


def records(messages):
    out = []
    for exchange, msg in messages:
        if exchange == "binance":
            r = parse_binance(msg, 0)
            out.append(
                (
                    "binance",
                    r["symbol"].removesuffix("USDT"),
                    r["trade_id"],
                    r["trade_time"],
                    r["price"],
                    r["quantity"],
                )
            )
        else:
            for r in parse_coinbase(msg, 0):
                out.append(
                    (
                        "coinbase",
                        r["product_id"].split("-")[0],
                        r["trade_id"],
                        r["event_time"],
                        r["price"],
                        r["size"],
                    )
                )
    return out


def test_start_must_be_minute_aligned():
    with pytest.raises(ValueError):
        SyntheticMarket(SynthSettings(), ASSETS, START + 1)


def test_deterministic_for_seed():
    assert simulate(120) == simulate(120)
    assert simulate(120) != simulate(120, seed=7)


def test_messages_parse_and_cover_all_streams():
    recs = records(simulate(60))
    assert {(r[0], r[1]) for r in recs} == {(e, a) for e in ("binance", "coinbase") for a in ASSETS}
    expected = 60 * 5 * len(ASSETS) * 2
    assert 0.9 * expected <= len(recs) <= 1.1 * expected


def test_typical_notional_range():
    notionals = sorted(float(r[4] * r[5]) for r in records(simulate(120)))
    median = notionals[len(notionals) // 2]
    assert 500 <= median <= 5_000


def test_whales_every_three_minutes_rotating():
    whales = [r for r in records(simulate(6 * WHALE_INTERVAL_S)) if r[4] * r[5] >= Decimal(500_000)]
    assert len(whales) == 6
    assert {(r[0], r[1]) for r in whales} == {(e, a) for e in ("binance", "coinbase") for a in ASSETS}


def test_late_ratio_within_tolerance():
    # Messages are emitted in true time order; a late trade is stamped LATE_SHIFT_MS earlier,
    # so it shows up as a backwards jump in event time relative to the previous trade.
    times = [m["data"]["T"] for e, m in simulate(600, late_ratio=0.05) if e == "binance"]
    late = sum(1 for prev, cur in zip(times, times[1:], strict=False) if prev - cur > LATE_SHIFT_MS / 2)
    assert 0.03 <= late / len(times) <= 0.07


def test_duplicate_ratio_within_tolerance():
    recs = [r for r in records(simulate(600, dup_ratio=0.05)) if r[0] == "coinbase"]
    ids = [(r[1], r[2]) for r in recs]
    dup_fraction = (len(ids) - len(set(ids))) / len(set(ids))
    assert 0.03 <= dup_fraction <= 0.07
    binance_ids = [r[2] for r in records(simulate(600)) if r[0] == "binance"]
    assert len(binance_ids) == len(set(binance_ids))


def test_spread_blowout_on_eth_coinbase():
    recs = records(simulate(34 * 60, assets=("ETH",), tps=1))

    def mean_bps(minutes):
        by_second = defaultdict(dict)
        for exchange, _, _, t, price, _ in recs:
            if (t - START) // 60_000 in minutes:
                by_second[(t - START) // 1000][exchange] = float(price)
        pairs = [v for v in by_second.values() if len(v) == 2]
        return sum((v["coinbase"] / v["binance"] - 1) * 10_000 for v in pairs) / len(pairs)

    assert 0 < mean_bps(range(10, 30)) < 5
    assert 24 < mean_bps(range(30, 33)) < 30


def bars_1m(recs, exchange):
    bars = defaultdict(list)
    for ex, _, _, t, price, _ in recs:
        if ex == exchange:
            bars[(t - START) // 60_000].append(price)
    return [{"minute": m, "high": max(p), "low": min(p), "close": p[-1]} for m, p in sorted(bars.items())]


def find_double_bottoms(bars, tolerance=0.005, min_depth=0.01):
    """Python port of the MATCH_RECOGNIZE pattern (S D1{2,} P{2,} D2{2,} B) over 1-minute bars."""
    direction = ["-"] + [
        "D" if b["close"] < a["close"] else "U" for a, b in zip(bars, bars[1:], strict=False)
    ]
    runs, i = [], 1
    while i < len(bars):
        j = i
        while j + 1 < len(bars) and direction[j + 1] == direction[i]:
            j += 1
        runs.append((direction[i], i, j))
        i = j + 1
    matches = []
    for k in range(len(runs) - 2):
        (d1, d1s, d1e), (p, ps, pe), (d2, d2s, d2e) = runs[k : k + 3]
        if (d1, p, d2) != ("D", "U", "D") or min(d1e - d1s, pe - ps, d2e - d2s) < 1 or d2e + 1 >= len(bars):
            continue
        first_low = min(b["low"] for b in bars[d1s : d1e + 1])
        second_low = min(b["low"] for b in bars[d2s : d2e + 1])
        neckline = max(b["high"] for b in bars[ps : pe + 1])
        breakout = bars[d2e + 1]
        if (
            breakout["close"] > neckline
            and abs(second_low - first_low) / first_low <= tolerance
            and (neckline - first_low) / first_low >= min_depth
        ):
            matches.append(bars[d1s]["minute"])
    return matches


@pytest.mark.parametrize("exchange", ["binance", "coinbase"])
def test_double_bottom_on_sol_matches_pattern(exchange):
    recs = records(simulate((DOUBLE_BOTTOM_START_MINUTE + 25) * 60, assets=("SOL",), tps=1))
    matches = find_double_bottoms(bars_1m(recs, exchange))
    assert any(DOUBLE_BOTTOM_START_MINUTE - 3 <= m <= DOUBLE_BOTTOM_START_MINUTE for m in matches), matches


def test_fresh_start_backfills_from_aligned_minute():
    now = 1_780_000_123_456
    assert aligned_start_ms(now, 120) == (now // 60_000 - 120) * 60_000


def test_resume_continues_after_existing_data():
    now = 1_780_000_123_456
    last = now - 600_000 + 5_000
    start = aligned_start_ms(now, 120, resume_after_ms=last)
    assert start > last and start % 60_000 == 0 and start - last <= 60_000


def test_trade_ids_do_not_collide_across_runs():
    def trade_ids(market):
        messages = [msg for _ in range(120) for msg in market.next_second()]
        return {(rec[0], rec[2]) for rec in records(messages)}

    first = SyntheticMarket(SynthSettings(), ASSETS, START)
    second = SyntheticMarket(SynthSettings(), ASSETS, START + 60_000)
    assert not trade_ids(first) & trade_ids(second)
