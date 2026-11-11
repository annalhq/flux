"""Deterministic synthetic market emitting exchange-shaped trade messages.

The event clock starts SYNTH_BACKFILL_MINUTES before now (minute-aligned) and runs SYNTH_SPEED
times faster than the wall clock until it catches up, then in real time. Scenario timings are
offsets from the start of the event clock.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, ROUND_UP, Decimal

from flux_producers.config import SynthSettings

EXCHANGES = ("binance", "coinbase")
REFERENCE_PRICES = {"BTC": 100_000.0, "ETH": 3_500.0, "SOL": 180.0}
DEFAULT_REFERENCE_PRICE = 100.0
PRICE_TICK = Decimal("0.01")
QTY_STEP = {"BTC": Decimal("0.00001"), "ETH": Decimal("0.0001"), "SOL": Decimal("0.001")}
DEFAULT_QTY_STEP = Decimal("0.001")

MINUTE_VOLATILITY = 0.0005
COINBASE_BASIS = 0.0002
COINBASE_BASIS_NOISE = 0.0001
NOTIONAL_MEDIAN = 1_500.0
NOTIONAL_SIGMA = 0.6
LATE_SHIFT_MS = 30_000

WHALE_INTERVAL_S = 180
WHALE_OFFSET_S = 90
SPREAD_BLOWOUT_ASSET = "ETH"
SPREAD_BLOWOUT_MINUTES = range(30, 33)
SPREAD_BLOWOUT_BPS = 25
DOUBLE_BOTTOM_ASSET = "SOL"
DOUBLE_BOTTOM_START_MINUTE = 60
# Per-bar drift: first dip, rally to the neckline, second dip to within 0.2 % of the first low,
# then a breakout bar that closes above the neckline and a short follow-through.
DOUBLE_BOTTOM_DRIFT = (-0.004,) * 5 + (0.0035,) * 4 + (-0.0035,) * 4 + (0.018,) + (0.003,) * 3

Message = tuple[str, dict]

log = logging.getLogger(__name__)


def _iso(ms: int) -> str:
    dt = datetime.fromtimestamp(ms // 1000, UTC) + timedelta(milliseconds=ms % 1000)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % 1000:03d}Z"


def aligned_start_ms(now_ms: int, backfill_minutes: int, resume_after_ms: int | None = None) -> int:
    """Minute-aligned start of the event clock; continues after `resume_after_ms` if data exists."""
    start = (now_ms // 60_000) * 60_000 - backfill_minutes * 60_000
    if resume_after_ms is not None:
        start = max(start, (resume_after_ms // 60_000 + 1) * 60_000)
    return start


class SyntheticMarket:
    """Generates one simulated second of trades at a time; fully determined by seed and start."""

    def __init__(self, settings: SynthSettings, assets: tuple[str, ...], start_ms: int) -> None:
        if start_ms % 60_000:
            raise ValueError("start_ms must be minute-aligned so scenario bars match 1-minute windows")
        self.settings = settings
        self.assets = assets
        self.start_ms = start_ms
        self.second = 0
        self._rng = random.Random(settings.seed)
        self._prices = {a: REFERENCE_PRICES.get(a, DEFAULT_REFERENCE_PRICE) for a in assets}
        # Trade ids are unique per exchange across assets and across runs with different starts.
        base = (start_ms // 1000) * 1_000_000_000
        self._binance_ids = {a: base + i * 100_000_000 for i, a in enumerate(assets)}
        self._coinbase_ids = dict(self._binance_ids)
        self._sequence = 0

    @property
    def next_second_start_ms(self) -> int:
        return self.start_ms + self.second * 1000

    def _drift(self, asset: str, minute: int) -> float:
        if asset == DOUBLE_BOTTOM_ASSET:
            bar = minute - DOUBLE_BOTTOM_START_MINUTE
            if 0 <= bar < len(DOUBLE_BOTTOM_DRIFT):
                return DOUBLE_BOTTOM_DRIFT[bar]
        return 0.0

    def _basis(self, asset: str, minute: int) -> float:
        basis = COINBASE_BASIS
        if asset == SPREAD_BLOWOUT_ASSET and minute in SPREAD_BLOWOUT_MINUTES:
            basis += SPREAD_BLOWOUT_BPS / 10_000
        return basis

    def _trade_count(self) -> int:
        whole = int(self.settings.tps)
        return whole + (1 if self._rng.random() < self.settings.tps - whole else 0)

    def next_second(self) -> list[Message]:
        sec = self.second
        self.second += 1
        minute = sec // 60
        t0 = self.start_ms + sec * 1000
        out: list[tuple[int, Message]] = []

        for asset in self.assets:
            step = math.log1p(self._drift(asset, minute)) / 60
            noise = self._rng.gauss(0.0, MINUTE_VOLATILITY / math.sqrt(60))
            self._prices[asset] *= math.exp(step + noise)
            reference = self._prices[asset]
            basis = self._basis(asset, minute)
            for exchange in EXCHANGES:
                for offset in sorted(self._rng.randrange(1000) for _ in range(self._trade_count())):
                    price = reference
                    if exchange == "coinbase":
                        price *= 1 + basis + self._rng.gauss(0.0, COINBASE_BASIS_NOISE)
                    notional = self._rng.lognormvariate(math.log(NOTIONAL_MEDIAN), NOTIONAL_SIGMA)
                    out.extend(self._emit(exchange, asset, t0 + offset, price, notional))

        if sec % WHALE_INTERVAL_S == WHALE_OFFSET_S:
            k = sec // WHALE_INTERVAL_S
            asset = self.assets[k % len(self.assets)]
            exchange = EXCHANGES[k % len(EXCHANGES)]
            notional = 2 * self.settings.whale_min_notional
            out.extend(self._emit(exchange, asset, t0 + 500, self._prices[asset], notional, whale=True))

        out.sort(key=lambda item: item[0])
        return [msg for _, msg in out]

    def _emit(
        self, exchange: str, asset: str, event_ms: int, price: float, notional: float, whale: bool = False
    ) -> list[tuple[int, Message]]:
        price_d = Decimal(f"{price:.2f}").quantize(PRICE_TICK)
        step = QTY_STEP.get(asset, DEFAULT_QTY_STEP)
        rounding = ROUND_UP if whale else ROUND_HALF_UP
        qty = max((Decimal(notional) / price_d).quantize(step, rounding=rounding), step)
        taker_buys = self._rng.random() < 0.5
        stamped_ms = event_ms - LATE_SHIFT_MS if self._rng.random() < self.settings.late_ratio else event_ms

        if exchange == "binance":
            self._binance_ids[asset] += 1
            msg = {
                "stream": f"{asset.lower()}usdt@trade",
                "data": {
                    "e": "trade",
                    "E": stamped_ms + 2,
                    "s": f"{asset}USDT",
                    "t": self._binance_ids[asset],
                    "p": str(price_d),
                    "q": str(qty),
                    "T": stamped_ms,
                    "m": not taker_buys,
                    "M": True,
                },
            }
            return [(event_ms, ("binance", msg))]

        self._coinbase_ids[asset] += 1
        trade = {
            "trade_id": str(self._coinbase_ids[asset]),
            "product_id": f"{asset}-USD",
            "price": str(price_d),
            "size": str(qty),
            "side": "SELL" if taker_buys else "BUY",  # Coinbase reports the maker side
            "time": _iso(stamped_ms),
        }
        copies = 2 if self._rng.random() < self.settings.dup_ratio else 1
        messages = []
        for _ in range(copies):
            self._sequence += 1
            msg = {
                "channel": "market_trades",
                "client_id": "",
                "timestamp": _iso(event_ms),
                "sequence_num": self._sequence,
                "events": [{"type": "update", "trades": [dict(trade)]}],
            }
            messages.append((event_ms, ("coinbase", msg)))
        return messages


def event_clock_ms(
    start_ms: int, wall_elapsed_s: float, speed: float, warmup_s: float, now_ms: float
) -> float:
    """Event time reached after `wall_elapsed_s` seconds of wall clock.

    The first `warmup_s` seconds run at real time so every partition receives data before the clock
    accelerates: a delivery skew of a fraction of a second between partitions would otherwise become
    tens of simulated seconds and land behind the watermark. Never runs ahead of the wall clock.
    """
    accelerated = max(0.0, wall_elapsed_s - warmup_s)
    simulated_s = min(wall_elapsed_s, warmup_s) + accelerated * speed
    return min(start_ms + simulated_s * 1000, now_ms)


async def run(
    settings: SynthSettings,
    assets: tuple[str, ...],
    emit: Callable[[str, dict], None],
    resume_after_ms: int | None = None,
    clock: Callable[[], float] = time.time,
) -> None:
    """Pace the market against the wall clock and hand every message to `emit`."""
    wall_start = clock()
    start_ms = aligned_start_ms(int(wall_start * 1000), settings.backfill_minutes, resume_after_ms)
    log.info("synthetic event clock starts at %s", _iso(start_ms))
    market = SyntheticMarket(settings, assets, start_ms)
    while True:
        now = clock()
        target_ms = event_clock_ms(
            market.start_ms, now - wall_start, settings.speed, settings.warmup_seconds, now * 1000
        )
        generated = 0
        while market.next_second_start_ms + 1000 <= target_ms:
            for exchange, msg in market.next_second():
                emit(exchange, msg)
            generated += 1
            if generated % 5 == 0:
                await asyncio.sleep(0)
        await asyncio.sleep(0.05 if generated == 0 else 0)
