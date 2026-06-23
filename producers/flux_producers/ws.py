"""WebSocket session loop with jittered exponential backoff."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable

import websockets

from flux_producers import metrics

log = logging.getLogger(__name__)


class Backoff:
    """1 s -> 60 s exponential backoff with +/-20 % jitter."""

    def __init__(
        self,
        initial: float = 1.0,
        maximum: float = 60.0,
        jitter: float = 0.2,
        rng: random.Random | None = None,
    ) -> None:
        self.initial = initial
        self.maximum = maximum
        self.jitter = jitter
        self._rng = rng or random.Random()
        self._current = initial

    def next(self) -> float:
        base = self._current
        self._current = min(self._current * 2, self.maximum)
        return base * self._rng.uniform(1 - self.jitter, 1 + self.jitter)

    def reset(self) -> None:
        self._current = self.initial


async def run_session(
    exchange: str,
    url: str,
    on_open: Callable[[websockets.WebSocketClientProtocol], Awaitable[None]],
    on_message: Callable[[str | bytes], None],
    healthy_after: float = 30.0,
) -> None:
    """Keep a WebSocket session alive forever. Backoff resets only after a healthy period."""
    backoff = Backoff()
    while True:
        try:
            async with websockets.connect(
                url, ping_interval=20, ping_timeout=20, close_timeout=10, max_size=2**22
            ) as ws:
                log.info("%s: connected to %s", exchange, url)
                connected_at = time.monotonic()
                await on_open(ws)
                async for raw in ws:
                    on_message(raw)
                    if connected_at and time.monotonic() - connected_at >= healthy_after:
                        backoff.reset()
                        connected_at = 0.0
                log.warning("%s: connection closed by server", exchange)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("%s: connection error: %s: %s", exchange, type(e).__name__, e)
        delay = backoff.next()
        metrics.WS_RECONNECTS.labels(exchange).inc()
        log.info("%s: reconnecting in %.1f s", exchange, delay)
        await asyncio.sleep(delay)
