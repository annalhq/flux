"""Coinbase Advanced Trade market_trades channel."""

from __future__ import annotations

import json
import logging

from flux_producers.ingest import Ingestor
from flux_producers.ws import run_session

log = logging.getLogger(__name__)


def subscriptions(assets: tuple[str, ...]) -> list[dict]:
    product_ids = [f"{a}-USD" for a in assets]
    # heartbeats keeps the connection open when a product has no trades for a while.
    return [
        {"type": "subscribe", "product_ids": product_ids, "channel": channel}
        for channel in ("market_trades", "heartbeats")
    ]


async def run(url: str, assets: tuple[str, ...], ingestor: Ingestor) -> None:
    async def on_open(ws) -> None:
        for sub in subscriptions(assets):
            await ws.send(json.dumps(sub))

    def on_message(raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except ValueError:
            ingestor.handle_text("coinbase", raw)
            return
        if isinstance(msg, dict) and msg.get("type") == "error":
            log.error("coinbase: server error: %s", msg.get("message", msg))
            return
        ingestor.handle("coinbase", msg)

    await run_session("coinbase", url, on_open, on_message)
