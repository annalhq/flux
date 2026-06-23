"""Binance combined trade stream."""

from __future__ import annotations

from flux_producers.ingest import Ingestor
from flux_producers.ws import run_session


def stream_url(base_url: str, assets: tuple[str, ...]) -> str:
    streams = "/".join(f"{a.lower()}usdt@trade" for a in assets)
    return f"{base_url}?streams={streams}"


async def run(base_url: str, assets: tuple[str, ...], ingestor: Ingestor) -> None:
    async def on_open(_ws) -> None:
        return None

    await run_session(
        "binance", stream_url(base_url, assets), on_open, lambda raw: ingestor.handle_text("binance", raw)
    )
