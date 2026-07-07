"""Run the producers: `python -m flux_producers` (PRODUCER_MODE=live|synthetic)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import sys

from flux_producers import binance, coinbase, metrics, synthetic
from flux_producers.config import Settings
from flux_producers.ingest import Ingestor
from flux_producers.kafka_io import TradeProducer

log = logging.getLogger("flux_producers")


async def _run(settings: Settings, producer: TradeProducer) -> None:
    if settings.mode == "live":
        sources = [
            binance.run(
                settings.binance_ws_url, settings.assets, Ingestor(producer, "binance", observe_lag=True)
            ),
            coinbase.run(
                settings.coinbase_ws_url, settings.assets, Ingestor(producer, "coinbase", observe_lag=True)
            ),
        ]
    else:
        ingestor = Ingestor(producer, "synthetic", observe_lag=False)
        resume_after = producer.latest_event_ms()
        sources = [synthetic.run(settings.synth, settings.assets, ingestor.handle, resume_after)]

    tasks = [asyncio.create_task(producer.poll_forever())] + [asyncio.create_task(s) for s in sources]
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    waiter = asyncio.create_task(stop.wait())
    done, _ = await asyncio.wait([waiter, *tasks], return_when=asyncio.FIRST_COMPLETED)
    for task in tasks + [waiter]:
        task.cancel()
    for task in tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await task
    for task in done:
        if task is not waiter and task.exception():
            raise task.exception()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings.from_env()
    log.info("starting: mode=%s assets=%s", settings.mode, ",".join(settings.assets))
    metrics.serve(settings.metrics_port)
    producer = TradeProducer(settings)
    producer.wait_until_ready()
    try:
        asyncio.run(_run(settings, producer))
    finally:
        remaining = producer.flush(30)
        if remaining:
            log.error("shutdown: %d messages undelivered", remaining)
        else:
            log.info("shutdown: all messages delivered")
    return 0


if __name__ == "__main__":
    sys.exit(main())
