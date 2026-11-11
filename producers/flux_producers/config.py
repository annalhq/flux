"""Environment-driven settings."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

MODES = ("live", "synthetic")


@dataclass(frozen=True)
class SynthSettings:
    seed: int = 42
    backfill_minutes: int = 120
    speed: float = 120.0
    warmup_seconds: float = 15.0
    tps: float = 5.0
    late_ratio: float = 0.001
    dup_ratio: float = 0.0005
    whale_min_notional: float = 250_000.0


@dataclass(frozen=True)
class Settings:
    mode: str
    kafka_bootstrap: str
    schema_registry_url: str
    assets: tuple[str, ...]
    metrics_port: int
    synth: SynthSettings
    schema_dir: Path
    producer_version: str
    binance_ws_url: str = "wss://stream.binance.com:9443/stream"
    coinbase_ws_url: str = "wss://advanced-trade-ws.coinbase.com"

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> Settings:
        from flux_producers import __version__

        mode = env.get("PRODUCER_MODE", "synthetic").strip().lower()
        if mode not in MODES:
            raise ValueError(f"PRODUCER_MODE must be one of {MODES}, got {mode!r}")
        assets = tuple(a.strip().upper() for a in env.get("ASSETS", "BTC,ETH,SOL").split(",") if a.strip())
        if not assets:
            raise ValueError("ASSETS is empty")
        return cls(
            mode=mode,
            kafka_bootstrap=env.get("KAFKA_BOOTSTRAP", "kafka-1:9092,kafka-2:9092,kafka-3:9092"),
            schema_registry_url=env.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
            assets=assets,
            metrics_port=int(env.get("PRODUCER_METRICS_PORT", "8000")),
            synth=SynthSettings(
                seed=int(env.get("SYNTH_SEED", "42")),
                backfill_minutes=int(env.get("SYNTH_BACKFILL_MINUTES", "120")),
                speed=float(env.get("SYNTH_SPEED", "120")),
                warmup_seconds=float(env.get("SYNTH_WARMUP_SECONDS", "15")),
                tps=float(env.get("SYNTH_TPS", "5")),
                late_ratio=float(env.get("SYNTH_LATE_RATIO", "0.001")),
                dup_ratio=float(env.get("SYNTH_DUP_RATIO", "0.0005")),
                whale_min_notional=float(env.get("WHALE_MIN_NOTIONAL", "250000")),
            ),
            schema_dir=Path(env.get("SCHEMA_DIR", Path(__file__).resolve().parent.parent / "schemas")),
            producer_version=env.get("FLUX_VERSION", __version__),
        )
