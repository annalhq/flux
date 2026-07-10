"""End-to-end assertions against a running platform (`make smoke`).

Checks run in order and stop at the first failure. Checks that depend on data arriving are
retried until they pass or the overall time budget runs out. Exit code 0 means every check passed.
"""

from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
ENV = {k: v for k, v in dotenv_values(ROOT / ".env").items() if v is not None}
EXPECTED_JOBS = {"bars_1m", "whales", "spreads", "double_bottom", "lake_sink"}
MAX_FLINK_LAG = 5_000
RECON_CLI = ["/opt/recon-venv/bin/python", "-m", "flux_recon.cli"]


class CheckFailed(Exception):
    pass


def http_json(url: str, data: bytes | None = None, headers: dict | None = None):
    req = urllib.request.Request(url, data=data, headers=headers or {})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def clickhouse(sql: str) -> list[list]:
    token = base64.b64encode(
        f"{ENV.get('CLICKHOUSE_USER', 'flux')}:{ENV.get('CLICKHOUSE_PASSWORD', '')}".encode()
    )
    url = "http://localhost:8123/?" + urllib.parse.urlencode({"default_format": "JSONCompact"})
    return http_json(url, sql.encode(), {"Authorization": f"Basic {token.decode()}"})["data"]


def prometheus(expr: str) -> list[dict]:
    url = "http://localhost:9090/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    return http_json(url)["data"]["result"]


def compose(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, capture_output=True, text=True)


# --- checks: each returns a short detail string or raises CheckFailed ---------------------


def services_healthy() -> str:
    result = compose("--profile", "producers", "ps", "--all", "--format", "json")
    services = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    if not services:
        raise CheckFailed("no containers found; run `make demo` first")
    bad = []
    for s in services:
        name, state, health = s["Service"], s["State"], s.get("Health", "")
        if name == "minio-init":
            if not (state == "exited" and s.get("ExitCode") == 0):
                bad.append(f"{name} {state}/{s.get('ExitCode')}")
        elif state != "running" or health not in ("", "healthy"):
            bad.append(f"{name} {state} {health}".strip())
    if bad:
        raise CheckFailed(", ".join(bad))
    return f"{len(services)} containers, all running and healthy"


def flink_jobs_running() -> str:
    jobs = http_json("http://localhost:8080/jobs/overview")["jobs"]
    running = {j["name"] for j in jobs if j["state"] == "RUNNING"}
    missing = EXPECTED_JOBS - running
    if missing:
        raise CheckFailed(f"not running: {', '.join(sorted(missing))}")
    return f"{len(EXPECTED_JOBS)} jobs RUNNING"


def bars_cover_all_pairs() -> str:
    [[pairs, bars]] = clickhouse("SELECT uniqExact(exchange, asset), count() FROM flux.bars_1m FINAL")
    if int(pairs) < 6:
        raise CheckFailed(f"{pairs} of 6 (exchange, asset) pairs have bars")
    return f"6 pairs, {bars} bars"


def at_least_one(label: str, sql: str) -> Callable[[], str]:
    def check() -> str:
        [[n]] = clickhouse(sql)
        if int(n) < 1:
            raise CheckFailed(f"no {label} yet")
        return f"{n} {label}"

    return check


def no_ingest_errors() -> str:
    [[n]] = clickhouse("SELECT count() FROM flux.ingest_errors")
    if int(n):
        raise CheckFailed(f"{n} rows in flux.ingest_errors")
    return "0 ingest errors"


def lake_has_rows() -> str:
    base = ENV.get("ICEBERG_REST_URI", "http://nessie:19120/iceberg").replace("nessie:", "localhost:")
    config = http_json(f"{base}/v1/config?warehouse={ENV.get('ICEBERG_WAREHOUSE', 'warehouse')}")
    # The catalog returns the prefix already URL-encoded (e.g. "main%7Cwarehouse").
    prefix = config.get("overrides", {}).get("prefix") or config["defaults"]["prefix"]
    meta = http_json(f"{base}/v1/{prefix}/namespaces/market/tables/trades")["metadata"]
    current = next(
        (s for s in meta.get("snapshots", []) if s["snapshot-id"] == meta.get("current-snapshot-id")), None
    )
    rows = int((current or {}).get("summary", {}).get("total-records", 0))
    if rows <= 0:
        raise CheckFailed("market.trades has no committed rows")
    return f"{rows} rows in market.trades"


def reconcile_last_closed_hour() -> str:
    end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=1)
    args = [start.isoformat(), end.isoformat()]
    fresh = compose("exec", "-T", "airflow", *RECON_CLI, "freshness", *args)
    if fresh.returncode != 0:
        raise CheckFailed(
            f"{start:%H:%M} hour not reconcilable yet: {fresh.stdout.strip() or fresh.stderr.strip()}"
        )
    result = compose(
        "exec", "-T", "airflow", *RECON_CLI, "reconcile", *args, "--run-id", f"smoke__{start:%Y%m%dT%H}"
    )
    if result.returncode != 0:
        raise CheckFailed(f"reconcile failed: {result.stderr.strip()[-300:]}")
    summary = json.loads(result.stdout)
    detail = (
        f"{start:%H:%M}-{end:%H:%M} {summary['status']}: "
        f"{summary['windows_match']}/{summary['windows_total']} windows exact, "
        f"completeness {summary['completeness_pct']:.4f} %"
    )
    if summary["status"] not in ("OK", "LATE_ONLY"):
        raise CheckFailed(detail)
    return detail


def flink_lag_low() -> str:
    query = 'sum by (consumergroup) (kafka_consumergroup_lag{consumergroup=~"flink-.*"})'
    lags = {r["metric"]["consumergroup"]: float(r["value"][1]) for r in prometheus(query)}
    if len(lags) < 2 * len(EXPECTED_JOBS):
        raise CheckFailed(f"lag reported for {len(lags)} of {2 * len(EXPECTED_JOBS)} Flink consumer groups")
    high = {g: int(v) for g, v in lags.items() if v >= MAX_FLINK_LAG}
    if high:
        raise CheckFailed(f"lag >= {MAX_FLINK_LAG}: {high}")
    return f"max lag {int(max(lags.values()))} across {len(lags)} groups"


CHECKS: list[tuple[str, Callable[[], str], bool]] = [
    ("services healthy", services_healthy, False),
    ("flink jobs running", flink_jobs_running, True),
    ("bars for 2 exchanges x 3 assets", bars_cover_all_pairs, True),
    ("whale trades", at_least_one("whale trades", "SELECT count() FROM flux.whale_trades FINAL"), True),
    (
        "arbitrage signals",
        at_least_one("arbitrage signals", "SELECT count() FROM flux.arbitrage_signals"),
        True,
    ),
    (
        "SOL double bottom",
        at_least_one(
            "SOL double bottoms", "SELECT count() FROM flux.double_bottoms FINAL WHERE asset = 'SOL'"
        ),
        True,
    ),  # noqa: E501
    ("no ingest errors", no_ingest_errors, False),
    ("iceberg rows", lake_has_rows, True),
    ("reconcile last closed hour", reconcile_last_closed_hour, True),
    ("flink consumer lag", flink_lag_low, True),
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--timeout", type=float, default=360, help="overall budget in seconds")
    args = parser.parse_args()
    deadline = time.monotonic() + args.timeout
    results: list[tuple[str, str, str, float]] = []
    failed = False
    for name, check, retry in CHECKS:
        started = time.monotonic()
        while True:
            try:
                detail, status = check(), "PASS"
                break
            except (CheckFailed, OSError, ValueError, KeyError) as e:
                detail, status = (
                    f"{type(e).__name__}: {e}" if not isinstance(e, CheckFailed) else str(e),
                    "FAIL",
                )
                if not retry or time.monotonic() + 10 > deadline:
                    break
                time.sleep(10)
        results.append((name, status, detail, time.monotonic() - started))
        print(f"{status}  {name:<32} {detail}", flush=True)
        if status == "FAIL":
            failed = True
            break

    print("\n" + f"{'CHECK':<32} {'RESULT':<6} {'TIME':>6}  DETAIL")
    for name, status, detail, took in results:
        print(f"{name:<32} {status:<6} {took:>5.0f}s  {detail}")
    skipped = [name for name, *_ in CHECKS[len(results) :]]
    if skipped:
        print(f"not run: {', '.join(skipped)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
