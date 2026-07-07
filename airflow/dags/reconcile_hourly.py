"""Hourly batch-vs-stream reconciliation.

For each closed hour (the DAG run's data interval), compares every (exchange, asset, 1-minute
window) in the Iceberg lake against flux.bars_1m in ClickHouse, exactly: counts, volume and
notional. Results land in flux.recon_windows and flux.recon_runs. Backfill older hours with
`airflow dags backfill` or `make recon HOUR=...`.
"""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sensors.python import PythonSensor

# Reconciliation dependencies live in their own virtualenv (see infra/airflow/Dockerfile); tasks
# call its CLI so Airflow's interpreter never imports them.
RECON_PYTHON = "/opt/recon-venv/bin/python"
INTERVAL = {"start": "{{ data_interval_start.isoformat() }}", "end": "{{ data_interval_end.isoformat() }}"}


def _recon_cli(*args: str) -> subprocess.CompletedProcess:
    result = subprocess.run([RECON_PYTHON, "-m", "flux_recon.cli", *args], capture_output=True, text=True)
    if result.stderr.strip():
        print(result.stderr.strip())
    return result


def _fresh(start: str, end: str) -> bool:
    result = _recon_cli("freshness", start, end)
    print(result.stdout.strip())
    if result.returncode not in (0, 1):
        raise RuntimeError(f"freshness check failed with exit code {result.returncode}")
    return result.returncode == 0


@dag(
    dag_id="reconcile_hourly",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["quality", "lakehouse"],
    doc_md=__doc__,
)
def reconcile_hourly():
    wait_for_freshness = PythonSensor(
        task_id="wait_for_freshness",
        python_callable=_fresh,
        op_kwargs=INTERVAL,
        mode="reschedule",
        poke_interval=60,
        timeout=30 * 60,
    )

    @task
    def reconcile(start: str, end: str, run_id: str) -> dict:
        result = _recon_cli("reconcile", start, end, "--run-id", run_id)
        if result.returncode != 0:
            raise RuntimeError(f"reconcile failed with exit code {result.returncode}")
        return json.loads(result.stdout)

    @task
    def assert_quality(summary: dict) -> None:
        status = summary["status"]
        print(json.dumps(summary, indent=2))
        if status == "NO_DATA":
            raise AirflowSkipException("no trades in this interval on either side")
        if status == "FAILED":
            raise AirflowFailException(
                f"reconciliation failed: missing={summary['windows_missing']} "
                f"exceed={summary['windows_exceed']} mismatch={summary['windows_mismatch']} "
                f"completeness={summary['completeness_pct']:.4f}%"
            )
        if status == "LATE_ONLY":
            print(
                f"WARNING: {summary['windows_late']} windows lost late events; "
                f"completeness {summary['completeness_pct']:.4f}%"
            )

    summary = reconcile(start=INTERVAL["start"], end=INTERVAL["end"], run_id="{{ run_id }}")
    wait_for_freshness >> summary
    assert_quality(summary)


reconcile_hourly()
