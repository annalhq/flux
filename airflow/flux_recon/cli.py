"""Operational commands, run with the reconciliation interpreter:

python -m flux_recon.cli lake-stats
python -m flux_recon.cli freshness START END     exit 0 when the interval can be reconciled
python -m flux_recon.cli reconcile START END [--run-id ID]
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime

from flux_recon import lake
from flux_recon.reconcile import freshness, parse_instant, reconcile


def lake_stats() -> int:
    table = lake.trades()
    snapshot = table.current_snapshot()
    if snapshot is None:
        print(f"{lake.TRADES_TABLE}: no snapshots yet")
        return 1
    summary = snapshot.summary or {}
    committed = datetime.fromtimestamp(snapshot.timestamp_ms / 1000, UTC)
    age = (datetime.now(UTC) - committed).total_seconds()
    rows = [
        ("table", lake.TRADES_TABLE),
        ("location", table.metadata.location),
        ("partition spec", ", ".join(f.name for f in table.spec().fields)),
        ("rows", summary.get("total-records", "?")),
        ("data files", summary.get("total-data-files", "?")),
        ("partitions", table.inspect.partitions().num_rows),
        ("nessie commits", lake.nessie_commits()),
        ("latest commit", f"{committed:%Y-%m-%d %H:%M:%S} UTC ({age:.0f} s ago)"),
    ]
    width = max(len(k) for k, _ in rows)
    for key, value in rows:
        print(f"{key:<{width}}  {value}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="flux_recon.cli")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("lake-stats", help="row, partition and commit counts of market.trades")
    for name in ("freshness", "reconcile"):
        p = sub.add_parser(name)
        p.add_argument(
            "start", type=parse_instant, help="ISO-8601 with offset, e.g. 2026-11-11T12:00:00+00:00"
        )
        p.add_argument("end", type=parse_instant)
        if name == "reconcile":
            p.add_argument("--run-id", default=f"manual__{datetime.now(UTC):%Y%m%dT%H%M%S}")
    args = parser.parse_args()

    if args.command == "lake-stats":
        return lake_stats()
    if args.command == "freshness":
        ready, reason = freshness(args.start, args.end)
        print(reason)
        return 0 if ready else 1
    print(json.dumps(reconcile(args.start, args.end, args.run_id), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
