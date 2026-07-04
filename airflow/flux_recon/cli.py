"""Operational commands: `python -m flux_recon.cli lake-stats`."""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime

from flux_recon import lake


def lake_stats() -> int:
    table = lake.trades()
    snapshot = table.current_snapshot()
    if snapshot is None:
        print(f"{lake.TRADES_TABLE}: no snapshots yet")
        return 1
    summary = snapshot.summary or {}
    partitions = table.inspect.partitions()
    committed = datetime.fromtimestamp(snapshot.timestamp_ms / 1000, UTC)
    age = (datetime.now(UTC) - committed).total_seconds()
    rows = [
        ("table", lake.TRADES_TABLE),
        ("location", table.metadata.location),
        ("partition spec", ", ".join(f.name for f in table.spec().fields)),
        ("rows", summary.get("total-records", "?")),
        ("data files", summary.get("total-data-files", "?")),
        ("partitions", partitions.num_rows),
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
    sub.add_parser("lake-stats", help="row, partition and snapshot counts of market.trades")
    args = parser.parse_args()
    if args.command == "lake-stats":
        return lake_stats()
    return 2


if __name__ == "__main__":
    sys.exit(main())
