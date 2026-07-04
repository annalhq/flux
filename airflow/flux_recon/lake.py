"""Read access to the Iceberg lake through Nessie's Iceberg REST catalog."""

from __future__ import annotations

import json
import os
import urllib.request
from collections.abc import Mapping

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.table import Table

TRADES_TABLE = "market.trades"


def catalog(env: Mapping[str, str] = os.environ) -> Catalog:
    return load_catalog(
        "lake",
        type="rest",
        uri=env.get("ICEBERG_REST_URI", "http://nessie:19120/iceberg"),
        warehouse=env.get("ICEBERG_WAREHOUSE", "warehouse"),
        **{
            "s3.endpoint": env.get("S3_ENDPOINT", "http://minio:9000"),
            "s3.access-key-id": env["MINIO_ROOT_USER"],
            "s3.secret-access-key": env["MINIO_ROOT_PASSWORD"],
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def trades(env: Mapping[str, str] = os.environ) -> Table:
    return catalog(env).load_table(TRADES_TABLE)


def nessie_commits(env: Mapping[str, str] = os.environ, branch: str = "main", limit: int = 10_000) -> int:
    """Commits on a Nessie branch. Nessie keeps table history as commits, not Iceberg snapshots."""
    base = env.get("ICEBERG_REST_URI", "http://nessie:19120/iceberg").removesuffix("/iceberg")
    url = f"{base}/api/v2/trees/{branch}/history?max-records={limit}"
    with urllib.request.urlopen(url, timeout=10) as resp:
        return len(json.load(resp)["logEntries"])
