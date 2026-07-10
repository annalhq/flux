"""Alert rules and provisioned dashboards stay consistent with the platform they observe."""

import json
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
ALERTS = yaml.safe_load((ROOT / "infra/prometheus/alerts.yml").read_text())
DATASOURCES = yaml.safe_load((ROOT / "infra/grafana/provisioning/datasources/datasources.yaml").read_text())
DASHBOARDS = sorted((ROOT / "infra/grafana/dashboards").glob("*.json"))
DDL = "\n".join(p.read_text() for p in (ROOT / "infra/clickhouse/ddl").glob("*.sql"))
CLICKHOUSE_OBJECTS = set(re.findall(r"(?:TABLE|VIEW)\s+(?:IF NOT EXISTS\s+)?(flux\.\w+)", DDL))

RULES = [rule for group in ALERTS["groups"] for rule in group["rules"]]


@pytest.mark.parametrize("rule", RULES, ids=[r["alert"] for r in RULES])
def test_alert_has_severity_and_summary(rule):
    assert rule["labels"]["severity"] in ("warning", "critical")
    assert rule["annotations"]["summary"]


def test_alert_names_unique():
    names = [r["alert"] for r in RULES]
    assert len(names) == len(set(names))


def _panels(dashboard: dict):
    for panel in dashboard["panels"]:
        yield panel
        yield from panel.get("panels", [])


@pytest.mark.parametrize("path", DASHBOARDS, ids=[p.name for p in DASHBOARDS])
def test_dashboard_is_provisionable(path):
    dashboard = json.loads(path.read_text())
    assert dashboard["id"] is None and dashboard["uid"].startswith("flux-")
    known = {ds["uid"] for ds in DATASOURCES["datasources"]}
    ids = [p["id"] for p in _panels(dashboard)]
    assert len(ids) == len(set(ids))
    for panel in _panels(dashboard):
        for target in panel.get("targets", []):
            assert target["datasource"]["uid"] in known, panel["title"]


@pytest.mark.parametrize("path", DASHBOARDS, ids=[p.name for p in DASHBOARDS])
def test_clickhouse_queries_reference_existing_objects(path):
    dashboard = json.loads(path.read_text())
    sql = [t["rawSql"] for p in _panels(dashboard) for t in p.get("targets", []) if "rawSql" in t]
    sql += [a["target"]["rawSql"] for a in dashboard.get("annotations", {}).get("list", []) if "target" in a]
    sql += [v["query"] for v in dashboard.get("templating", {}).get("list", [])]
    for statement in sql:
        for obj in re.findall(r"\bflux\.\w+", statement):
            assert obj in CLICKHOUSE_OBJECTS, f"{path.name}: {obj} is not created by the DDL"
