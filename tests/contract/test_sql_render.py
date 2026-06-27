"""Every Flink job renders completely and keeps its sink identity unique."""

import re

import pytest

from flink.submit import JOBS, render, statements

JOB_NAMES = sorted(JOBS)


@pytest.mark.parametrize("name", JOB_NAMES)
def test_job_renders_without_placeholders(name, env):
    sql = render(JOBS[name], env)
    assert "${" not in sql
    assert any(s.upper().startswith("INSERT") for s in statements(sql))


def test_transactional_id_prefixes_are_unique(env):
    prefixes = []
    for name in JOB_NAMES:
        prefixes += re.findall(r"'sink\.transactional-id-prefix'\s*=\s*'([^']+)'", render(JOBS[name], env))
    assert prefixes and len(prefixes) == len(set(prefixes))


def test_every_kafka_sink_is_exactly_once(env):
    for name in JOB_NAMES:
        for stmt in statements(render(JOBS[name], env)):
            if stmt.upper().startswith("CREATE TEMPORARY TABLE SINK_"):
                assert "'sink.delivery-guarantee' = 'exactly-once'" in stmt, name
                assert "'sink.transactional-id-prefix'" in stmt, name
