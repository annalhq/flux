import pytest

from flink.submit import JOBS, Job, RenderError, explain_script, job_variables, render, statements, substitute


def test_substitute_resolves_nested_references():
    assert substitute("a=${A}", {"A": "${B}!", "B": "x"}) == "a=x!"


def test_substitute_fails_on_missing_variable():
    with pytest.raises(RenderError, match="MISSING"):
        substitute("x ${MISSING} y", {})


def test_job_variables_require_checkpoint_interval():
    with pytest.raises(RenderError, match="SIGNAL_CHECKPOINT_INTERVAL"):
        job_variables(Job("bars_1m", 2, "SIGNAL_CHECKPOINT_INTERVAL"), {})


def test_per_job_values_override_env(env):
    values = job_variables(JOBS["bars_1m"], {**env, "JOB_NAME": "wrong", "PARALLELISM": "9"})
    assert values["JOB_NAME"] == "bars_1m"
    assert values["PARALLELISM"] == "2"
    assert values["CHECKPOINT_INTERVAL"] == env["SIGNAL_CHECKPOINT_INTERVAL"]


def test_render_contains_job_identity(env):
    sql = render(JOBS["bars_1m"], env)
    assert "SET 'pipeline.name' = 'bars_1m';" in sql
    assert "'properties.group.id' = 'flink-bars_1m-binance'" in sql
    assert "'sink.transactional-id-prefix' = 'flux-bars_1m'" in sql


def test_explain_script_rewrites_only_inserts(env):
    script = explain_script(render(JOBS["bars_1m"], env))
    stmts = statements(script)
    assert sum(s.startswith("EXPLAIN INSERT") for s in stmts) == 1
    assert not any(s.startswith("INSERT") for s in stmts)


def test_explain_requires_insert():
    with pytest.raises(RenderError):
        explain_script("SET 'a' = 'b';\n")


def test_statements_ignore_comments():
    assert statements("-- c\nSELECT 1;\n-- only a comment;\n\nSELECT 2;") == ["SELECT 1", "SELECT 2"]
