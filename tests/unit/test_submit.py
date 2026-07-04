import pytest

from flink.submit import (
    JOBS,
    Job,
    RenderError,
    explain_script,
    job_variables,
    latest_restore_point,
    render,
    statements,
    substitute,
)


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


def test_lake_job_includes_catalog_and_own_checkpoint_interval(env):
    sql = render(JOBS["lake_sink"], {**env, "LAKE_CHECKPOINT_INTERVAL": "60 s"})
    assert sql.index("CREATE CATALOG lake") < sql.index("INSERT INTO lake.market.trades")
    assert "SET 'execution.checkpointing.interval' = '60 s';" in sql


def test_restore_path_is_set_before_any_statement_runs(env):
    sql = render(JOBS["bars_1m"], env, restore_from="s3://flink-checkpoints/checkpoints/bars_1m/abc/chk-7")
    restore = sql.index(
        "SET 'execution.savepoint.path' = 's3://flink-checkpoints/checkpoints/bars_1m/abc/chk-7';"
    )
    assert restore < sql.index("CREATE TEMPORARY TABLE")
    assert "execution.savepoint.path" not in render(JOBS["bars_1m"], env)


def test_checkpoints_are_grouped_by_job_name(env):
    assert "SET 'state.checkpoints.dir' = 's3://flink-checkpoints/checkpoints/bars_1m';" in render(
        JOBS["bars_1m"], env
    )


def test_latest_restore_point_picks_newest_of_checkpoints_and_savepoints():
    points = [
        ("2026-11-11T10:00:00Z", "s3://flink-checkpoints/checkpoints/bars_1m/j1/chk-9/_metadata"),
        ("2026-11-11T12:00:00Z", "s3://flink-checkpoints/savepoints/bars_1m/savepoint-1a2b3c-d4e5/_metadata"),
        ("2026-11-11T11:00:00Z", "s3://flink-checkpoints/checkpoints/bars_1m/j2/chk-3/_metadata"),
    ]
    assert latest_restore_point(points) == "s3://flink-checkpoints/savepoints/bars_1m/savepoint-1a2b3c-d4e5"
    assert latest_restore_point([]) is None
