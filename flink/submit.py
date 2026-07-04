"""Render Flink SQL jobs and submit them idempotently through the SQL client.

submit.py render [JOB...]   write flink/sql/.rendered/<job>.sql
submit.py up [JOB...]       render and submit jobs that are not already active, restoring
                            each from its newest retained checkpoint (--fresh to skip,
                            --from PATH to restore from a specific checkpoint or savepoint)
submit.py down [JOB...]     stop jobs with a savepoint; the next `up` restores from it
submit.py status            job state, uptime and checkpoint health
submit.py explain JOB       print the optimized plan of the job's INSERT
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "flink" / "sql"
RENDERED_DIR = SQL_DIR / ".rendered"
CONTAINER_RENDERED_DIR = "/opt/flink/sql/.rendered"
COMMON_FILES = ("00_settings.sql", "10_sources.sql", "20_views.sql")
SNIPPETS = {"KAFKA_JSON_SINK": "kafka_json_sink.sql"}
ACTIVE_STATES = {"CREATED", "INITIALIZING", "RUNNING", "RESTARTING", "RECONCILING"}
SUBMIT_TIMEOUT_S = 60
CHECKPOINT_BUCKET = "flink-checkpoints"
RESTORE_PREFIXES = ("checkpoints", "savepoints")
SAVEPOINT_TIMEOUT_S = 120

_VAR = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


@dataclass(frozen=True)
class Job:
    name: str
    parallelism: int
    checkpoint_var: str
    includes: tuple[str, ...] = ()


JOBS = {
    job.name: job
    for job in (
        Job("bars_1m", 2, "SIGNAL_CHECKPOINT_INTERVAL"),
        Job("whales", 1, "SIGNAL_CHECKPOINT_INTERVAL"),
        Job("spreads", 2, "SIGNAL_CHECKPOINT_INTERVAL"),
        Job("double_bottom", 1, "SIGNAL_CHECKPOINT_INTERVAL"),
        Job("lake_sink", 1, "LAKE_CHECKPOINT_INTERVAL", includes=("lake/00_catalog.sql",)),
    )
}


class RenderError(ValueError):
    pass


def substitute(text: str, values: Mapping[str, str], max_depth: int = 5) -> str:
    """Resolve ${VAR} references, including references inside substituted values."""
    for _ in range(max_depth):
        rendered = _VAR.sub(lambda m: values.get(m.group(1), m.group(0)), text)
        if rendered == text:
            break
        text = rendered
    unresolved = sorted(set(_VAR.findall(text)))
    if unresolved or "${" in text:
        raise RenderError(f"unresolved variables: {', '.join(unresolved) or text[text.index('${') :][:40]}")
    return text


def job_variables(job: Job, env: Mapping[str, str]) -> dict[str, str]:
    if job.checkpoint_var not in env:
        raise RenderError(f"{job.name}: {job.checkpoint_var} is not set")
    values = {k: v for k, v in env.items() if v is not None}
    values.update(
        JOB_NAME=job.name,
        PARALLELISM=str(job.parallelism),
        CHECKPOINT_INTERVAL=env[job.checkpoint_var],
    )
    for var, filename in SNIPPETS.items():
        values[var] = (SQL_DIR / "common" / filename).read_text().rstrip()
    return values


def render(job: Job, env: Mapping[str, str], restore_from: str | None = None) -> str:
    parts = [(SQL_DIR / "common" / name).read_text() for name in COMMON_FILES]
    if restore_from:
        parts.insert(1, f"SET 'execution.savepoint.path' = '{restore_from}';\n")
    parts += [(SQL_DIR / name).read_text() for name in job.includes]
    parts.append((SQL_DIR / "jobs" / f"{job.name}.sql").read_text())
    return substitute("\n".join(parts), job_variables(job, env))


def statements(sql: str) -> list[str]:
    out = []
    for chunk in re.split(r";[ \t]*(?:\n|$)", sql):
        code = "\n".join(line for line in chunk.splitlines() if not line.strip().startswith("--")).strip()
        if code:
            out.append(code)
    return out


def explain_script(sql: str) -> str:
    """Same script with every INSERT replaced by EXPLAIN INSERT, so nothing is submitted."""
    stmts = statements(sql)
    if not any(s.upper().startswith("INSERT") for s in stmts):
        raise RenderError("job has no INSERT statement")
    return "".join(f"{'EXPLAIN ' if s.upper().startswith('INSERT') else ''}{s};\n" for s in stmts)


def write_rendered(name: str, sql: str) -> Path:
    RENDERED_DIR.mkdir(parents=True, exist_ok=True)
    path = RENDERED_DIR / name
    path.write_text(sql)
    return path


def load_env() -> dict[str, str]:
    from dotenv import dotenv_values

    return {**{k: v for k, v in dotenv_values(ROOT / ".env").items() if v is not None}, **os.environ}


def resolve_jobs(names: list[str]) -> list[Job]:
    unknown = [n for n in names if n not in JOBS]
    if unknown:
        raise SystemExit(f"unknown job(s): {', '.join(unknown)}; known: {', '.join(JOBS)}")
    return [JOBS[n] for n in names] if names else list(JOBS.values())


def latest_restore_point(entries: list[tuple[str, str]]) -> str | None:
    """Newest complete checkpoint or savepoint from (lastModified, metadata path) pairs."""
    if not entries:
        return None
    return max(entries)[1].removesuffix("/_metadata")


def restore_points(job_name: str) -> list[tuple[str, str]]:
    """Complete checkpoints and savepoints of a job, as (lastModified, s3 path of _metadata).

    Layout: <bucket>/checkpoints/<job>/<job_id>/chk-<n>/ and <bucket>/savepoints/<job>/savepoint-*/;
    only directories containing _metadata are complete.
    """
    listings = " ; ".join(
        f"mc ls --recursive --json flux/{CHECKPOINT_BUCKET}/{prefix}/{job_name}/"
        for prefix in RESTORE_PREFIXES
    )
    script = (
        f'mc alias set flux http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && '
        f"{{ {listings} ; }}"
    )
    cmd = ["docker", "compose", "exec", "-T", "minio", "sh", "-c", script]
    result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    points = []
    for line in result.stdout.splitlines():
        entry = json.loads(line)
        if entry.get("status") != "success" or entry.get("type") != "file":
            continue
        if not entry["key"].endswith("_metadata"):
            continue
        base = entry["url"].split(f"/{CHECKPOINT_BUCKET}/", 1)[1]
        points.append((entry["lastModified"], f"s3://{CHECKPOINT_BUCKET}/{base}{entry['key']}"))
    return points


class FlinkRest:
    def __init__(self, url: str) -> None:
        self.url = url.rstrip("/")

    def get(self, path: str) -> dict:
        with urllib.request.urlopen(f"{self.url}{path}", timeout=10) as resp:
            return json.loads(resp.read())

    def post(self, path: str, body: dict) -> dict:
        req = urllib.request.Request(
            f"{self.url}{path}",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())

    def jobs(self) -> list[dict]:
        return self.get("/jobs/overview")["jobs"]

    def active(self, name: str) -> dict | None:
        matches = [j for j in self.jobs() if j["name"] == name and j["state"] in ACTIVE_STATES]
        return max(matches, key=lambda j: j["start-time"]) if matches else None


def sql_client(script: str) -> str:
    cmd = [
        "docker", "compose", "exec", "-T", "flink-jobmanager",
        "/opt/flink/bin/sql-client.sh", "-f", f"{CONTAINER_RENDERED_DIR}/{script}",
    ]  # fmt: skip
    result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    output = result.stdout + result.stderr
    if result.returncode != 0 or "[ERROR]" in output:
        raise RuntimeError(f"sql-client failed for {script}:\n{output.strip()}")
    return output


def cmd_render(names: list[str], env: Mapping[str, str]) -> None:
    for job in resolve_jobs(names):
        print(f"{job.name}: {write_rendered(f'{job.name}.sql', render(job, env)).relative_to(ROOT)}")


def cmd_up(
    names: list[str],
    env: Mapping[str, str],
    rest: FlinkRest,
    fresh: bool = False,
    restore_from: str | None = None,
) -> None:
    jobs = resolve_jobs(names)
    if restore_from and len(jobs) != 1:
        raise RenderError("--from needs exactly one job")
    for job in jobs:
        if existing := rest.active(job.name):
            print(f"{job.name}: already {existing['state']} ({existing['jid']})")
            continue
        source = restore_from or (None if fresh else latest_restore_point(restore_points(job.name)))
        print(f"{job.name}: {'restoring from ' + source if source else 'starting from committed offsets'}")
        write_rendered(f"{job.name}.sql", render(job, env, restore_from=source))
        sql_client(f"{job.name}.sql")
        deadline = time.monotonic() + SUBMIT_TIMEOUT_S
        while time.monotonic() < deadline:
            current = rest.active(job.name)
            if current and current["state"] == "RUNNING":
                print(f"{job.name}: RUNNING ({current['jid']})")
                break
            time.sleep(2)
        else:
            raise RuntimeError(f"{job.name}: not RUNNING after {SUBMIT_TIMEOUT_S} s")


def cmd_down(names: list[str], rest: FlinkRest) -> None:
    for job in resolve_jobs(names):
        current = rest.active(job.name)
        if not current:
            print(f"{job.name}: not running")
            continue
        target = f"s3://{CHECKPOINT_BUCKET}/savepoints/{job.name}"
        trigger = rest.post(f"/jobs/{current['jid']}/stop", {"drain": False, "targetDirectory": target})
        deadline = time.monotonic() + SAVEPOINT_TIMEOUT_S
        while time.monotonic() < deadline:
            status = rest.get(f"/jobs/{current['jid']}/savepoints/{trigger['request-id']}")
            if status["status"]["id"] == "COMPLETED":
                operation = status.get("operation", {})
                if "failure-cause" in operation:
                    raise RuntimeError(
                        f"{job.name}: savepoint failed: {operation['failure-cause'].get('class')}"
                    )
                print(f"{job.name}: stopped, savepoint {operation['location']}")
                break
            time.sleep(2)
        else:
            raise RuntimeError(f"{job.name}: savepoint not completed after {SAVEPOINT_TIMEOUT_S} s")


def _age(ms: int | None) -> str:
    if not ms:
        return "-"
    seconds = int(time.time() - ms / 1000)
    return f"{seconds // 3600}h{seconds % 3600 // 60:02d}m" if seconds >= 3600 else f"{seconds}s"


def cmd_status(rest: FlinkRest) -> None:
    jobs = sorted(rest.jobs(), key=lambda j: (j["state"] not in ACTIVE_STATES, j["name"]))
    print(f"{'JOB':<16} {'STATE':<12} {'UPTIME':>8} {'LAST CKPT':>10} {'CKPTS':>6} {'FAILED':>6}  ID")
    for j in jobs:
        ckpt = rest.get(f"/jobs/{j['jid']}/checkpoints")
        latest = (ckpt.get("latest") or {}).get("completed") or {}
        counts = ckpt.get("counts", {})
        uptime = _age(j["start-time"]) if j["state"] in ACTIVE_STATES else "-"
        print(
            f"{j['name']:<16} {j['state']:<12} {uptime:>8} {_age(latest.get('latest_ack_timestamp')):>10} "
            f"{counts.get('completed', 0):>6} {counts.get('failed', 0):>6}  {j['jid']}"
        )
    if not jobs:
        print("(no jobs)")


def cmd_explain(name: str, env: Mapping[str, str]) -> None:
    [job] = resolve_jobs([name])
    write_rendered(f"{job.name}.explain.sql", explain_script(render(job, env)))
    print(sql_client(f"{job.name}.explain.sql"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Flux Flink SQL job submitter")
    parser.add_argument("--rest", default=os.environ.get("FLINK_REST", "http://localhost:8080"))
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("render").add_argument("jobs", nargs="*")
    up = sub.add_parser("up")
    up.add_argument("jobs", nargs="*")
    restore = up.add_mutually_exclusive_group()
    restore.add_argument("--fresh", action="store_true", help="ignore retained checkpoints")
    restore.add_argument(
        "--from", dest="restore_from", metavar="PATH", help="checkpoint or savepoint to restore"
    )
    sub.add_parser("down").add_argument("jobs", nargs="*")
    sub.add_parser("status")
    sub.add_parser("explain").add_argument("job")
    args = parser.parse_args()

    env, rest = load_env(), FlinkRest(args.rest)
    try:
        if args.command == "render":
            cmd_render(args.jobs, env)
        elif args.command == "up":
            cmd_up(args.jobs, env, rest, fresh=args.fresh, restore_from=args.restore_from)
        elif args.command == "down":
            cmd_down(args.jobs, rest)
        elif args.command == "status":
            cmd_status(rest)
        elif args.command == "explain":
            cmd_explain(args.job, env)
    except (RenderError, RuntimeError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
