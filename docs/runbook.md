# Runbook

All commands run from the repository root. Host ports are listed in the README.

## Start and stop

| Task | Command |
| --- | --- |
| Fresh demo (wipes all data) | `make demo` (`SYNTH_LATE_RATIO=0 make demo` for exact reconciliation) |
| Fresh start with live exchange data | `make live` |
| Start or resume without wiping | `make up && make producers` |
| Stop, keep data | `make jobs-down && make down` |
| Status | `make ps`, `make jobs-status`, `make lake-stats` |
| End-to-end check | `make smoke` |

`make up` is health-gated and idempotent: it starts containers, waits for healthchecks, runs
`scripts/bootstrap.py` (topics, schemas, ClickHouse DDL) and submits any Flink job that is not running.

Do not run live and synthetic producers against the same topics: event times would interleave and the
watermarks would drop data. Switch modes with `make demo` or `make live`, which start from empty volumes.

## Flink jobs

| Task | Command |
| --- | --- |
| Stop all jobs with savepoints | `make jobs-down` |
| Start jobs, restoring each from its newest checkpoint or savepoint | `make jobs` |
| Start one job ignoring saved state (reads from committed Kafka offsets) | `uv run python flink/submit.py up JOB --fresh` |
| Restore one job from a specific location | `uv run python flink/submit.py up JOB --from s3://flink-checkpoints/...` |
| Show the optimized plan | `uv run python flink/submit.py explain JOB` |

Checkpoints are kept under `s3://flink-checkpoints/checkpoints/<job>/` and savepoints under
`s3://flink-checkpoints/savepoints/<job>/`. The JobManager has no HA: after a host reboot the jobs are
gone but their last checkpoints are not, so `make jobs` restores them.

Changing Flink memory or connector jars restarts the cluster; always `make jobs-down` first.

Avoid `--fresh` while the job's topics hold a large backlog: the readers that get their partitions
first move the watermark ahead and the others lose data as late. Restoring from a checkpoint or
savepoint has no such race.

If a job's SQL changed so that state is no longer compatible, the restore fails. Either keep the old
state by reverting, or start it `--fresh` (open windows at that moment are lost) and expect the
reconciliation of that hour to show it.

## Reconciliation

| Task | Command |
| --- | --- |
| Reconcile one closed hour | `make recon HOUR=2026-11-11T13` |
| Backfill a range through Airflow | `docker compose exec airflow airflow dags backfill reconcile_hourly -s 2026-11-11T10:00:00+00:00 -e 2026-11-11T13:00:00+00:00` |
| Latest results | `docker compose exec clickhouse clickhouse-client -q "SELECT * FROM flux.recon_runs FINAL ORDER BY interval_start DESC LIMIT 5 FORMAT Vertical"` |
| Windows that did not match | `... -q "SELECT * FROM flux.recon_windows FINAL WHERE status != 'MATCH' ORDER BY window_start"` |

`make recon` does not create an Airflow run. Do not use `airflow dags test` for this: a manual run
occupies its logical date and the scheduler then skips that interval.

Reading a run status:

| Status | Meaning | Action |
| --- | --- | --- |
| `OK` | Every window equal | none |
| `LATE_ONLY` | Some windows lost events behind the watermark; completeness ≥ 99.9 % | none; check `recon_windows` if the count is unusual |
| `FAILED` | A window is missing on one side, the stream exceeds the lake, values differ, or completeness < 99.9 % | inspect `recon_windows`; check the Flink job, ClickHouse ingest and `flux.ingest_errors` |
| `NO_DATA` | No trades in the hour | check producers |

A long gap in one Kafka partition (broker or host stall longer than the 10 s idle timeout) makes Flink
advance the watermark without that partition and drop its trades as late. It shows up as `LATE_DROPS`
concentrated in one asset.

## Producers and the DLQ

Producers wait for Kafka and Schema Registry before sending. Anything they cannot parse, serialize or
deliver goes to `dlq.producers` with the original payload (JSON envelope; see
`producers/schemas/dlq_envelope.json`). Inspect it in Kafka UI (http://localhost:8090) or with any
consumer. `ProducerDLQ` fires on any new DLQ message.

Trades that never reached the raw topics are invisible to reconciliation, because both sides agree they
are absent. The DLQ and the `ProducerSilent` / `ProducerTargetDown` alerts are the controls for that.

## Replay

| What | How |
| --- | --- |
| Rebuild a ClickHouse table from its topic | `TRUNCATE TABLE flux.<table>`, then reset the consumer group, e.g. `docker compose exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group clickhouse-bars-1m --topic agg.bars-1m --reset-offsets --to-earliest --execute` (detach the Kafka table first: `DETACH TABLE flux.kafka_bars_1m`, then `ATTACH`). `ReplacingMergeTree` keys make replays idempotent. |
| Recompute a Flink job from raw data | `uv run python flink/submit.py down JOB`, delete its consumer groups (`flink-<job>-binance`, `flink-<job>-coinbase`), then `submit.py up JOB --fresh`. Raw retention is 48 h. |

## Reset

`make clean` removes all containers and volumes: Kafka data, checkpoints, the lake, ClickHouse,
Airflow and Grafana state. `make demo` does this first.

## Resources

The stack is sized for a 7.5 GB host (~3.5 GB used with all jobs). Container memory limits do not cap
swap; if the host runs out of memory, Kafka loses its KRaft quorum and Flink drops data from stalled
partitions. Check `free -m` before adding jobs, and see `docs/decisions.md` for the sizing.
