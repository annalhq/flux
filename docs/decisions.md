# Decisions and [verify] outcomes

ADRs are summarised in `docs/rebuild/03-target-architecture.md`. This file records deviations
from the rebuild kit and the outcome of every **[verify]** item as it is resolved.

## Version pins resolved (2026-11-11)

| Component | Pin | Note |
| --- | --- | --- |
| Nessie | `ghcr.io/projectnessie/nessie:0.108.8` | Latest patch of the 0.108 line; has JDBC2 store + Iceberg REST. Config keys checked against `application.properties` at tag `nessie-0.108.8`. |
| MinIO | `pgsty/minio:RELEASE.2026-08-04T00-00-00Z` | **Deviation.** `minio/minio` and `minio/mc` are no longer published on Docker Hub. Compared `pgsty/minio` (active, ~4M pulls, matching `mc`), `coollabsio/minio` (last release 2025-10, no `mc`), `bitnamilegacy/minio` (frozen). Chose pgsty. |
| mc | `pgsty/mc:RELEASE.2026-11-09T00-00-00Z` | Same rationale. |
| kafka-exporter | `danielqsj/kafka-exporter:v1.9.0` | |
| Grafana ClickHouse plugin | `4.14.0` | Newest version in the grafana.com catalog compatible with Grafana 10.4.0 (4.14.1 is tagged on GitHub but not published; 4.15+ require Grafana >= 11.6). Provisioning uses v4 `jsonData` keys (`host`, `port`, `protocol`, `username`, `defaultDatabase`). |

## Other choices

- ClickHouse password is passed as `FLUX_CH_USER_PASSWORD`, not `CLICKHOUSE_PASSWORD`, because the
  image entrypoint rewrites the `default` user when `CLICKHOUSE_USER/PASSWORD` are set.
- Flink connector jars are fetched with `ADD --chmod=644` instead of `wget`, so the image build does not
  depend on tools present in the base image.
- Reconciliation dependencies live in `/opt/recon-venv` inside the Airflow image. Airflow 2.9.3's
  constraint file pins `pyiceberg==0.6.1`, which conflicts with the required `0.8.1`.
- Dockerfiles avoid BuildKit-only syntax (`ADD --chmod`) so they build with the legacy builder too.

## Verified (2026-11-11)

- Nessie JDBC2 store on Postgres persists across `docker compose restart nessie` (namespace survived).
- Nessie Iceberg REST answers at `http://nessie:19120/iceberg/v1/main/...`.
- Flink 1.19.1 ships `metrics-prometheus` in `plugins/`; both JM and TM expose `:9249`.
- Images ship the healthcheck tools used: `curl` (Flink, Nessie, Schema Registry, Airflow), `mc` (pgsty/minio), `wget` (Prometheus, Grafana).

## Ingestion (topics, schemas, producers)

### Deviations from the spec

- **Double-bottom drift schedule.** The spec's last phase (4 bars at +0.5 %) can never satisfy
  `B AS close_price > MAX(P.high_price)`: the first bar after the second dip closes at ~0.985 x
  reference while the neckline is ~0.994 x. Replaced with one +1.8 % breakout bar followed by three
  +0.3 % bars. Verified with the Python port of the pattern in `tests/unit/test_synthetic.py`: the
  spec schedule yields no match, the adjusted one matches at minute 60 on both exchanges, and pure
  noise yields none.
- **`flux.vwap_5m` column `volume` renamed to `total_volume`.** With the alias `sum(volume) AS volume`,
  ClickHouse resolves `volume` inside the VWAP expression to the alias and rejects the nested aggregate.
- **Synthetic restarts resume instead of replaying.** Kafka message timestamps are the trade's event
  time. On start, synthetic mode reads the newest timestamps in the raw topics and continues from the
  next minute boundary; trade ids are derived from the run's start so runs never collide. Without this,
  a container restart (`restart: unless-stopped`) would replay the 2 h backfill with duplicate ids and
  event times going backwards, which the watermarks would drop.
- **Producers do not auto-register schemas** (`auto.register.schemas=false`). Subjects are owned by
  `scripts/bootstrap.py`, which sets `BACKWARD` before registering.
- **Separate DLQ producer instance**, so a full trade queue (`BufferError`) cannot block dead-lettering.
- **Event-lag histogram is recorded in live mode only.** Synthetic event times are simulated, so the
  lag would measure the backfill offset rather than ingestion latency.

### Verified

- ClickHouse 24.3 Kafka engine reads `read_committed`: an aborted transaction on `agg.bars-1m` was never
  ingested, the committed one was (exact decimals, VWAP alias correct), and a malformed message in the
  same transaction went to `flux.ingest_errors` without stalling the consumer.
- Explicit partitioning: BTC/ETH/SOL land on partitions 0/1/2 of both raw topics.
- Bootstrap is idempotent; a second run reports every topic up to date and re-registers identical schemas.
- Synthetic backfill (2 h) catches up in about 2 minutes on this host; afterwards the newest event lags the
  wall clock by < 1 s. Restarting the producers resumes at the next minute with no event-time regression
  apart from the intended 30 s late events.

- **Coinbase `side` is the maker side** (spec item marked [verify]). Over 363 live `market_trades` updates
  compared with the `ticker` best bid/ask, 315 were `SELL` at the ask or `BUY` at the bid; 44 the other
  way (book moved between ticker and trade). The canonical view must therefore invert it:
  `taker_side = CASE WHEN UPPER(side) = 'BUY' THEN 'SELL' ELSE 'BUY' END`. The synthetic generator emits
  maker-side values to match.
- Live parsers against both exchanges (run inside the producer image): Binance median trade-to-ingest
  lag 303 ms, Coinbase 176 ms; Coinbase sent 300 snapshot trades on subscribe, all skipped;
  the `heartbeats` subscription is accepted.
- Networks that intercept TLS (e.g. a Fortinet firewall on exchange domains) make certificate
  verification fail by design. Mount the proxy's root CA and set `SSL_CERT_FILE`; never disable verification.

## Flink SQL: `bars_1m`

### Verified

- Plan (`submit.py explain bars_1m`): a single `WindowAggregate` (TUMBLE 1 min on `event_ts`) over the
  `UNION ALL` of both sources; watermarks `event_ts - 5 s`, idle timeout 10 s.
- `make jobs` is idempotent: a second run reports `already RUNNING` with the same job id.
- End-to-end exactness against the raw topics (all closed windows of a 2 h synthetic backfill):
  771 of 775 windows equal in `trade_count`, `volume` and `notional` to the last decimal; the other 4
  differ by exactly the 4 trades that arrived behind the watermark. No window has more in the stream
  than in the raw data, and none exists on only one side.
- The first checkpoint after submission can fail with "Not all required tasks are currently running";
  that is a trigger-before-start race, not a fault. Subsequent checkpoints complete every 10 s.

### Late-data behaviour

A trade is dropped only if its 1-minute window has already fired, i.e. the watermark has passed the
window end. With `SYNTH_LATE_RATIO` events stamped 30 s early and a 5 s watermark delay, only late
events in roughly the last 25 s of a minute are dropped, about 40 % of them in real time. During the
120x backfill, the periodic watermark trails event time further and almost none are dropped (4 of
~230). Reconciliation should therefore expect `LATE_DROPS` mostly in real-time hours.

### Resources

With all services and one job, host free memory is ~1.2 GB of 7.5 GB. Watch it as jobs are added;
the first lever is `taskmanager.memory.process.size`.

## Flink SQL: `whales`, `spreads`, `double_bottom`

### Plans (abbreviated, from `submit.py explain`)

`spreads` plans as a window join, so state is bounded by the open 5 s windows and no TTL is needed:

```text
Sink <- Calc <- WindowJoin
                 :- Exchange <- Calc <- WindowAggregate(TUMBLE 5 s) <- WindowTableFunction <- Union(binance_raw, coinbase_raw)
                 +- Exchange <- Calc <- WindowAggregate(TUMBLE 5 s) <- Reused
```

`double_bottom` plans a `Match` operator over the 1-minute window aggregate:

```text
Sink <- Calc <- Match(PARTITION BY exchange, asset ORDER BY window_time) <- Exchange <- Calc
     <- WindowAggregate(TUMBLE 1 min) <- Exchange <- Calc <- Union(binance_raw, coinbase_raw)
```

`whales` is a stateless filter: `Sink <- Union(Calc(filter) <- binance_raw, Calc(filter) <- coinbase_raw)`.

### Verified (2 h 19 min of synthetic data)

- 4 jobs RUNNING, 6 of 8 slots; per-job consumer groups visible in kafka-exporter.
- Whales: 46, exactly one per 3 sim-minutes, rotating over all 6 (exchange, asset) pairs, notional 500 000.
- Spreads: 4 983 windows; median spread ~2 bps, the configured Coinbase basis.
- Spread blowout: 35 ETH signals in sim minutes 30-32 at 24-32 bps.
- Double bottom: SOL on both exchanges, pattern start sim minute 59, confirmed at minute 74.
- `ingest_errors` = 0.

### Known limitation: arrival-order `LAST_VALUE`

`LAST_VALUE` (and `FIRST_VALUE`) follow arrival order, not event time. A late trade that is still inside
an open window becomes that window's last price even though it happened up to 30 s earlier.
With synthetic late events this produces a few arbitrage signals outside the scripted scenario
(1 ETH signal in minute 29 carrying blowout prices; BTC/ETH outliers of 10-12 bps; SOL outliers of
13-38 bps during the fast double-bottom drift). Bars' `open`/`close` are affected the same way; additive
measures (volume, notional, counts) are not. On live data late trades are rare, so the effect is small.
A fix is an event-time arg-max aggregate (for example a UDAF returning the price at `MAX(event_ts)`).

## Job lifecycle: checkpoints, savepoints, restore

- Each job checkpoints to `s3://flink-checkpoints/checkpoints/<job_name>/<job_id>/` (set per job in
  `00_settings.sql`), so retained checkpoints can be found by job name after the job id is gone.
- `submit.py down` stops jobs with a savepoint to `s3://flink-checkpoints/savepoints/<job_name>/`.
- `submit.py up` restores a job that is not running from its newest complete checkpoint or savepoint
  (a directory with `_metadata`). `--fresh` starts from committed Kafka offsets; `--from PATH` restores
  from a specific location.
- The JobManager has no HA, so a host reboot loses running jobs; their retained checkpoints survive in
  MinIO. Verified twice: after a reboot (restore from retained checkpoints) and after a planned restart
  (stop with savepoint, recreate Flink with new memory settings, auto-restore). Bars stayed exact across
  both: no window exceeds the raw data, no duplicates, nothing on one side only.

## Incident: host memory exhaustion (2026-11-11 12:33 UTC)

The host (7.5 GB RAM, 9.6 GB swap) ran out of swap; the kernel logged 10 s page-allocation stalls. All
three brokers lost the KRaft quorum at once. Partitions 0 and 2 (BTC, SOL) were unavailable for longer
than `table.exec.source.idle-timeout` (10 s) while partition 1 kept flowing, so Flink marked them idle,
advanced the watermark on ETH alone, and dropped 15-30 BTC/SOL trades per exchange in the 12:35 window
as late. Reconciliation will report that window, which is the intended behaviour: the idle timeout
trades completeness for liveness, and the batch side shows exactly what was lost.

Container memory limits do not cap swap, so the stack pushed the host into swap instead of one
container being OOM-killed. Footprint reduced to fit this host:

| Service | Before | After |
| --- | --- | --- |
| Kafka x3 | heap 512 MB, limit 1 GB | heap 384 MB, limit 768 MB |
| Flink TaskManager | process 3200 MB, managed fraction 0.4 | 2048 MB, 0.25 |
| Flink JobManager | process 1400 MB | 1024 MB; SQL client JVM `-Xmx256m` (`env.java.opts.client`) |
| Airflow | 2 webserver workers, limit 2 GB | 1 worker, 1.5 GB |
| Nessie | `-Xmx384m` | `-Xmx256m` |

With 5 jobs the containers use ~3.5 GB; host available memory ~1.8 GB.

## Lakehouse: `lake_sink` and `market.trades`

### Verified

- Flink's Iceberg REST catalog works against Nessie 0.108.8 (`uri=http://nessie:19120/iceberg`,
  `warehouse=warehouse`); no fallback from ADR-5 was needed. PyIceberg 0.8.1 reads the same table through
  the same endpoint without a branch suffix.
- Table partitioned by `event_date, exchange`; one commit per 60 s checkpoint.
- `lake_sink` kept checkpointing through `docker compose restart nessie`; the table and row count were
  unchanged after the restart (catalog persisted in Postgres).
- Exactness for the closed hour 12:00-13:00 UTC, lake vs raw topics, per exchange: equal row count,
  distinct trade ids, `Σ quantity` and `Σ price × quantity` (Coinbase includes the 35 synthetic duplicates
  on both sides).

### Findings

- **Nessie exposes only the current snapshot** in the Iceberg metadata it serves; table history is the
  Nessie commit log. `lake-stats` therefore reports Nessie commits on `main`, and Iceberg snapshot expiry
  does not apply. Old files are reclaimed with Nessie's GC tooling instead.
- **DuckDB decimal width:** `DECIMAL(18,8) * DECIMAL(18,8)` stays at width 18 and overflows on
  `sum(price * quantity)`. Cast both operands to `DECIMAL(38,8)` to get `DECIMAL(38,16)`, the type
  ClickHouse stores.
- Each commit writes one file per partition (two per minute), so file counts grow quickly; compaction is
  needed for long runs.

## Reconciliation (`reconcile_hourly`)

### Design

- `flux_recon/compare.py` is pure and unit-tested (written test-first). Statuses follow ADR-6 plus
  **`VALUE_MISMATCH`** (deviation): equal counts with different volume or notional. Late data cannot
  explain it, so it fails the run like `STREAM_EXCEEDS_BATCH`. `recon_runs.windows_mismatch` holds the count
  (added with an idempotent `ALTER ... ADD COLUMN IF NOT EXISTS`).
- Order of checks per window: `MATCH` (all equal), `STREAM_EXCEEDS_BATCH` (any stream measure larger),
  `LATE_DROPS` (fewer stream events), else `VALUE_MISMATCH`.
- Freshness is proved per (exchange, asset), not by wall clock: the stream side needs bars with
  `window_end >= interval_end`; the lake needs a committed trade at or after `interval_end + 1 min`.
  Raw partitions are ordered and each Iceberg commit is a consistent cut, so one committed trade past
  that point proves every earlier trade of the partition is in the lake. This also holds during the
  accelerated synthetic backfill, where wall-clock checks would pass before the data has arrived.
- Airflow tasks call `/opt/recon-venv/bin/python -m flux_recon.cli` instead of `@task.external_python`.
  The operator's probes of the venv (for Airflow and pendulum) print tracebacks on every run; a CLI
  boundary keeps logs clean and makes the same commands usable by hand.
- `make recon HOUR=H` runs the CLI (`freshness`, then `reconcile`) for the hour starting at H and writes
  the same result tables, but creates no Airflow DAG run. Deviation from the spec, which used
  `airflow dags test`: a manual run occupies its logical date, and Airflow 2 allows one run per logical
  date, so the scheduler silently skipped the matching scheduled interval (observed: `next_dagrun` jumped
  past 13:00 after a manual run with that date). Backfills through Airflow use `airflow dags backfill`,
  which creates runs with the scheduler's own logical dates.

### Verified

| Hour (UTC) | Status | Windows | Late windows | Completeness | Duplicates |
| --- | --- | --- | --- | --- | --- |
| 11:00 | LATE_ONLY | 360 | 1 | 99.9991 % | 34 |
| 12:00 | LATE_ONLY | 348 | 18 | 99.9019 % | 35 |
| 13:00 (scheduled run) | LATE_ONLY | 360 | 23 | 99.9767 % | 27 |

- Hour 13 ran mostly in real time, so more late events fell behind the watermark than during the
  backfill hours (23 windows vs 1), as predicted in the late-data section.
- Hour 12 includes the 12:35 memory incident (86 trades dropped behind the watermark): it appears as
  `LATE_DROPS`, just above the 99.9 % threshold.
- Negative test: a bar with `trade_count + 1` injected into `bars_1m` made the DAG fail with
  `STREAM_EXCEEDS_BATCH`; after removing it the re-run returned `LATE_ONLY` and replaced the stored results.

### Incident: producers dead-lettered trades at boot

After the reboot, producers started before Schema Registry answered. Every serialization failed with
`ConnectionError` for 36 s and 3 978 trades went to `dlq.producers` (sim minutes 12:39-12:41, which is
why hour 12 has 348 windows: those trades are absent on both sides, so reconciliation cannot see them;
the DLQ and its alert are the control for upstream loss). Fix: `TradeProducer.wait_until_ready()` blocks
until Kafka metadata and both value subjects resolve, with backoff. Re-tested by stopping Schema Registry,
restarting producers and starting the registry 25 s later: producers waited 50 s, then started; the DLQ
did not grow.

Restarting synthetic producers resumes at the next minute boundary after the newest event, so a restart
leaves up to one partial minute without trades. Both sides agree on it.

## Observability

- Alert rules (`infra/prometheus/alerts.yml`) were written against the metric names Prometheus actually
  scrapes (Flink `flink_jobmanager_job_*`, `flink_taskmanager_job_task_*`; kafka-exporter; ClickHouse
  `ClickHouseProfileEvents_*`) and validated with `promtool check rules`. Added to the spec's set:
  `KafkaBrokerMissing`, `ProducerTargetDown`, `FlinkBackpressured`, `ClickHouseKafkaRowsRejected`.
- **`ProducerSilent` as specified cannot fire when the producer is down**: the scrape target disappears,
  the gauge goes stale and `time() - gauge` returns nothing. The rule uses
  `max_over_time(flux_producer_last_event_timestamp_seconds[1h])` so the last known value still counts.
  Verified: producers stopped at 14:08:58, `ProducerSilent` firing for both exchanges by 14:13:15.
- Flink restart and checkpoint counts and Kafka offsets are gauges. Dashboards use `delta` / `deriv` on
  them (Prometheus flags `rate` / `increase` on gauges); counts are rounded because `increase` and `delta`
  extrapolate.
- Grafana ClickHouse plugin 4.14 query format: `editorType: "sql"`, `rawSql`, `format` 1 = table,
  0 = time series (one series per string column). Stat panels need `reduceOptions.fields: "/.*/"` to show
  string values such as the reconciliation status. Every panel query was run through `/api/ds/query`.

## Startup: watermarks on an empty topic and on a backlog

Two startup races, both found by reconciliation on clean `SYNTH_LATE_RATIO=0` demos:

1. **Jobs started on empty topics** mark every partition idle (10 s idle timeout). When the producers
   start, the first partitions to receive data drive the watermark alone; at 120x a partition arriving
   ~0.2 s later had ~23 simulated seconds behind the watermark (binance BTC lost 113 of 300 trades in the
   first minute; the hour reconciled `FAILED` at 99.85 %).
2. **Jobs started on a backlog** (tried as a fix for 1: producers first, jobs later) are worse. Flink's
   Kafka enumerator assigns partitions as source readers register, and a reader without partitions is
   idle. One reader started a few seconds early and raced through the backlog, moving the watermark about
   an hour ahead; the partitions of the other reader (binance p0, coinbase p2) then lost their first 64
   simulated minutes as late. The output topic held no bars for them, so ClickHouse ingest was not at fault.

Fix: jobs start before the producers (the spec's order), and the synthetic clock runs at real time for
its first `SYNTH_WARMUP_SECONDS` (15) before accelerating, so partition skew stays a fraction of a second
of event time, far inside the 5 s watermark delay. Consequence for operations: do not start a windowed
job `--fresh` on a large backlog; restore from a checkpoint or savepoint (state includes offsets, so
there is no race), or accept that reconciliation will report the loss. Watermark alignment
(`scan.watermark.alignment.*`) bounds drift between active splits but does not cover readers that have
no split yet.

## Exactly-once across a restart

On a clean demo, `bars_1m` was stopped with a savepoint at 15:02:04 and restored at 15:02:45 while the
producers kept writing. Reconciling 14:55-15:06: 66 of 66 windows exact, completeness 100 %, `OK`.

## Smoke test

`make smoke` (10 checks, 6-minute budget) passed on a clean `SYNTH_LATE_RATIO=0 make demo`, including
reconciliation of the hour 13:00-14:00: `OK`, 360 of 360 windows exact, completeness 100 %.
The first run caught a double URL-encoding of the Iceberg REST prefix (`main%7Cwarehouse` is returned
already encoded).

## Final verification (clean demo, `SYNTH_LATE_RATIO=0`, 4 h backfill)

| Hour (UTC) | Run | Status | Windows exact | Completeness |
| --- | --- | --- | --- | --- |
| 11:00 (from 11:13) | `airflow dags backfill` | OK | 282 / 282 | 100 % |
| 12:00 | `airflow dags backfill` | OK | 360 / 360 | 100 % |
| 13:00 | `airflow dags backfill` | OK | 360 / 360 | 100 % |
| 14:00 | scheduled | OK | 360 / 360 | 100 % |

`make smoke`: 10 of 10 checks passed. Screenshots in `docs/img/` are from this run.
ClickHouse `ProfileEvents` are counters without a `_total` suffix; the Platform dashboard uses `deriv`
on them so Prometheus raises no "might not be a counter" warnings.
