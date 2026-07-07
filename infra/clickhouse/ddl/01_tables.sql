CREATE DATABASE IF NOT EXISTS flux;

CREATE TABLE IF NOT EXISTS flux.bars_1m (
  exchange     LowCardinality(String),
  asset        LowCardinality(String),
  quote        LowCardinality(String),
  window_start DateTime64(3, 'UTC'),
  window_end   DateTime64(3, 'UTC'),
  open_price   Decimal(18, 8),
  high_price   Decimal(18, 8),
  low_price    Decimal(18, 8),
  close_price  Decimal(18, 8),
  volume       Decimal(38, 8),
  notional     Decimal(38, 16),
  trade_count  UInt64,
  vwap         Float64 ALIAS toFloat64(notional) / toFloat64(volume),
  ingested_at  DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (exchange, asset, window_start)
TTL toDateTime(window_start) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS flux.whale_trades (
  exchange     LowCardinality(String),
  asset        LowCardinality(String),
  quote        LowCardinality(String),
  trade_id     String,
  price        Decimal(18, 8),
  quantity     Decimal(18, 8),
  notional     Decimal(38, 16),
  taker_side   LowCardinality(String),
  event_ts     DateTime64(3, 'UTC'),
  ingested_at  DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_ts)
ORDER BY (asset, event_ts, exchange, trade_id);     -- identity = (exchange, trade_id); event_ts is fixed per trade

CREATE TABLE IF NOT EXISTS flux.spreads_5s (
  asset          LowCardinality(String),
  window_start   DateTime64(3, 'UTC'),
  window_end     DateTime64(3, 'UTC'),
  binance_price  Decimal(18, 8),
  coinbase_price Decimal(18, 8),
  spread_bps     Float64,
  ingested_at    DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (asset, window_start)
TTL toDateTime(window_start) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS flux.double_bottoms (
  exchange       LowCardinality(String),
  asset          LowCardinality(String),
  first_low      Decimal(18, 8),
  second_low     Decimal(18, 8),
  neckline       Decimal(18, 8),
  breakout_close Decimal(18, 8),
  pattern_start  DateTime64(3, 'UTC'),
  confirmed_at   DateTime64(3, 'UTC'),
  ingested_at    DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (exchange, asset, confirmed_at);

CREATE TABLE IF NOT EXISTS flux.ingest_errors (
  topic      LowCardinality(String),
  partition  UInt64,
  offset     UInt64,
  raw        String,
  error      String,
  seen_at    DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = MergeTree
ORDER BY (topic, seen_at)
TTL toDateTime(seen_at) + INTERVAL 14 DAY;

CREATE TABLE IF NOT EXISTS flux.recon_windows (
  interval_start      DateTime('UTC'),
  exchange            LowCardinality(String),
  asset               LowCardinality(String),
  window_start        DateTime64(3, 'UTC'),
  stream_count        UInt64,
  batch_count         UInt64,
  batch_distinct_ids  UInt64,
  stream_volume       Decimal(38, 8),
  batch_volume        Decimal(38, 8),
  stream_notional     Decimal(38, 16),
  batch_notional      Decimal(38, 16),
  status              LowCardinality(String),
  dag_run_id          String,
  checked_at          DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(checked_at)
PARTITION BY toYYYYMM(interval_start)
ORDER BY (exchange, asset, window_start);

CREATE TABLE IF NOT EXISTS flux.recon_runs (
  interval_start     DateTime('UTC'),
  interval_end       DateTime('UTC'),
  windows_total      UInt32,
  windows_match      UInt32,
  windows_late       UInt32,
  windows_missing    UInt32,
  windows_exceed     UInt32,
  windows_mismatch   UInt32,
  stream_events      UInt64,
  batch_events       UInt64,
  duplicate_events   UInt64,
  completeness_pct   Float64,
  status             LowCardinality(String),   -- OK | LATE_ONLY | FAILED | NO_DATA
  dag_run_id         String,
  checked_at         DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(checked_at)
ORDER BY interval_start;

-- Brings databases created with an earlier recon_runs schema up to date; no-op otherwise.
ALTER TABLE flux.recon_runs ADD COLUMN IF NOT EXISTS windows_mismatch UInt32 AFTER windows_exceed;
