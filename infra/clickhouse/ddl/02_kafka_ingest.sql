-- Kafka engine consumers for the Flink output topics. Flink writes transactionally and
-- librdkafka reads read_committed, so only committed results are ingested. Rows that fail
-- to parse are routed to flux.ingest_errors instead of stalling the consumer.

-- agg.bars-1m -> bars_1m
CREATE TABLE IF NOT EXISTS flux.kafka_bars_1m (
  exchange String, asset String, quote String,
  window_start String, window_end String,
  open_price String, high_price String, low_price String, close_price String,
  volume String, notional String, trade_count UInt64
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
         kafka_topic_list = 'agg.bars-1m',
         kafka_group_name = 'clickhouse-bars-1m',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS flux.mv_bars_1m TO flux.bars_1m AS
SELECT
  exchange, asset, quote,
  parseDateTime64BestEffort(window_start, 3, 'UTC') AS window_start,
  parseDateTime64BestEffort(window_end,   3, 'UTC') AS window_end,
  toDecimal64(open_price, 8)   AS open_price,
  toDecimal64(high_price, 8)   AS high_price,
  toDecimal64(low_price, 8)    AS low_price,
  toDecimal64(close_price, 8)  AS close_price,
  toDecimal128(volume, 8)      AS volume,
  toDecimal128(notional, 16)   AS notional,
  trade_count
FROM flux.kafka_bars_1m
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS flux.mv_bars_1m_errors TO flux.ingest_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
       _raw_message AS raw, _error AS error
FROM flux.kafka_bars_1m
WHERE length(_error) > 0;

-- signals.whales -> whale_trades
CREATE TABLE IF NOT EXISTS flux.kafka_whales (
  exchange String, asset String, quote String, trade_id String,
  price String, quantity String, notional String, taker_side String,
  event_ts String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
         kafka_topic_list = 'signals.whales',
         kafka_group_name = 'clickhouse-whales',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS flux.mv_whales TO flux.whale_trades AS
SELECT
  exchange, asset, quote, trade_id,
  toDecimal64(price, 8)        AS price,
  toDecimal64(quantity, 8)     AS quantity,
  toDecimal128(notional, 16)   AS notional,
  taker_side,
  parseDateTime64BestEffort(event_ts, 3, 'UTC') AS event_ts
FROM flux.kafka_whales
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS flux.mv_whales_errors TO flux.ingest_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
       _raw_message AS raw, _error AS error
FROM flux.kafka_whales
WHERE length(_error) > 0;

-- agg.spreads-5s -> spreads_5s
CREATE TABLE IF NOT EXISTS flux.kafka_spreads_5s (
  asset String, window_start String, window_end String,
  binance_price String, coinbase_price String, spread_bps Float64
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
         kafka_topic_list = 'agg.spreads-5s',
         kafka_group_name = 'clickhouse-spreads-5s',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS flux.mv_spreads_5s TO flux.spreads_5s AS
SELECT
  asset,
  parseDateTime64BestEffort(window_start, 3, 'UTC') AS window_start,
  parseDateTime64BestEffort(window_end,   3, 'UTC') AS window_end,
  toDecimal64(binance_price, 8)  AS binance_price,
  toDecimal64(coinbase_price, 8) AS coinbase_price,
  spread_bps
FROM flux.kafka_spreads_5s
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS flux.mv_spreads_5s_errors TO flux.ingest_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
       _raw_message AS raw, _error AS error
FROM flux.kafka_spreads_5s
WHERE length(_error) > 0;

-- signals.double-bottom -> double_bottoms
CREATE TABLE IF NOT EXISTS flux.kafka_double_bottoms (
  exchange String, asset String,
  first_low String, second_low String, neckline String, breakout_close String,
  pattern_start String, confirmed_at String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
         kafka_topic_list = 'signals.double-bottom',
         kafka_group_name = 'clickhouse-double-bottom',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS flux.mv_double_bottoms TO flux.double_bottoms AS
SELECT
  exchange, asset,
  toDecimal64(first_low, 8)      AS first_low,
  toDecimal64(second_low, 8)     AS second_low,
  toDecimal64(neckline, 8)       AS neckline,
  toDecimal64(breakout_close, 8) AS breakout_close,
  parseDateTime64BestEffort(pattern_start, 3, 'UTC') AS pattern_start,
  parseDateTime64BestEffort(confirmed_at,  3, 'UTC') AS confirmed_at
FROM flux.kafka_double_bottoms
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS flux.mv_double_bottoms_errors TO flux.ingest_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
       _raw_message AS raw, _error AS error
FROM flux.kafka_double_bottoms
WHERE length(_error) > 0;
