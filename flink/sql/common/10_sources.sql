-- Timestamp fields are Avro timestamp-millis; reading them as BIGINT is valid schema resolution.
CREATE TEMPORARY TABLE binance_raw (
  event_type      STRING,
  event_time      BIGINT,
  symbol          STRING,
  trade_id        BIGINT,
  price           DECIMAL(18, 8),
  quantity        DECIMAL(18, 8),
  trade_time      BIGINT,
  is_buyer_maker  BOOLEAN,
  ingestion_time  BIGINT,
  event_ts AS TO_TIMESTAMP_LTZ(trade_time, 3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '${WATERMARK_DELAY_SECONDS}' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'raw.binance.trades',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id' = 'flink-${JOB_NAME}-binance',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'avro-confluent',
  'avro-confluent.url' = '${SCHEMA_REGISTRY_URL}'
);

CREATE TEMPORARY TABLE coinbase_raw (
  trade_id        STRING,
  product_id      STRING,
  price           DECIMAL(18, 8),
  size            DECIMAL(18, 8),
  side            STRING,
  event_time      BIGINT,
  ingestion_time  BIGINT,
  event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '${WATERMARK_DELAY_SECONDS}' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'raw.coinbase.trades',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id' = 'flink-${JOB_NAME}-coinbase',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'avro-confluent',
  'avro-confluent.url' = '${SCHEMA_REGISTRY_URL}'
);
