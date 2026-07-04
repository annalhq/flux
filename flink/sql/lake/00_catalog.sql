-- Iceberg catalog served by Nessie's Iceberg REST endpoint. Flink writes data files to MinIO
-- with S3FileIO; Nessie writes table metadata and versions it in Postgres.
CREATE CATALOG lake WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = '${ICEBERG_REST_URI}',
  'warehouse' = '${ICEBERG_WAREHOUSE}',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id' = '${MINIO_ROOT_USER}',
  's3.secret-access-key' = '${MINIO_ROOT_PASSWORD}',
  'client.region' = 'us-east-1'
);

CREATE DATABASE IF NOT EXISTS lake.market;

CREATE TABLE IF NOT EXISTS lake.market.trades (
  exchange    STRING,
  asset       STRING,
  quote       STRING,
  pair        STRING,
  trade_id    STRING,
  price       DECIMAL(18, 8),
  quantity    DECIMAL(18, 8),
  taker_side  STRING,
  event_ts    TIMESTAMP_LTZ(6),
  event_ms    BIGINT,
  ingest_ms   BIGINT,
  event_date  DATE
) PARTITIONED BY (event_date, exchange)
WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'zstd',
  'write.target-file-size-bytes' = '134217728',
  'write.distribution-mode' = 'hash'
);
