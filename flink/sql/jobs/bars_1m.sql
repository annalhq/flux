-- Decimals are written as strings so ClickHouse parses them exactly.
CREATE TEMPORARY TABLE sink_bars_1m (
  exchange STRING, asset STRING, quote STRING,
  window_start TIMESTAMP(3), window_end TIMESTAMP(3),
  open_price STRING, high_price STRING, low_price STRING, close_price STRING,
  volume STRING, notional STRING, trade_count BIGINT
) WITH (
${KAFKA_JSON_SINK},
  'topic' = 'agg.bars-1m',
  'sink.transactional-id-prefix' = 'flux-${JOB_NAME}'
);

INSERT INTO sink_bars_1m
SELECT exchange, asset, quote, window_start, window_end,
       CAST(open_price AS STRING), CAST(high_price AS STRING),
       CAST(low_price AS STRING), CAST(close_price AS STRING),
       CAST(volume AS STRING), CAST(notional AS STRING), trade_count
FROM bars_1m;
