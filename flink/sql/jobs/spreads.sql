-- Last price per exchange in 5-second windows, joined across exchanges on the same window.
-- Joining on both window bounds lets the planner use a window join, which keeps no state
-- beyond the open windows.
CREATE TEMPORARY VIEW px_5s AS
SELECT exchange, asset, window_start, window_end,
       LAST_VALUE(price) AS last_price, COUNT(*) AS n
FROM TABLE(TUMBLE(TABLE trades, DESCRIPTOR(event_ts), INTERVAL '5' SECOND))
GROUP BY exchange, asset, window_start, window_end;

CREATE TEMPORARY TABLE sink_spreads (
  asset STRING, window_start TIMESTAMP(3), window_end TIMESTAMP(3),
  binance_price STRING, coinbase_price STRING, spread_bps DOUBLE
) WITH (
${KAFKA_JSON_SINK},
  'topic' = 'agg.spreads-5s',
  'sink.transactional-id-prefix' = 'flux-${JOB_NAME}'
);

-- spread_bps > 0: Coinbase (USD) priced above Binance (USDT); includes the USDT/USD basis.
INSERT INTO sink_spreads
SELECT b.asset, b.window_start, b.window_end,
       CAST(b.last_price AS STRING), CAST(c.last_price AS STRING),
       (CAST(c.last_price AS DOUBLE) - CAST(b.last_price AS DOUBLE)) / CAST(b.last_price AS DOUBLE) * 10000
FROM (SELECT * FROM px_5s WHERE exchange = 'binance') b
JOIN (SELECT * FROM px_5s WHERE exchange = 'coinbase') c
  ON  b.asset = c.asset
  AND b.window_start = c.window_start
  AND b.window_end   = c.window_end;
