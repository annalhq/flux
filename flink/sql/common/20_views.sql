-- Canonical trade model shared by every job. String literals are CAST because UNION ALL of
-- CHAR(n) literals pads the shorter one with spaces.
CREATE TEMPORARY VIEW trades AS
SELECT
  CAST('binance' AS STRING)                                           AS exchange,
  REGEXP_REPLACE(symbol, 'USDT$', '')                                 AS asset,
  CAST('USDT' AS STRING)                                              AS quote,
  symbol                                                              AS pair,
  CAST(trade_id AS STRING)                                            AS trade_id,
  price,
  quantity,
  CAST(CASE WHEN is_buyer_maker THEN 'SELL' ELSE 'BUY' END AS STRING) AS taker_side,
  event_ts,
  trade_time                                                          AS event_ms,
  ingestion_time                                                      AS ingest_ms
FROM binance_raw
UNION ALL
SELECT
  CAST('coinbase' AS STRING),
  SPLIT_INDEX(product_id, '-', 0),
  SPLIT_INDEX(product_id, '-', 1),
  product_id,
  trade_id,
  price,
  size,
  -- Coinbase reports the maker side; the taker is on the opposite side.
  CAST(CASE WHEN UPPER(side) = 'BUY' THEN 'SELL' ELSE 'BUY' END AS STRING),
  event_ts,
  event_time,
  ingestion_time
FROM coinbase_raw;

-- 1-minute OHLCV bars. Additive measures (volume, notional, trade_count) are exact and roll up;
-- VWAP is derived downstream. FIRST_VALUE/LAST_VALUE follow arrival order within the window.
CREATE TEMPORARY VIEW bars_1m AS
SELECT
  exchange, asset, quote,
  window_start, window_end, window_time,
  FIRST_VALUE(price)     AS open_price,
  MAX(price)             AS high_price,
  MIN(price)             AS low_price,
  LAST_VALUE(price)      AS close_price,
  SUM(quantity)          AS volume,
  SUM(price * quantity)  AS notional,
  COUNT(*)               AS trade_count
FROM TABLE(TUMBLE(TABLE trades, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
GROUP BY exchange, asset, quote, window_start, window_end, window_time;
