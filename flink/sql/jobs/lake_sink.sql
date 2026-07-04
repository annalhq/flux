-- Every canonical trade, unfiltered and including late events, so the lake is the complete
-- record that reconciliation compares the streaming aggregates against. Iceberg commits on
-- each checkpoint, which makes the sink exactly-once.
INSERT INTO lake.market.trades
SELECT exchange, asset, quote, pair, trade_id, price, quantity, taker_side,
       CAST(event_ts AS TIMESTAMP_LTZ(6)), event_ms, ingest_ms,
       CAST(event_ts AS DATE)
FROM trades;
