CREATE TEMPORARY TABLE sink_whales (
  exchange STRING, asset STRING, quote STRING, trade_id STRING,
  price STRING, quantity STRING, notional STRING, taker_side STRING,
  event_ts TIMESTAMP_LTZ(3)
) WITH (
${KAFKA_JSON_SINK},
  'topic' = 'signals.whales',
  'sink.transactional-id-prefix' = 'flux-${JOB_NAME}'
);

INSERT INTO sink_whales
SELECT exchange, asset, quote, trade_id,
       CAST(price AS STRING), CAST(quantity AS STRING), CAST(price * quantity AS STRING),
       taker_side, event_ts
FROM trades
WHERE price * quantity >= ${WHALE_MIN_NOTIONAL};
