CREATE TEMPORARY TABLE sink_double_bottom (
  exchange STRING, asset STRING,
  first_low STRING, second_low STRING, neckline STRING, breakout_close STRING,
  pattern_start TIMESTAMP(3), confirmed_at TIMESTAMP(3)
) WITH (
${KAFKA_JSON_SINK},
  'topic' = 'signals.double-bottom',
  'sink.transactional-id-prefix' = 'flux-${JOB_NAME}'
);

-- Double bottom on 1-minute bars: a decline (D1), a rally to the neckline (P), a second decline
-- (D2) ending within DB_DIP_TOLERANCE of the first low, then a close above the neckline (B).
-- S is undefined, so it matches any bar and anchors PREV() for the first D1 bar.
INSERT INTO sink_double_bottom
SELECT exchange, asset,
       CAST(first_low AS STRING), CAST(second_low AS STRING),
       CAST(neckline AS STRING), CAST(breakout_close AS STRING),
       pattern_start, confirmed_at
FROM bars_1m
MATCH_RECOGNIZE (
  PARTITION BY exchange, asset
  ORDER BY window_time
  MEASURES
    MIN(D1.low_price)       AS first_low,
    MIN(D2.low_price)       AS second_low,
    MAX(P.high_price)       AS neckline,
    LAST(B.close_price)     AS breakout_close,
    FIRST(D1.window_start)  AS pattern_start,
    LAST(B.window_end)      AS confirmed_at
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (S D1{2,} P{2,} D2{2,} B) WITHIN INTERVAL '3' HOUR
  DEFINE
    D1 AS close_price < PREV(close_price),
    P  AS close_price > PREV(close_price),
    D2 AS close_price < PREV(close_price),
    B  AS close_price > MAX(P.high_price)
          AND ABS(CAST(MIN(D2.low_price) AS DOUBLE) - CAST(MIN(D1.low_price) AS DOUBLE))
              / CAST(MIN(D1.low_price) AS DOUBLE) <= ${DB_DIP_TOLERANCE}
          AND (CAST(MAX(P.high_price) AS DOUBLE) - CAST(MIN(D1.low_price) AS DOUBLE))
              / CAST(MIN(D1.low_price) AS DOUBLE) >= ${DB_MIN_DEPTH}
);
