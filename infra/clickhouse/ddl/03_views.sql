CREATE OR REPLACE VIEW flux.vwap_5m AS
SELECT exchange, asset,
       toStartOfInterval(window_start, INTERVAL 5 MINUTE) AS bucket,
       toFloat64(sum(notional)) / toFloat64(sum(volume))  AS vwap,
       sum(volume)       AS total_volume,
       sum(trade_count)  AS trades
FROM flux.bars_1m FINAL
GROUP BY exchange, asset, bucket;

CREATE OR REPLACE VIEW flux.arbitrage_signals AS
SELECT asset, window_start, binance_price, coinbase_price, spread_bps,
       if(spread_bps > 0, 'BUY_BINANCE_SELL_COINBASE', 'BUY_COINBASE_SELL_BINANCE') AS direction
FROM flux.spreads_5s FINAL
WHERE abs(spread_bps) >= 10;      -- keep in sync with ARB_SIGNAL_BPS
