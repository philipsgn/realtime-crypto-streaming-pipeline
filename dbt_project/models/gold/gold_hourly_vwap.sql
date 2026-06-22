-- Stage 2 — dbt Transformation Layer
-- Reads from silver_trades. Computes hourly volume-weighted average price (VWAP).

SELECT
    symbol,
    date_trunc('hour', window_start) AS hour_start,
    SUM(vwap * total_volume) / SUM(total_volume) AS hourly_vwap,
    SUM(total_volume) AS hourly_total_volume,
    SUM(trade_count) AS hourly_trade_count
FROM {{ ref('silver_trades') }}
GROUP BY
    symbol,
    date_trunc('hour', window_start)
