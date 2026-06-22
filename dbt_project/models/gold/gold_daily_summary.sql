-- Stage 2 — dbt Transformation Layer
-- Reads from silver_trades. Computes daily summary metrics including high, low, and VWAP.

SELECT
    symbol,
    date_trunc('day', window_start) AS day_start,
    SUM(vwap * total_volume) / SUM(total_volume) AS daily_vwap,
    SUM(total_volume) AS daily_volume,
    MAX(price_close) AS daily_high,
    MIN(price_close) AS daily_low,
    (last(price_close, window_start) - first(price_open, window_start)) / first(price_open, window_start) * 100 AS daily_price_change_pct
FROM {{ ref('silver_trades') }}
GROUP BY
    symbol,
    date_trunc('day', window_start)
