-- Stage 2 — dbt Transformation Layer
-- Reads raw metrics from TimescaleDB (trade_metrics_1min)

SELECT
    window_start,
    window_end,
    symbol,
    vwap,
    total_volume,
    trade_count,
    price_open,
    price_close,
    buy_volume,
    price_change_pct,
    window_minutes
FROM {{ source('crypto_pipeline', 'trade_metrics_1min') }}
