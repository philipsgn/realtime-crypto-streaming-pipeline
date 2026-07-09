-- Gold serving view for real-time, cross-symbol volume comparison in Grafana.
{{ config(materialized='view') }}

SELECT
    window_start,
    MAX(window_end) AS window_end,
    symbol,
    SUM(total_volume) AS base_volume,
    SUM(vwap * total_volume) AS quote_volume_usdt,
    SUM(vwap * total_volume) / NULLIF(SUM(total_volume), 0) AS minute_vwap,
    SUM(trade_count) AS trade_count,
    MAX(price_open) AS price_open,
    MAX(price_close) AS price_close,
    SUM(buy_volume) AS buy_volume,
    (MAX(price_close) - MAX(price_open))
        / NULLIF(MAX(price_open), 0) * 100 AS price_change_pct
FROM {{ ref('silver_trades') }}
GROUP BY
    window_start,
    symbol
