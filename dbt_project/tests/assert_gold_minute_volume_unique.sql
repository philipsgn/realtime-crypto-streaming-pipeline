-- The Grafana serving grain is exactly one row per minute and symbol.
SELECT
    window_start,
    symbol,
    COUNT(*) AS row_count
FROM {{ ref('gold_minute_volume') }}
GROUP BY
    window_start,
    symbol
HAVING COUNT(*) > 1
