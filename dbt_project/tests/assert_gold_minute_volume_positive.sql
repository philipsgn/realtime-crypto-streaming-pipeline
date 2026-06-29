-- Every valid Gold minute must have positive USDT notional volume.
SELECT
    window_start,
    symbol,
    quote_volume_usdt
FROM {{ ref('gold_minute_volume') }}
WHERE quote_volume_usdt <= 0
