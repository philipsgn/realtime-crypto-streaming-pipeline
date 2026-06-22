-- Stage 2 — dbt Transformation Layer
-- Reads from bronze_trades. Filters out invalid rows and price outliers.

WITH flagged_trades AS (
    SELECT
        *,
        CASE
            WHEN vwap > 0 
                 AND total_volume > 0 
                 AND trade_count > 0
                 AND price_close >= (price_open * 0.5)
                 AND price_close <= (price_open * 1.5)
            THEN true
            ELSE false
        END AS is_valid
    FROM {{ ref('bronze_trades') }}
)

SELECT *
FROM flagged_trades
WHERE is_valid = true
