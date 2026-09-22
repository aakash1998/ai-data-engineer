
-- silver_crypto (v1)
-- Staging: dedupe on natural key (coin_id, snapshot_ts), cast types,
-- standardize timestamps to UTC, add surrogate key.
-- Full refresh, idempotent: re-running on the same bronze yields identical silver.
--
-- SCHEMA DRIFT CONTRACT (pre-hook above): the CoinGecko feed once renamed
-- total_volume -> volume_24h mid-day. The loader preserves both raw columns;
-- staging coalesces them so downstream never breaks.
WITH ranked AS (
    SELECT
        coin_id,
        symbol,
        name,
        TRY_CAST(current_price AS DOUBLE)              AS current_price,
        TRY_CAST(market_cap AS DOUBLE)                 AS market_cap,
        -- drift-tolerant: prefer the canonical field, fall back to the alias
        COALESCE(TRY_CAST(total_volume AS DOUBLE),
                 TRY_CAST(volume_24h AS DOUBLE))       AS total_volume,
        TRY_CAST(price_change_percentage_24h AS DOUBLE) AS price_change_percentage_24h,
        -- CoinGecko last_updated is ISO-8601 with offset; normalize to UTC.
        CAST(last_updated AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS snapshot_ts,
        vs_currency,
        _run_id,
        CAST(_ingested_at AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY coin_id, CAST(last_updated AS TIMESTAMPTZ) AT TIME ZONE 'UTC'
            ORDER BY CAST(_ingested_at AS TIMESTAMPTZ) DESC
        ) AS rn
    FROM "drill_drift"."main"."bronze_crypto"
)
SELECT
    md5(coin_id || '|' || CAST(snapshot_ts AS VARCHAR)) AS crypto_snapshot_sk,
    coin_id,
    symbol,
    name,
    current_price,
    market_cap,
    total_volume,
    price_change_percentage_24h,
    snapshot_ts,
    vs_currency,
    _run_id,
    ingested_at
FROM ranked
WHERE rn = 1