
-- gold_crypto_daily (v1)
-- Mart: one row per coin per day. OHLC approximated from intra-day snapshots
-- (ingest runs every ~6h, so 2-4 snapshots/day): open = first snapshot price,
-- close = last snapshot price, high/low = min/max.
-- Grain key: (coin_id, market_date). Idempotent full refresh.
WITH ordered AS (
    SELECT
        coin_id,
        symbol,
        name,
        CAST(snapshot_ts AS DATE) AS market_date,
        snapshot_ts,
        current_price,
        market_cap,
        total_volume,
        price_change_percentage_24h,
        ROW_NUMBER() OVER (PARTITION BY coin_id, CAST(snapshot_ts AS DATE)
                           ORDER BY snapshot_ts ASC)  AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY coin_id, CAST(snapshot_ts AS DATE)
                           ORDER BY snapshot_ts DESC) AS rn_desc
    FROM "drill_drift"."main"."silver_crypto"
),
agg AS (
    SELECT
        coin_id,
        CAST(snapshot_ts AS DATE) AS market_date,
        MAX(symbol) AS symbol,
        MAX(name)   AS coin_name,
        COUNT(*)    AS snapshot_count,
        MIN(current_price) AS low_price,
        MAX(current_price) AS high_price,
        AVG(current_price) AS avg_price,
        AVG(total_volume)  AS avg_volume,
        AVG(market_cap)    AS avg_market_cap,
        MAX(ingested_at)   AS last_ingested_at
    FROM "drill_drift"."main"."silver_crypto"
    GROUP BY coin_id, CAST(snapshot_ts AS DATE)
)
SELECT
    md5(a.coin_id || '|' || CAST(a.market_date AS VARCHAR)) AS crypto_daily_sk,
    a.coin_id,
    a.symbol,
    a.coin_name,
    a.market_date,
    o_open.current_price  AS open_price,
    o_close.current_price AS close_price,
    a.low_price,
    a.high_price,
    a.avg_price,
    a.avg_volume,
    a.avg_market_cap,
    o_close.price_change_percentage_24h AS change_24h_pct,
    a.snapshot_count,
    a.last_ingested_at
FROM agg a
LEFT JOIN ordered o_open
  ON o_open.coin_id = a.coin_id
 AND CAST(o_open.snapshot_ts AS DATE) = a.market_date
 AND o_open.rn_asc = 1
LEFT JOIN ordered o_close
  ON o_close.coin_id = a.coin_id
 AND CAST(o_close.snapshot_ts AS DATE) = a.market_date
 AND o_close.rn_desc = 1