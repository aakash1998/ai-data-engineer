
    

    create  table
      "drill_drift"."main"."silver_weather__dbt_tmp"
  
    
    as (
      
-- silver_weather (v1)
-- Staging: dedupe on natural key (city, hour_ts), cast types,
-- add surrogate key. Open-Meteo already returns UTC hourly grain.
-- Full refresh, idempotent.
WITH ranked AS (
    SELECT
        city,
        TRY_CAST(latitude AS DOUBLE)  AS latitude,
        TRY_CAST(longitude AS DOUBLE) AS longitude,
        CAST(hour_ts AS TIMESTAMP)    AS hour_ts,   -- already UTC ISO
        TRY_CAST(temperature_2m AS DOUBLE) AS temperature_2m,
        _run_id,
        CAST(_ingested_at AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY city, CAST(hour_ts AS TIMESTAMP)
            ORDER BY CAST(_ingested_at AS TIMESTAMPTZ) DESC
        ) AS rn
    FROM "drill_drift"."main"."bronze_weather"
)
SELECT
    md5(city || '|' || CAST(hour_ts AS VARCHAR)) AS weather_hour_sk,
    city,
    latitude,
    longitude,
    hour_ts,
    temperature_2m,
    _run_id,
    ingested_at
FROM ranked
WHERE rn = 1
    );
    
  