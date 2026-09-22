{{ config(materialized='table', tags=['gold']) }}
-- gold_weather_hourly (v1)
-- Mart: one row per city per hour (latest observation wins on re-delivery).
-- Enriched with the city's daily min/max for context.
-- Grain key: (city, hour_ts). Idempotent full refresh.
WITH daily AS (
    SELECT
        city,
        CAST(hour_ts AS DATE) AS weather_date,
        MIN(temperature_2m) AS day_min_temp_c,
        MAX(temperature_2m) AS day_max_temp_c
    FROM {{ ref('silver_weather') }}
    GROUP BY city, CAST(hour_ts AS DATE)
)
SELECT
    s.weather_hour_sk,
    s.city,
    s.hour_ts,
    s.temperature_2m AS temp_c,
    d.day_min_temp_c,
    d.day_max_temp_c,
    s.latitude,
    s.longitude,
    s._run_id AS source_run_id,
    s.ingested_at
FROM {{ ref('silver_weather') }} s
JOIN daily d
  ON d.city = s.city
 AND d.weather_date = CAST(s.hour_ts AS DATE)
