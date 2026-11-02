{{
    config(
        materialized = 'table',
        schema       = 'gold',
        tags         = ['gold', 'fact', 'weather']
    )
}}

{#
    fact_weather_observation — Weather telemetry fact table

    Grain: one row per weather poll record (hourly).

    FK Join Strategy (100% RI):
        INNER JOIN → date_key, park_key, weather_key  (all NOT NULL — orphan guard)
#}

WITH silver_wx AS (

    SELECT *
    FROM {{ ref('silver_weather') }}
    WHERE COALESCE(is_deleted, FALSE) = FALSE
      AND weather_sk IS NOT NULL
      AND weather_date IS NOT NULL
      AND weather_code IS NOT NULL
      AND weather_code <> ''

),

fact_weather_observation AS (

    SELECT

        -- Grain = park + poll timestamp, so reuse the silver key
        w.weather_sk                    AS weather_obs_key,

        d.date_key,
        p.park_key,
        wd.weather_key,

        w.weather_timestamp             AS poll_datetime,

        w.temperature_c,
        w.feels_like_c,
        w.temperature_min_c,
        w.temperature_max_c,
        w.temperature_range,
        w.humidity_pct,
        w.wind_speed_ms,
        w.wind_direction_deg,
        w.uv_index,
        w.visibility_m,
        w.rainfall_1h_mm,
        w.air_quality_index,
        w.temperature_category,
        w.is_raining,
        w.air_quality_status,

        CURRENT_TIMESTAMP()             AS gold_loaded_at

    FROM silver_wx w
    INNER JOIN {{ ref('dim_date') }}    d  ON d.full_date = w.weather_date
    INNER JOIN {{ ref('dim_park') }}    p  ON p.park_code = w.park_code
    INNER JOIN {{ ref('dim_weather') }} wd ON wd.weather_code = w.weather_code

)

SELECT * FROM fact_weather_observation