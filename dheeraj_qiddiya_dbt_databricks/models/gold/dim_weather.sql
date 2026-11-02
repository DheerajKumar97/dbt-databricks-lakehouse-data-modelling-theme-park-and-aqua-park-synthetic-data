{{
    config(
        materialized = 'table',
        schema       = 'gold',
        tags         = ['gold', 'dimension', 'weather']
    )
}}

{#
    dim_weather — Weather condition dimension

    Sourced from silver_weather (deduped on weather_code).
    weather_key = new UUID surrogate key (hashed on weather_code).
    This is a junk/reference dimension — one row per distinct weather condition.
#}

WITH distinct_weather AS (
    SELECT
        weather_code,
        MAX(weather_description) AS weather_description
    FROM {{ ref('silver_weather') }}
    WHERE COALESCE(is_deleted, FALSE) = FALSE
      AND weather_code IS NOT NULL
      AND weather_code <> ''
    GROUP BY weather_code
),

dim_weather AS (
    SELECT
        {{ uuid_surrogate_key(['weather_code']) }}  AS weather_key,
        weather_code,
        weather_description,
        CURRENT_TIMESTAMP()                         AS gold_loaded_at
    FROM distinct_weather
)

SELECT * FROM dim_weather
