{{
    config(
        materialized = 'table',
        schema       = 'gold',
        tags         = ['gold', 'dimension', 'weather']
    )
}}

WITH distinct_weather AS (

    SELECT
        weather_code,
        MAX(weather_description) AS weather_description
    FROM {{ ref('silver_weather') }}
    WHERE weather_code IS NOT NULL
    GROUP BY weather_code

),

dim_weather AS (

    SELECT

        -- Surrogate Key
        REGEXP_REPLACE(
            {{ dbt_utils.generate_surrogate_key(['weather_code']) }},
            '^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$',
            '$1-$2-$3-$4-$5'
        )                       AS weather_key,

        -- Natural Key
        weather_code,

        -- Attributes
        weather_description,

        -- Audit
        CURRENT_TIMESTAMP()     AS gold_loaded_at

    FROM distinct_weather

)

SELECT * FROM dim_weather
