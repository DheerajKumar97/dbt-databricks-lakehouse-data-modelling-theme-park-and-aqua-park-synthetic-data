{{
    config(
        materialized     = 'incremental',
        unique_key       = 'weather_sk',
        schema           = 'silver',
        tags             = ['silver', 'weather', 'daily'],
        on_schema_change = 'sync_all_columns'
    )
}}

WITH bronze_raw_weather AS (

    SELECT *
    FROM {{ ref('raw_weather') }}

    {% if is_incremental() %}
        WHERE TRY_CAST(ingestion_timestamp AS TIMESTAMP) > (
            SELECT MAX(silver_loaded_at) FROM {{ this }}
        )
    {% endif %}

),

deduped_raw_weather AS (

    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY park_code, poll_datetime
                ORDER BY TRY_CAST(ingestion_timestamp AS TIMESTAMP) DESC
            ) AS rn
        FROM bronze_raw_weather
    )
    WHERE rn = 1

),

cast_and_clean AS (

    SELECT

        -- Location
        park_code,

        -- Time
        TRY_CAST(poll_datetime AS TIMESTAMP)                  AS weather_timestamp,
        CAST(TRY_CAST(poll_datetime AS TIMESTAMP) AS DATE)    AS weather_date,

        -- Temperature
        TRY_CAST(temperature_c AS DOUBLE)                     AS temperature_c,
        TRY_CAST(feels_like_c AS DOUBLE)                      AS feels_like_c,
        TRY_CAST(temperature_min_c AS DOUBLE)                 AS temperature_min_c,
        TRY_CAST(temperature_max_c AS DOUBLE)                 AS temperature_max_c,

        -- Atmosphere
        TRY_CAST(humidity_pct AS INT)                         AS humidity_pct,
        TRY_CAST(wind_speed_ms AS DOUBLE)                     AS wind_speed_ms,
        TRY_CAST(wind_direction_deg AS INT)                   AS wind_direction_deg,
        TRY_CAST(uv_index AS INT)                             AS uv_index,
        TRY_CAST(visibility_m AS INT)                         AS visibility_m,
        TRY_CAST(rainfall_1h_mm AS DOUBLE)                    AS rainfall_1h_mm,

        -- Weather Condition
        UPPER(TRIM(weather_code))                             AS weather_code,
        INITCAP(TRIM(weather_description))                    AS weather_description,

        -- Air Quality
        TRY_CAST(air_quality_index AS INT)                    AS air_quality_index,

        -- API Status
        TRY_CAST(http_status_code AS INT)                     AS http_status_code,
        api_error_message,

        -- Metadata
        TRY_CAST(ingestion_timestamp AS TIMESTAMP)            AS ingestion_timestamp,
        TRY_CAST(ingestion_date AS DATE)                      AS ingestion_date,
        batch_id,
        pipeline_name,
        UPPER(TRIM(validation_status))                        AS validation_status,

        CASE
            WHEN LOWER(TRIM(is_duplicate)) IN ('true','yes','1') THEN TRUE
            WHEN LOWER(TRIM(is_duplicate)) IN ('false','no','0') THEN FALSE
            ELSE NULL
        END                                                  AS is_duplicate,

        source_region

    FROM deduped_raw_weather

),

silver_weather AS (

    SELECT

        {# -- Surrogate Key
        REGEXP_REPLACE(
            {{ dbt_utils.generate_surrogate_key(
                ['park_code','weather_timestamp']
            ) }},
            '^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$',
            '$1-$2-$3-$4-$5'
        ) AS weather_sk, #}

        -- Dimensions
        park_code,
        weather_timestamp,
        weather_date,

        -- Measures
        temperature_c,
        feels_like_c,
        temperature_min_c,
        temperature_max_c,
        humidity_pct,
        wind_speed_ms,
        wind_direction_deg,
        uv_index,
        visibility_m,
        rainfall_1h_mm,
        air_quality_index,

        -- Weather Info
        weather_code,
        weather_description,

        -- Derived Metrics (🔥 strong point)
        (temperature_max_c - temperature_min_c)              AS temperature_range,

        CASE
            WHEN temperature_c >= 40 THEN 'EXTREME_HEAT'
            WHEN temperature_c >= 30 THEN 'HOT'
            WHEN temperature_c >= 20 THEN 'WARM'
            ELSE 'COOL'
        END                                                  AS temperature_category,

        CASE
            WHEN rainfall_1h_mm > 0 THEN TRUE
            ELSE FALSE
        END                                                  AS is_raining,

        CASE
            WHEN air_quality_index <= 2 THEN 'GOOD'
            WHEN air_quality_index <= 4 THEN 'MODERATE'
            ELSE 'POOR'
        END                                                  AS air_quality_status,

        -- API Info
        http_status_code,
        api_error_message,

        -- Metadata
        ingestion_timestamp,
        ingestion_date,
        batch_id,
        pipeline_name,
        validation_status,
        is_duplicate,
        source_region,

        -- Audit
        CURRENT_TIMESTAMP()                                 AS silver_loaded_at

    FROM cast_and_clean

)

SELECT *
FROM silver_weather
