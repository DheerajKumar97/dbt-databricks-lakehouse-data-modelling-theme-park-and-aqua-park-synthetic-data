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

    -- Normalise the natural-key columns ONCE, before dedup and hashing
    SELECT
        *,
        UPPER(TRIM(park_code))                       AS park_code_clean,
        TRY_CAST(poll_datetime AS TIMESTAMP)         AS weather_timestamp_clean,
        TRY_CAST(ingestion_timestamp AS TIMESTAMP)   AS ingestion_ts_clean
    FROM {{ ref('raw_weather') }}

    {% if is_incremental() %}
        WHERE bronze_loaded_at > (
            SELECT COALESCE(MAX(silver_loaded_at), TIMESTAMP('1900-01-01'))
            FROM {{ this }}
        )
    {% endif %}

),

valid_keys AS (

    -- Orphan guard: never hash NULL/blank natural keys
    SELECT *
    FROM bronze_raw_weather
    WHERE park_code_clean IS NOT NULL
      AND park_code_clean <> ''
      AND weather_timestamp_clean IS NOT NULL

),

deduped_raw_weather AS (

    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY park_code_clean, weather_timestamp_clean
                ORDER BY ingestion_ts_clean DESC, batch_id DESC   -- deterministic tie-break
            ) AS rn
        FROM valid_keys
    )
    WHERE rn = 1

),

cast_and_clean AS (

    SELECT

        -- Surrogate Key: built from the CLEANED natural key
        {{ uuid_surrogate_key(['park_code_clean', 'weather_timestamp_clean']) }}
                                                              AS weather_sk,

        park_code_clean                                       AS park_code,
        weather_timestamp_clean                               AS weather_timestamp,
        CAST(weather_timestamp_clean AS DATE)                 AS weather_date,

        TRY_CAST(temperature_c AS DOUBLE)                     AS temperature_c,
        TRY_CAST(feels_like_c AS DOUBLE)                      AS feels_like_c,
        TRY_CAST(temperature_min_c AS DOUBLE)                 AS temperature_min_c,
        TRY_CAST(temperature_max_c AS DOUBLE)                 AS temperature_max_c,
        TRY_CAST(humidity_pct AS INT)                         AS humidity_pct,
        TRY_CAST(wind_speed_ms AS DOUBLE)                     AS wind_speed_ms,
        TRY_CAST(wind_direction_deg AS INT)                   AS wind_direction_deg,
        TRY_CAST(uv_index AS INT)                             AS uv_index,
        TRY_CAST(visibility_m AS INT)                         AS visibility_m,
        TRY_CAST(rainfall_1h_mm AS DOUBLE)                    AS rainfall_1h_mm,
        UPPER(TRIM(weather_code))                             AS weather_code,
        INITCAP(TRIM(weather_description))                    AS weather_description,
        TRY_CAST(air_quality_index AS INT)                    AS air_quality_index,
        TRY_CAST(http_status_code AS INT)                     AS http_status_code,
        api_error_message,
        ingestion_ts_clean                                    AS ingestion_timestamp,
        TRY_CAST(ingestion_date AS DATE)                      AS ingestion_date,
        batch_id,
        pipeline_name,
        UPPER(TRIM(validation_status))                        AS validation_status,
        CASE
            WHEN LOWER(TRIM(CAST(is_duplicate AS STRING))) IN ('true','yes','y','1')  THEN TRUE
            WHEN LOWER(TRIM(CAST(is_duplicate AS STRING))) IN ('false','no','n','0')  THEN FALSE
            ELSE NULL
        END                                                   AS is_duplicate,
        source_region,
        COALESCE(is_deleted, FALSE)                           AS is_deleted,
        deleted_at

    FROM deduped_raw_weather

),

silver_weather AS (

    SELECT

        -- Surrogate Key
        weather_sk,

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

        -- Derived Metrics
        (temperature_max_c - temperature_min_c)               AS temperature_range,

        CASE
            WHEN temperature_c >= 40 THEN 'EXTREME_HEAT'
            WHEN temperature_c >= 30 THEN 'HOT'
            WHEN temperature_c >= 20 THEN 'WARM'
            ELSE 'COOL'
        END                                                   AS temperature_category,

        CASE
            WHEN rainfall_1h_mm > 0 THEN TRUE
            ELSE FALSE
        END                                                   AS is_raining,

        CASE
            WHEN air_quality_index <= 2 THEN 'GOOD'
            WHEN air_quality_index <= 4 THEN 'MODERATE'
            ELSE 'POOR'
        END                                                   AS air_quality_status,

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

        -- Soft delete tracking
        is_deleted,
        deleted_at,

        -- Audit
        CURRENT_TIMESTAMP()                                   AS silver_loaded_at

    FROM cast_and_clean

)

{% if is_incremental() %}
, hard_deleted_in_bronze AS (

    SELECT
        t.* EXCEPT (is_deleted, deleted_at, silver_loaded_at),
        TRUE                AS is_deleted,
        CURRENT_TIMESTAMP() AS deleted_at,
        CURRENT_TIMESTAMP() AS silver_loaded_at
    FROM {{ this }} t
    LEFT ANTI JOIN (
        SELECT
            UPPER(TRIM(park_code))               AS park_code,
            TRY_CAST(poll_datetime AS TIMESTAMP) AS weather_timestamp
        FROM {{ ref('raw_weather') }}
    ) b
        ON t.park_code = b.park_code
       AND t.weather_timestamp = b.weather_timestamp
    WHERE COALESCE(t.is_deleted, FALSE) = FALSE

)
{% endif %}

SELECT * FROM silver_weather
{% if is_incremental() %}
UNION ALL
SELECT * FROM hard_deleted_in_bronze
{% endif %}
