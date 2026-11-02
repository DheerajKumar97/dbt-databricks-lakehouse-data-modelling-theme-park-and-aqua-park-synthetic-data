{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'merge',
        unique_key           = ['park_code', 'poll_datetime'],
        on_schema_change     = 'sync_all_columns',
        schema               = 'bronze',
        tags                 = ['bronze', 'weather'],
        tblproperties        = {'delta.enableChangeDataFeed': 'true'}
    )
}}

WITH source_weather AS (

    SELECT
        *,
        'SF' AS source_region
    FROM {{ source('landing_zone', 'sf_raw_weather') }}
    {% if is_incremental() %}
    WHERE TRY_CAST(ingestion_timestamp AS TIMESTAMP) >= (
        SELECT COALESCE(MAX(TRY_CAST(ingestion_timestamp AS TIMESTAMP)), TIMESTAMP('1900-01-01'))
               - INTERVAL {{ var('bronze_overlap_days', 3) }} DAYS
        FROM {{ this }}
        WHERE source_region = 'SF'
    )
    {% endif %}

    UNION ALL

    SELECT
        *,
        'AQ' AS source_region
    FROM {{ source('landing_zone', 'aq_raw_weather') }}
    {% if is_incremental() %}
    WHERE TRY_CAST(ingestion_timestamp AS TIMESTAMP) >= (
        SELECT COALESCE(MAX(TRY_CAST(ingestion_timestamp AS TIMESTAMP)), TIMESTAMP('1900-01-01'))
               - INTERVAL {{ var('bronze_overlap_days', 3) }} DAYS
        FROM {{ this }}
        WHERE source_region = 'AQ'
    )
    {% endif %}

),

deduplicated_weather AS (

    SELECT
        * EXCEPT (rn),
        FALSE                   AS is_deleted,
        CAST(NULL AS TIMESTAMP) AS deleted_at,
        CURRENT_TIMESTAMP()     AS bronze_loaded_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY park_code, poll_datetime
                ORDER BY
                    TRY_CAST(ingestion_timestamp AS TIMESTAMP) DESC NULLS LAST,
                    LOWER(TRIM(CAST(is_duplicate AS STRING))) ASC,
                    bronze_id ASC
            ) AS rn
        FROM source_weather
        WHERE park_code IS NOT NULL
          AND poll_datetime IS NOT NULL
    )
    WHERE rn = 1

)

{% if is_incremental() %}
, all_landing_keys AS (

    SELECT park_code, poll_datetime FROM {{ source('landing_zone', 'sf_raw_weather') }}
    UNION ALL
    SELECT park_code, poll_datetime FROM {{ source('landing_zone', 'aq_raw_weather') }}

),

deleted_weather AS (

    SELECT
        t.* EXCEPT (is_deleted, deleted_at, bronze_loaded_at),
        TRUE                AS is_deleted,
        CURRENT_TIMESTAMP() AS deleted_at,
        CURRENT_TIMESTAMP() AS bronze_loaded_at
    FROM {{ this }} t
    LEFT ANTI JOIN all_landing_keys s
        ON t.park_code = s.park_code
       AND t.poll_datetime = s.poll_datetime
    WHERE t.is_deleted = FALSE

)
{% endif %}

SELECT * FROM deduplicated_weather
{% if is_incremental() %}
UNION ALL
SELECT * FROM deleted_weather
{% endif %}
