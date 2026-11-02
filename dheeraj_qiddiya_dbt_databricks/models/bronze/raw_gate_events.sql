{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'merge',
        unique_key           = 'source_event_id',
        on_schema_change     = 'sync_all_columns',
        schema               = 'bronze',
        tags                 = ['bronze', 'gate_events'],
        tblproperties        = {'delta.enableChangeDataFeed': 'true'}
    )
}}

WITH source_gate_events AS (

    SELECT
        *,
        'SF' AS source_region
    FROM {{ source('landing_zone', 'sf_raw_gate_events') }}
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
    FROM {{ source('landing_zone', 'aq_raw_gate_events') }}
    {% if is_incremental() %}
    WHERE TRY_CAST(ingestion_timestamp AS TIMESTAMP) >= (
        SELECT COALESCE(MAX(TRY_CAST(ingestion_timestamp AS TIMESTAMP)), TIMESTAMP('1900-01-01'))
               - INTERVAL {{ var('bronze_overlap_days', 3) }} DAYS
        FROM {{ this }}
        WHERE source_region = 'AQ'
    )
    {% endif %}

),

deduplicated_gate_events AS (

    SELECT
        * EXCEPT (rn),
        FALSE                   AS is_deleted,
        CAST(NULL AS TIMESTAMP) AS deleted_at,
        CURRENT_TIMESTAMP()     AS bronze_loaded_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY source_event_id
                ORDER BY
                    TRY_CAST(ingestion_timestamp AS TIMESTAMP) DESC NULLS LAST,
                    TRY_CAST(event_datetime AS TIMESTAMP) DESC NULLS LAST,
                    LOWER(TRIM(CAST(is_duplicate AS STRING))) ASC,
                    bronze_id ASC
            ) AS rn
        FROM source_gate_events
        WHERE source_event_id IS NOT NULL
    )
    WHERE rn = 1

)

{% if is_incremental() %}
, all_landing_keys AS (

    SELECT source_event_id FROM {{ source('landing_zone', 'sf_raw_gate_events') }}
    UNION ALL
    SELECT source_event_id FROM {{ source('landing_zone', 'aq_raw_gate_events') }}

),

deleted_gate_events AS (

    SELECT
        t.* EXCEPT (is_deleted, deleted_at, bronze_loaded_at),
        TRUE                AS is_deleted,
        CURRENT_TIMESTAMP() AS deleted_at,
        CURRENT_TIMESTAMP() AS bronze_loaded_at
    FROM {{ this }} t
    LEFT ANTI JOIN all_landing_keys s
        ON t.source_event_id = s.source_event_id
    WHERE t.is_deleted = FALSE

)
{% endif %}

SELECT * FROM deduplicated_gate_events
{% if is_incremental() %}
UNION ALL
SELECT * FROM deleted_gate_events
{% endif %}
