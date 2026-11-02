{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'merge',
        unique_key           = 'source_customer_id',
        on_schema_change     = 'sync_all_columns',
        schema               = 'bronze',
        tags                 = ['bronze', 'customers'],
        tblproperties        = {'delta.enableChangeDataFeed': 'true'}
    )
}}

WITH source_customers AS (

    SELECT
        *,
        'SF' AS source_region
    FROM {{ source('landing_zone', 'sf_raw_customers') }}
    {% if is_incremental() %}
    WHERE TRY_CAST(record_created_at AS TIMESTAMP) >= (
        SELECT COALESCE(MAX(TRY_CAST(record_created_at AS TIMESTAMP)), TIMESTAMP('1900-01-01'))
               - INTERVAL {{ var('bronze_overlap_days', 3) }} DAYS
        FROM {{ this }}
        WHERE source_region = 'SF'
    )
    {% endif %}

    UNION ALL

    SELECT
        *,
        'AQ' AS source_region
    FROM {{ source('landing_zone', 'aq_raw_customers') }}
    {% if is_incremental() %}
    WHERE TRY_CAST(record_created_at AS TIMESTAMP) >= (
        SELECT COALESCE(MAX(TRY_CAST(record_created_at AS TIMESTAMP)), TIMESTAMP('1900-01-01'))
               - INTERVAL {{ var('bronze_overlap_days', 3) }} DAYS
        FROM {{ this }}
        WHERE source_region = 'AQ'
    )
    {% endif %}

),

deduplicated_customers AS (

    SELECT
        * EXCEPT (rn),
        CASE
            WHEN UPPER(TRIM(cdc_operation)) = 'DELETE' THEN TRUE
            ELSE FALSE
        END                     AS is_deleted,
        CASE
            WHEN UPPER(TRIM(cdc_operation)) = 'DELETE' THEN CURRENT_TIMESTAMP()
            ELSE CAST(NULL AS TIMESTAMP)
        END                     AS deleted_at,
        CURRENT_TIMESTAMP()     AS bronze_loaded_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY source_customer_id
                ORDER BY
                    TRY_CAST(record_created_at AS TIMESTAMP) DESC NULLS LAST,
                    TRY_CAST(ingestion_timestamp AS TIMESTAMP) DESC NULLS LAST,
                    LOWER(TRIM(CAST(is_duplicate AS STRING))) ASC,
                    bronze_id ASC
            ) AS rn
        FROM source_customers
        WHERE source_customer_id IS NOT NULL
    )
    WHERE rn = 1

)

{% if is_incremental() %}
, all_landing_keys AS (

    SELECT source_customer_id FROM {{ source('landing_zone', 'sf_raw_customers') }}
    UNION ALL
    SELECT source_customer_id FROM {{ source('landing_zone', 'aq_raw_customers') }}

),

deleted_customers AS (

    SELECT
        t.* EXCEPT (is_deleted, deleted_at, bronze_loaded_at),
        TRUE                AS is_deleted,
        CURRENT_TIMESTAMP() AS deleted_at,
        CURRENT_TIMESTAMP() AS bronze_loaded_at
    FROM {{ this }} t
    LEFT ANTI JOIN all_landing_keys s
        ON t.source_customer_id = s.source_customer_id
    WHERE t.is_deleted = FALSE

)
{% endif %}

SELECT * FROM deduplicated_customers
{% if is_incremental() %}
UNION ALL
SELECT * FROM deleted_customers
{% endif %}
