{{
    config(
        materialized     = 'incremental',
        unique_key       = 'product_sk',
        schema           = 'silver',
        tags             = ['silver', 'products', 'daily'],
        on_schema_change = 'sync_all_columns'
    )
}}

WITH bronze_raw_products AS (

    SELECT *
    FROM {{ ref('raw_products') }}

    {% if is_incremental() %}
        WHERE bronze_loaded_at > (
            SELECT COALESCE(MAX(silver_loaded_at), TIMESTAMP('1900-01-01')) FROM {{ this }}
        )
    {% endif %}

),

deduplicated_products AS (

    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY product_code
                ORDER BY TRY_CAST(ingestion_timestamp AS TIMESTAMP) DESC
            ) AS rn
        FROM bronze_raw_products
    )
    WHERE rn = 1

),

cleaned_products AS (

    SELECT

        -- Identity
        TRIM(product_code)                                     AS product_code,
        INITCAP(TRIM(description))                             AS product_name,
        UPPER(TRIM(category))                                  AS category,
        TRY_CAST(price AS DOUBLE)                              AS price,
        TRY_CAST(ingestion_timestamp AS TIMESTAMP)             AS source_ingested_at,
        CAST(ingestion_date AS DATE)                           AS ingestion_date,
        TRIM(batch_id)                                         AS batch_id,
        LOWER(TRIM(pipeline_name))                             AS pipeline_name,
        UPPER(TRIM(validation_status))                         AS validation_status,

        CASE
            WHEN LOWER(TRIM(is_duplicate)) = 'true'  THEN TRUE
            WHEN LOWER(TRIM(is_duplicate)) = 'false' THEN FALSE
            ELSE NULL
        END                                                    AS is_duplicate,

        UPPER(TRIM(source_region))                             AS source_region,

        COALESCE(is_deleted, FALSE)                            AS is_deleted,
        deleted_at

    FROM deduplicated_products

),

silver_products AS (

    SELECT

        REGEXP_REPLACE(
            {{ dbt_utils.generate_surrogate_key(
                ['product_code']
            ) }},
            '^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$',
            '$1-$2-$3-$4-$5'
        )  AS product_sk,
        product_code,
        product_name,
        category,
        source_region,

        price,

        CASE
            WHEN category = 'F&B'        THEN 'Food & Beverage'
            WHEN category = 'MERCH'      THEN 'Merchandise'
            WHEN category = 'ADMISSION'  THEN 'Tickets'
            WHEN category = 'FASTPASS'   THEN 'Fast Track'
            ELSE 'Other'
        END                                                    AS category_group,

        CASE
            WHEN price < 50  THEN 'Low'
            WHEN price < 200 THEN 'Medium'
            ELSE 'High'
        END                                                    AS price_band,

        source_ingested_at,
        ingestion_date,
        batch_id,
        pipeline_name,
        validation_status,
        is_duplicate,

        is_deleted,
        deleted_at,

        CURRENT_TIMESTAMP()                                    AS silver_loaded_at

    FROM cleaned_products

)

{% if is_incremental() %}
, hard_deleted_in_bronze AS (

    SELECT
        t.* EXCEPT (is_deleted, deleted_at, silver_loaded_at),
        TRUE                AS is_deleted,
        CURRENT_TIMESTAMP() AS deleted_at,
        CURRENT_TIMESTAMP() AS silver_loaded_at
    FROM {{ this }} t
    LEFT ANTI JOIN {{ ref('raw_products') }} b
        ON t.product_code = b.product_code
    WHERE COALESCE(t.is_deleted, FALSE) = FALSE

)
{% endif %}

SELECT * FROM silver_products
{% if is_incremental() %}
UNION ALL
SELECT * FROM hard_deleted_in_bronze
{% endif %}
