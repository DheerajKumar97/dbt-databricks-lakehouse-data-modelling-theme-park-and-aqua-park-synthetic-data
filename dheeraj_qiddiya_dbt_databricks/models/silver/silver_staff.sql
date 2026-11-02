{{
    config(
        materialized     = 'incremental',
        unique_key       = 'staff_sk',
        schema           = 'silver',
        tags             = ['silver', 'staff', 'daily'],
        on_schema_change = 'sync_all_columns'
    )
}}

WITH bronze_raw_staff AS (

    SELECT *
    FROM {{ ref('raw_staff') }}

    {% if is_incremental() %}
        WHERE bronze_loaded_at > (
            SELECT COALESCE(MAX(silver_loaded_at), TIMESTAMP('1900-01-01')) FROM {{ this }}
        )
    {% endif %}

),

deduped_raw_staff AS (

    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY source_employee_id
                ORDER BY TRY_CAST(ingestion_timestamp AS TIMESTAMP) DESC
            ) AS rn
        FROM bronze_raw_staff
    )
    WHERE rn = 1

),

cleaned_raw_staff AS (

    SELECT

        -- Identity
        source_employee_id,
        source_region,

        -- Department
        INITCAP(TRIM(department))                              AS department,

        -- Nationality
        INITCAP(TRIM(nationality))                             AS nationality,

        -- Boolean normalization
        CASE
            WHEN LOWER(TRIM(is_active)) IN ('1','true','yes') THEN TRUE
            WHEN LOWER(TRIM(is_active)) IN ('0','false','no') THEN FALSE
            ELSE NULL
        END                                                    AS is_active,

        -- Metadata
        TRY_CAST(ingestion_timestamp AS TIMESTAMP)             AS ingestion_timestamp,
        TRY_CAST(ingestion_date AS DATE)                       AS ingestion_date,
        batch_id,
        pipeline_name,
        UPPER(TRIM(validation_status))                         AS validation_status,

        -- Duplicate flag
        CASE
            WHEN LOWER(TRIM(is_duplicate)) IN ('true','yes','1') THEN TRUE
            WHEN LOWER(TRIM(is_duplicate)) IN ('false','no','0') THEN FALSE
            ELSE NULL
        END                                                    AS is_duplicate,

        COALESCE(is_deleted, FALSE)                            AS is_deleted,
        deleted_at

    FROM deduped_raw_staff

),

silver_staff AS (

    SELECT

        -- Surrogate Key
        REGEXP_REPLACE(
            {{ dbt_utils.generate_surrogate_key(
                ['source_employee_id']
            ) }},
            '^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$',
            '$1-$2-$3-$4-$5'
        )   AS staff_sk,

        -- Business Columns
        source_employee_id,
        source_region,
        department,
        nationality,
        is_active,

        -- Metadata
        ingestion_timestamp,
        ingestion_date,
        batch_id,
        pipeline_name,
        validation_status,
        is_duplicate,

        -- Derived Column
        CASE
            WHEN is_active = TRUE THEN 'ACTIVE'
            ELSE 'INACTIVE'
        END                                                   AS staff_status,

        -- Soft delete tracking
        is_deleted,
        deleted_at,

        -- Audit
        CURRENT_TIMESTAMP()                                   AS silver_loaded_at

    FROM cleaned_raw_staff

)

{% if is_incremental() %}
, hard_deleted_in_bronze AS (

    SELECT
        t.* EXCEPT (is_deleted, deleted_at, silver_loaded_at),
        TRUE                AS is_deleted,
        CURRENT_TIMESTAMP() AS deleted_at,
        CURRENT_TIMESTAMP() AS silver_loaded_at
    FROM {{ this }} t
    LEFT ANTI JOIN {{ ref('raw_staff') }} b
        ON t.source_employee_id = b.source_employee_id
    WHERE COALESCE(t.is_deleted, FALSE) = FALSE

)
{% endif %}

SELECT * FROM silver_staff
{% if is_incremental() %}
UNION ALL
SELECT * FROM hard_deleted_in_bronze
{% endif %}
