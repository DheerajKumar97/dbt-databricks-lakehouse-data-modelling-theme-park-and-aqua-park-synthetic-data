{{
    config(
        materialized     = 'incremental',
        unique_key       = 'customer_sk',
        schema           = 'silver',
        tags             = ['silver', 'customers', 'daily'],
        on_schema_change = 'sync_all_columns'
    )
}}

WITH bronze_raw_customers AS (

    SELECT *
    FROM {{ ref('raw_customers') }}

    {% if is_incremental() %}
        WHERE TRY_CAST(ingestion_timestamp AS TIMESTAMP) > (
            SELECT MAX(silver_loaded_at) FROM {{ this }}
        )
    {% endif %}

),

{# filtered_raw_customers AS (

    SELECT *
    FROM bronze_raw_customers
    WHERE UPPER(TRIM(validation_status)) = 'VALID'
      AND UPPER(TRIM(cdc_operation))    != 'DELETE'

), #}

deduped_raw_customers AS (

    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY source_customer_id, source_system
                ORDER BY TRY_CAST(record_created_at AS TIMESTAMP) DESC
            ) AS rn
        FROM bronze_raw_customers  
    )
    WHERE rn = 1

),

cast_and_clean AS (

    SELECT

        -- Identity
        source_customer_id,
        source_system,
        source_region,

        -- Names
        INITCAP(TRIM(full_name))                                    AS full_name,
        INITCAP(TRIM(first_name))                                   AS first_name,
        INITCAP(TRIM(last_name))                                    AS last_name,

        -- Demographics
        UPPER(TRIM(gender))                                         AS gender,
        INITCAP(TRIM(nationality))                                  AS nationality,
        TRY_CAST(date_of_birth AS DATE)                             AS date_of_birth,

        -- Contact
        LOWER(TRIM(email))                                          AS email,
        TRIM(phone)                                                 AS phone,

        -- Loyalty
        INITCAP(TRIM(resident_type))                                AS resident_type,
        INITCAP(TRIM(loyalty_tier))                                 AS loyalty_tier,
        loyalty_card_number,
        CAST(loyalty_points AS INT)                                 AS loyalty_points,

        -- Registration
        TRY_CAST(registration_date AS DATE)                         AS registration_date,

        -- Boolean: app_installed → 'True'/'False'
        CASE
            WHEN LOWER(TRIM(app_installed)) = 'true'  THEN TRUE
            WHEN LOWER(TRIM(app_installed)) = 'false' THEN FALSE
            ELSE NULL
        END                                                         AS app_installed,

        -- Boolean: opt-ins → 'Y'/'N'
        CASE
            WHEN UPPER(TRIM(email_opt_in)) = 'Y' THEN TRUE
            WHEN UPPER(TRIM(email_opt_in)) = 'N' THEN FALSE
            ELSE NULL
        END                                                         AS email_opt_in,

        CASE
            WHEN UPPER(TRIM(sms_opt_in)) = 'Y' THEN TRUE
            WHEN UPPER(TRIM(sms_opt_in)) = 'N' THEN FALSE
            ELSE NULL
        END                                                         AS sms_opt_in,

        -- Source timestamp
        TRY_CAST(record_created_at AS TIMESTAMP)                    AS source_created_at

    FROM deduped_raw_customers      

),

silver_customers AS (          

    SELECT

        -- Surrogate key
        REGEXP_REPLACE(
            {{ dbt_utils.generate_surrogate_key(
                ['source_customer_id']
            ) }},
            '^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$',
            '$1-$2-$3-$4-$5'
        )                                                               AS customer_sk,

        -- Identity
        source_customer_id,
        source_system,
        source_region,

        -- Names
        full_name,
        first_name,
        last_name,
        CONCAT(first_name, ' ', last_name)                          AS derived_full_name,

        -- Demographics
        gender,
        nationality,
        date_of_birth,
        DATEDIFF(
            year,       
            date_of_birth,
            CURRENT_DATE()
        )                                                           AS age,

        -- Contact
        email,
        phone,

        -- Loyalty
        resident_type,
        loyalty_tier,
        loyalty_card_number,
        loyalty_points,

        -- Registration
        registration_date,
        DATEDIFF(
            day,              
            registration_date,
            CURRENT_DATE()
        )                                                           AS tenure_days,

        -- Booleans
        app_installed,
        email_opt_in,
        sms_opt_in,
        CASE
            WHEN email_opt_in = TRUE
              OR sms_opt_in   = TRUE THEN TRUE
            ELSE FALSE
        END                                                         AS is_any_opted_in,

        -- Lineage
        source_created_at,

        -- Silver audit
        CURRENT_TIMESTAMP()                                         AS silver_loaded_at

    FROM cast_and_clean

)

SELECT * FROM silver_customers