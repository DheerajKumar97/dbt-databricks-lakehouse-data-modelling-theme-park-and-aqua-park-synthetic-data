{{
    config(
        materialized     = 'incremental',
        unique_key       = 'transaction_sk',
        schema           = 'silver',
        tags             = ['silver', 'transactions', 'daily'],
        on_schema_change = 'sync_all_columns'
    )
}}

WITH bronze_raw_transactions AS (

    SELECT *
    FROM {{ ref('raw_transactions') }}

    {% if is_incremental() %}
        WHERE TRY_CAST(ingestion_timestamp AS TIMESTAMP) > (
            SELECT MAX(silver_loaded_at) FROM {{ this }}
        )
    {% endif %}

),

deduped_raw_transactions AS (

    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY source_transaction_id, product_code
                ORDER BY TRY_CAST(ingestion_timestamp AS TIMESTAMP) DESC
            ) AS rn
        FROM bronze_raw_transactions
    )
    WHERE rn = 1

),

cast_and_clean AS (

    SELECT

        source_transaction_id,
        source_system,
        source_table,
        park_code,
        terminal_id,
        cashier_id,

        -- ✅ FIX: renamed from customer_id → source_customer_id
        -- This ensures generate_surrogate_key hashes the same column name
        -- as silver_customers.sql, producing matching customer_sk values
        customer_id                                                AS source_customer_id,

        TRY_CAST(transaction_datetime AS TIMESTAMP)                AS transaction_timestamp,
        CAST(TRY_CAST(transaction_datetime AS TIMESTAMP) AS DATE)  AS transaction_date,

        product_code,
        INITCAP(TRIM(product_description))                         AS product_description,
        UPPER(TRIM(product_category))                              AS product_category,

        TRY_CAST(unit_price AS DOUBLE)                             AS unit_price,
        TRY_CAST(quantity AS INT)                                  AS quantity,

        INITCAP(TRIM(payment_method))                              AS payment_method,
        UPPER(TRIM(transaction_status))                            AS transaction_status,
        INITCAP(TRIM(sales_channel))                               AS sales_channel,

        CASE
            WHEN LOWER(TRIM(is_group_booking)) IN ('1','true','yes','y') THEN TRUE
            WHEN LOWER(TRIM(is_group_booking)) IN ('0','false','no','n') THEN FALSE
            ELSE NULL
        END                                                        AS is_group_booking,

        TRY_CAST(group_size AS INT)                                AS group_size,
        booking_reference,

        -- Financials
        TRY_CAST(gross_amount AS DOUBLE)                           AS gross_amount,
        TRY_CAST(discount_amount AS DOUBLE)                        AS discount_amount,
        TRY_CAST(net_amount AS DOUBLE)                             AS net_amount,
        TRY_CAST(vat_amount AS DOUBLE)                             AS vat_amount,
        TRY_CAST(total_charged AS DOUBLE)                          AS total_charged,

        refund_reference_id,
        UPPER(TRIM(cdc_operation))                                 AS cdc_operation,

        -- Metadata
        TRY_CAST(ingestion_timestamp AS TIMESTAMP)                 AS ingestion_timestamp,
        TRY_CAST(ingestion_date AS DATE)                           AS ingestion_date,
        batch_id,
        pipeline_name,
        UPPER(TRIM(validation_status))                             AS validation_status,

        CASE
            WHEN LOWER(TRIM(is_duplicate)) IN ('true','yes','1') THEN TRUE
            WHEN LOWER(TRIM(is_duplicate)) IN ('false','no','0') THEN FALSE
            ELSE NULL
        END                                                        AS is_duplicate,

        source_region

    FROM deduped_raw_transactions

),

silver_transactions AS (

    SELECT

        -- Transaction surrogate key (UUID format)
        REGEXP_REPLACE(
            {{ dbt_utils.generate_surrogate_key(['source_transaction_id']) }},
            '^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$',
            '$1-$2-$3-$4-$5'
        )                                                          AS transaction_sk,

        source_transaction_id,
        product_code,

        -- ✅ FIX: generate_surrogate_key now uses 'source_customer_id'
        -- matching silver_customers.sql exactly → 0 orphans
        REGEXP_REPLACE(
            {{ dbt_utils.generate_surrogate_key(['source_customer_id']) }},
            '^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$',
            '$1-$2-$3-$4-$5'
        )                                                          AS customer_sk,

        source_customer_id,
        cashier_id,
        terminal_id,
        park_code,

        transaction_timestamp,
        transaction_date,

        product_description,
        product_category,

        unit_price,
        quantity,
        gross_amount,
        discount_amount,
        net_amount,
        vat_amount,
        total_charged,

        (unit_price * quantity)                                    AS calculated_gross_amount,

        CASE
            WHEN total_charged = 0 THEN 0
            ELSE (discount_amount / NULLIF(gross_amount, 0)) * 100
        END                                                        AS discount_percentage,

        is_group_booking,
        group_size,
        booking_reference,

        payment_method,
        transaction_status,
        sales_channel,
        cdc_operation,

        ingestion_timestamp,
        ingestion_date,
        batch_id,
        pipeline_name,
        validation_status,
        is_duplicate,
        source_region,

        CURRENT_TIMESTAMP()                                        AS silver_loaded_at

    FROM cast_and_clean

)

SELECT *
FROM silver_transactions
