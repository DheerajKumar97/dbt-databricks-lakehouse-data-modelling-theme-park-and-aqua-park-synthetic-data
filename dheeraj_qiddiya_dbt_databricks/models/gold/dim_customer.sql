{{
    config(
        materialized = 'table',
        schema       = 'gold',
        tags         = ['gold', 'dimension', 'customer']
    )
}}

{#
    dim_customer — Customer dimension

    Sourced from silver_customers (active records only: is_deleted = FALSE).
    customer_key = silver customer_sk (deterministic UUID via dbt_utils).
    Carries forward all SCD Type-1 attributes without exposing is_deleted in Gold.
#}

WITH dim_customer AS (

    SELECT

        -- Surrogate Key (from silver)
        customer_sk                                              AS customer_key,

        -- Natural Key
        source_customer_id,

        -- Name
        full_name,
        first_name,
        last_name,

        -- Demographics
        gender,
        date_of_birth,
        nationality,

        -- Contact
        email,
        phone,

        -- Loyalty
        resident_type,
        loyalty_tier,
        loyalty_card_number,
        loyalty_points,

        -- Flags
        app_installed,
        email_opt_in,
        sms_opt_in,

        -- Registration
        registration_date,

        -- Source
        source_system,

        -- Audit
        CURRENT_TIMESTAMP()                                      AS gold_loaded_at

    FROM {{ ref('silver_customers') }}
    WHERE COALESCE(is_deleted, FALSE) = FALSE

)

SELECT * FROM dim_customer
