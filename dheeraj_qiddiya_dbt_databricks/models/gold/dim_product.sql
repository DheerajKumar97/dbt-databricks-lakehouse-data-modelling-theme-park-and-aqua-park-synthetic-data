{{
    config(
        materialized = 'table',
        schema       = 'gold',
        tags         = ['gold', 'dimension', 'product']
    )
}}

{#
    dim_product — Product dimension

    Sourced from silver_products.
    product_key = silver product_sk (deterministic UUID via dbt_utils).
    Includes derived category_group and price_band from silver.
#}

WITH dim_product AS (

    SELECT

        -- Surrogate Key (from silver)
        product_sk                                               AS product_key,

        -- Natural Key
        product_code,

        -- Attributes
        product_name,
        category                                                 AS product_category,
        price                                                    AS list_price,

        -- Derived (from silver)
        category_group,
        price_band,

        -- Audit
        CURRENT_TIMESTAMP()                                      AS gold_loaded_at

    FROM {{ ref('silver_products') }}
    WHERE COALESCE(is_deleted, FALSE) = FALSE

)

SELECT * FROM dim_product
