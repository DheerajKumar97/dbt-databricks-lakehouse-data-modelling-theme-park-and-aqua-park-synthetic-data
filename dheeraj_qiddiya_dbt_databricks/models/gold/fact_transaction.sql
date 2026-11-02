{{
    config(
        materialized = 'table',
        schema       = 'gold',
        tags         = ['gold', 'fact', 'transaction']
    )
}}

{#
    fact_transaction — Transactional fact table

    Grain: one row per transaction line item.
    
    FK Join Strategy (100% RI):
        INNER JOIN → date_key, park_key, product_key   (NOT NULL — orphan guard)
        LEFT  JOIN → customer_key, staff_key            (NULLABLE — anonymous/unmatched allowed)
#}

WITH silver_txn AS (

    SELECT *
    FROM {{ ref('silver_transactions') }}
    WHERE COALESCE(is_deleted, FALSE) = FALSE
      AND transaction_date IS NOT NULL

),

fact_transaction AS (

    SELECT

        -- Fact Surrogate Key
        t.transaction_sk                                         AS transaction_key,

        -- ═══ Dimension FKs ═══

        -- Date (INNER — mandatory)
        d.date_key,

        -- Park (INNER — mandatory)
        p.park_key,

        -- Customer (LEFT — nullable: anonymous or unmatched)
        c.customer_key,

        -- Product (INNER — mandatory)
        pr.product_key,

        -- Staff / Cashier (LEFT — nullable)
        s.staff_key,

        -- ═══ Degenerate Dimensions ═══
        t.source_transaction_id,
        t.terminal_id,
        t.booking_reference,
        CAST(NULL AS STRING)                                     AS refund_reference_id,

        -- ═══ Descriptive Attributes ═══
        t.payment_method,
        t.transaction_status,
        t.sales_channel,
        t.is_group_booking,
        t.group_size,

        -- ═══ Measures ═══
        t.quantity,
        t.unit_price,
        t.gross_amount,
        COALESCE(t.discount_amount, 0)                           AS discount_amount,
        t.net_amount,
        t.vat_amount,
        t.total_charged,
        CAST(NULL AS DOUBLE)                                     AS calculated_gross_amount,
        CAST(NULL AS DOUBLE)                                     AS discount_percentage,

        -- ═══ Audit ═══
        CURRENT_TIMESTAMP()                                      AS gold_loaded_at

    FROM silver_txn t

    -- ── NOT NULL FKs → INNER JOIN (orphan protection) ────────────────────
    INNER JOIN {{ ref('dim_date') }} d
        ON d.full_date = CAST(t.transaction_date AS DATE)

    INNER JOIN {{ ref('dim_park') }} p
        ON p.park_code = t.park_code

    INNER JOIN {{ ref('dim_product') }} pr
        ON pr.product_code = t.product_code

    -- ── NULLABLE FKs → LEFT JOIN ─────────────────────────────────────────
    LEFT JOIN {{ ref('dim_customer') }} c
        ON c.source_customer_id = t.source_customer_id

    LEFT JOIN {{ ref('dim_staff') }} s
        ON s.source_employee_id = t.cashier_id

)

SELECT * FROM fact_transaction