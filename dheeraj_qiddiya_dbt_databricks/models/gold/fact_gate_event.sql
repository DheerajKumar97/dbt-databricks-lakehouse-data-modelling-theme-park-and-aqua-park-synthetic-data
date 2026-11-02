{{
    config(
        materialized = 'table',
        schema       = 'gold',
        tags         = ['gold', 'fact', 'gate_event']
    )
}}

{#
    fact_gate_event — Gate entry/exit fact table

    Grain: one row per gate scan event.

    FK Join Strategy (100% RI):
        INNER JOIN → date_key, park_key, gate_key  (NOT NULL — orphan guard)
        LEFT  JOIN → customer_key                   (NULLABLE — anonymous visitors)
#}

WITH silver_gate AS (

    SELECT *
    FROM {{ ref('silver_gate_events') }}
    WHERE COALESCE(is_deleted, FALSE) = FALSE
      AND event_date IS NOT NULL

),

fact_gate_event AS (

    SELECT

        -- Fact Surrogate Key
        g.gate_event_sk                                          AS gate_event_key,

        -- ═══ Dimension FKs ═══

        -- Date (INNER — mandatory)
        d.date_key,

        -- Park (INNER — mandatory)
        p.park_key,

        -- Customer (LEFT — nullable: anonymous visitors)
        c.customer_key,

        -- Gate (INNER — mandatory)
        gt.gate_key,

        -- ═══ Degenerate Dimensions ═══
        g.source_event_id,
        g.ticket_barcode,

        -- ═══ Descriptive Attributes ═══
        g.event_type,
        g.event_datetime,
        g.rejection_reason,
        g.is_entry,
        g.is_rejected,

        -- ═══ Measures ═══
        1                                                        AS event_count,

        -- ═══ Audit ═══
        CURRENT_TIMESTAMP()                                      AS gold_loaded_at

    FROM silver_gate g

    -- ── NOT NULL FKs → INNER JOIN (orphan protection) ────────────────────
    INNER JOIN {{ ref('dim_date') }} d
        ON d.full_date = g.event_date

    INNER JOIN {{ ref('dim_park') }} p
        ON p.park_code = g.park_code

    INNER JOIN {{ ref('dim_gate') }} gt
        ON  gt.gate_id  = g.gate_id
        AND gt.park_key  = p.park_key

    -- ── NULLABLE FKs → LEFT JOIN ─────────────────────────────────────────
    LEFT JOIN {{ ref('dim_customer') }} c
        ON c.source_customer_id = g.source_customer_id

)

SELECT * FROM fact_gate_event
