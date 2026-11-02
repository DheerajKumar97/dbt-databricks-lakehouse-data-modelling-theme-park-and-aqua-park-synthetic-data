{{
    config(
        materialized = 'table',
        schema       = 'gold',
        tags         = ['gold', 'fact', 'feedback']
    )
}}

{#
    fact_customer_feedback — Customer feedback / NPS fact table

    Grain: one row per feedback submission.

    FK Join Strategy (100% RI):
        INNER JOIN → park_key         (NOT NULL — every feedback has a park)
        LEFT  JOIN → date_key         (NULLABLE — feedback may have unparseable date)
        LEFT  JOIN → customer_key     (NULLABLE — anonymous feedback allowed)
#}

WITH silver_fb AS (

    SELECT *
    FROM {{ ref('silver_feedback') }}
    WHERE COALESCE(is_deleted, FALSE) = FALSE

),

fact_customer_feedback AS (

    SELECT

        -- Fact Surrogate Key
        f.feedback_sk                                            AS feedback_key,

        -- ═══ Dimension FKs ═══

        -- Date (LEFT — nullable: bad/missing date allowed)
        d.date_key,

        -- Park (INNER — mandatory)
        p.park_key,

        -- Customer (LEFT — nullable: anonymous feedback)
        c.customer_key,

        -- ═══ Degenerate Dimensions ═══
        f.source_feedback_id,

        -- ═══ Measures / Scores ═══
        f.overall_score,
        f.nps_raw,
        f.cleanliness_score,
        f.staff_score,
        f.avg_score,

        -- ═══ Derived Attributes ═══
        f.nps_category,
        f.overall_score_category,

        -- ═══ Complaint ═══
        f.has_complaint,
        f.complaint_text,

        -- ═══ Audit ═══
        CURRENT_TIMESTAMP()                                      AS gold_loaded_at

    FROM silver_fb f

    -- ── NOT NULL FK → INNER JOIN ─────────────────────────────────────────
    INNER JOIN {{ ref('dim_park') }} p
        ON p.park_code = f.park_code

    -- ── NULLABLE FKs → LEFT JOIN ─────────────────────────────────────────
    LEFT JOIN {{ ref('dim_date') }} d
        ON d.full_date = f.feedback_date

    LEFT JOIN {{ ref('dim_customer') }} c
        ON c.source_customer_id = f.source_customer_id

)

SELECT * FROM fact_customer_feedback
