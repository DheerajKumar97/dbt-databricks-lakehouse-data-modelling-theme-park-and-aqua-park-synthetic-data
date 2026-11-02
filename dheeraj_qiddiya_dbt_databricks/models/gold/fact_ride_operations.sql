{{
    config(
        materialized = 'table',
        schema       = 'gold',
        tags         = ['gold', 'fact', 'ride_operations']
    )
}}

{#
    fact_ride_operations — Ride telemetry / operations fact table

    Grain: one row per ride poll record.

    FK Join Strategy (100% RI):
        INNER JOIN → date_key, park_key, ride_key  (NOT NULL — orphan guard)
        LEFT  JOIN → staff_key                      (NULLABLE — operator may be unrecorded)
#}

WITH silver_rides AS (

    SELECT *
    FROM {{ ref('silver_ride_opeartions') }}
    WHERE COALESCE(is_deleted, FALSE) = FALSE
      AND poll_date IS NOT NULL

),

fact_ride_operations AS (

    SELECT

        -- Fact Surrogate Key
        REGEXP_REPLACE(
            {{ dbt_utils.generate_surrogate_key(
                ['r.source_record_id']
            ) }},
            '^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$',
            '$1-$2-$3-$4-$5'
        )                                                        AS ride_ops_key,

        -- ═══ Dimension FKs ═══

        -- Date (INNER — mandatory)
        d.date_key,

        -- Park (INNER — mandatory)
        p.park_key,

        -- Ride (INNER — mandatory)
        rd.ride_key,

        -- Staff / Lead Operator (LEFT — nullable)
        s.staff_key,

        -- ═══ Degenerate Dimensions ═══
        r.source_record_id,

        -- ═══ Descriptive Attributes ═══
        r.poll_datetime,
        r.operational_status,
        r.safety_interlock_active,
        r.safety_status,
        r.wait_time_category,

        -- ═══ Measures ═══
        r.current_queue_length,
        r.estimated_wait_min,
        r.riders_last_cycle,

        -- ═══ Audit ═══
        CURRENT_TIMESTAMP()                                      AS gold_loaded_at

    FROM silver_rides r

    -- ── NOT NULL FKs → INNER JOIN (orphan protection) ────────────────────
    INNER JOIN {{ ref('dim_date') }} d
        ON d.full_date = r.poll_date

    INNER JOIN {{ ref('dim_park') }} p
        ON p.park_code = r.park_code

    INNER JOIN {{ ref('dim_ride') }} rd
        ON rd.ride_code = r.ride_code

    -- ── NULLABLE FKs → LEFT JOIN ─────────────────────────────────────────
    LEFT JOIN {{ ref('dim_staff') }} s
        ON s.source_employee_id = r.lead_operator_id

)

SELECT * FROM fact_ride_operations
