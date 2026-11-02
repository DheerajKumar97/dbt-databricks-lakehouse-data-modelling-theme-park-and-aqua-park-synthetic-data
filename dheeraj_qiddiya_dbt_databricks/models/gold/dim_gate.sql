{{
    config(
        materialized = 'table',
        schema       = 'gold',
        tags         = ['gold', 'dimension', 'gate']
    )
}}

{#
    dim_gate — Gate dimension

    Sourced from silver_gate_events (deduped on gate_id + park_code).
    gate_key = new UUID surrogate key (hashed on gate_id + park_code).
    Joins to dim_park for park_key FK.
#}

WITH distinct_gates AS (

    SELECT
        gate_id,
        MAX(gate_location)    AS gate_location,
        MAX(zone_code)        AS zone_code,
        park_code
    FROM {{ ref('silver_gate_events') }}
    WHERE COALESCE(is_deleted, FALSE) = FALSE
    GROUP BY gate_id, park_code

),

dim_gate AS (

    SELECT

        -- Surrogate Key (new — gate has no SK in silver)
        REGEXP_REPLACE(
            {{ dbt_utils.generate_surrogate_key(
                ['g.gate_id', 'g.park_code']
            ) }},
            '^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$',
            '$1-$2-$3-$4-$5'
        )                                                        AS gate_key,

        -- Natural Key
        g.gate_id,

        -- Attributes
        g.gate_location,
        g.zone_code,

        -- Park FK
        p.park_key,

        -- Audit
        CURRENT_TIMESTAMP()                                      AS gold_loaded_at

    FROM distinct_gates g

    INNER JOIN {{ ref('dim_park') }} p
        ON p.park_code = g.park_code

)

SELECT * FROM dim_gate
