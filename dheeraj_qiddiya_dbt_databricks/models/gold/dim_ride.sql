{{
    config(
        materialized = 'table',
        schema       = 'gold',
        tags         = ['gold', 'dimension', 'ride']
    )
}}

{#
    dim_ride — Ride / Attraction dimension

    Sourced from silver_ride_opeartions (deduped on ride_code).
    ride_key = silver ride_sk (deterministic UUID via dbt_utils on ride_code).
    Joins to dim_park for park_key FK.
#}

WITH deduped_rides AS (

    SELECT
        ride_sk,
        ride_code,
        ride_name,
        park_code,
        ROW_NUMBER() OVER (
            PARTITION BY ride_code
            ORDER BY poll_datetime DESC
        ) AS rn
    FROM {{ ref('silver_ride_opeartions') }}
    WHERE COALESCE(is_deleted, FALSE) = FALSE

),

dim_ride AS (

    SELECT

        -- Surrogate Key (from silver)
        r.ride_sk                                                AS ride_key,

        -- Natural Key
        r.ride_code,

        -- Attributes
        r.ride_name,

        -- Park FK
        p.park_key,

        -- Audit
        CURRENT_TIMESTAMP()                                      AS gold_loaded_at

    FROM deduped_rides r

    INNER JOIN {{ ref('dim_park') }} p
        ON p.park_code = r.park_code

    WHERE r.rn = 1

)

SELECT * FROM dim_ride
