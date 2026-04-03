{{
    config(
        materialized     = 'incremental',
        unique_key       = 'source_record_id',
        schema           = 'silver',
        tags             = ['silver', 'ride_operations', 'daily'],
        on_schema_change = 'sync_all_columns'
    )
}}

WITH bronze_raw_ride_operations AS (

    SELECT *
    FROM {{ ref('raw_ride_operations') }}

    {% if is_incremental() %}
        WHERE TRY_CAST(ingestion_timestamp AS TIMESTAMP) > (
            SELECT MAX(silver_loaded_at) FROM {{ this }}
        )
    {% endif %}

),

deduplicated_ride_operations AS (

    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY source_record_id
                ORDER BY TRY_CAST(ingestion_timestamp AS TIMESTAMP) DESC
            ) AS rn
        FROM bronze_raw_ride_operations
    )
    WHERE rn = 1

),

cleaned_ride_operations AS (

    SELECT

        TRIM(source_record_id)                                   AS source_record_id,
        INITCAP(TRIM(source_system))                             AS source_system,
        UPPER(TRIM(park_code))                                   AS park_code,
        UPPER(TRIM(source_region))                               AS source_region,

        UPPER(TRIM(ride_code))                                   AS ride_code,
        INITCAP(TRIM(ride_name))                                 AS ride_name,

        TRY_CAST(poll_datetime AS TIMESTAMP)                     AS poll_datetime,

        UPPER(TRIM(operational_status))                          AS operational_status,

        TRY_CAST(current_queue_length AS INT)                    AS current_queue_length,
        TRY_CAST(estimated_wait_min AS INT)                      AS estimated_wait_min,
        TRY_CAST(riders_last_cycle AS INT)                       AS riders_last_cycle,

        CASE
            WHEN LOWER(TRIM(safety_interlock_active)) = 'true' THEN TRUE
            WHEN LOWER(TRIM(safety_interlock_active)) = 'false' THEN FALSE
            ELSE NULL
        END                                                      AS safety_interlock_active,

        TRIM(lead_operator_id)                                   AS lead_operator_id,

        TRY_CAST(ingestion_timestamp AS TIMESTAMP)               AS source_ingested_at,
        CAST(ingestion_date AS DATE)                             AS ingestion_date,
        TRIM(batch_id)                                           AS batch_id,
        LOWER(TRIM(pipeline_name))                               AS pipeline_name,
        UPPER(TRIM(validation_status))                           AS validation_status,

        CASE
            WHEN LOWER(TRIM(is_duplicate)) = 'true'  THEN TRUE
            WHEN LOWER(TRIM(is_duplicate)) = 'false' THEN FALSE
            ELSE NULL
        END                                                      AS is_duplicate

    FROM deduplicated_ride_operations

),

silver_ride_operations AS (

    SELECT

        source_record_id,

        REGEXP_REPLACE(
            {{ dbt_utils.generate_surrogate_key(
                ['ride_code']
            ) }},
            '^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$',
            '$1-$2-$3-$4-$5'
        ) AS ride_sk,

        -- Identity
        source_system,
        park_code,
        source_region,

        -- Ride
        ride_code,
        ride_name,

        poll_datetime,

        CAST(poll_datetime AS DATE)                              AS poll_date,
        DATE_FORMAT(poll_datetime, 'EEEE')                       AS poll_day_of_week,
        HOUR(poll_datetime)                                      AS poll_hour,

        CASE
            WHEN HOUR(poll_datetime) BETWEEN 6  AND 11 THEN 'Morning'
            WHEN HOUR(poll_datetime) BETWEEN 12 AND 16 THEN 'Afternoon'
            WHEN HOUR(poll_datetime) BETWEEN 17 AND 20 THEN 'Evening'
            ELSE 'Off-Hours'
        END                                                      AS time_of_day,

        operational_status,

        current_queue_length,
        estimated_wait_min,
        riders_last_cycle,

        CASE
            WHEN estimated_wait_min >= 60 THEN 'Very High'
            WHEN estimated_wait_min >= 30 THEN 'High'
            WHEN estimated_wait_min >= 10 THEN 'Medium'
            ELSE 'Low'
        END                                                      AS wait_time_category,

        safety_interlock_active,

        CASE
            WHEN safety_interlock_active = TRUE THEN 'Attention Required'
            ELSE 'Normal'
        END                                                      AS safety_status,

        lead_operator_id,

        source_ingested_at,
        ingestion_date,
        batch_id,
        pipeline_name,
        validation_status,
        is_duplicate,

        CURRENT_TIMESTAMP()                                      AS silver_loaded_at

    FROM cleaned_ride_operations

)

SELECT *
FROM silver_ride_operations