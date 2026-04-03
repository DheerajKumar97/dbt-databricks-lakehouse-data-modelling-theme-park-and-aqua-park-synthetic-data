{{
    config(
        materialized     = 'incremental',
        unique_key       = 'gate_event_sk',
        schema           = 'silver',
        tags             = ['silver', 'gate_events', 'daily'],
        on_schema_change = 'sync_all_columns'
    )
}}

WITH bronze_raw_gate_events AS (

    SELECT *
    FROM {{ ref('raw_gate_events') }}

    {% if is_incremental() %}
        WHERE TRY_CAST(ingestion_timestamp AS TIMESTAMP) > (
            SELECT MAX(silver_loaded_at) FROM {{ this }}
        )
    {% endif %}

),

deduped_raw_gate_events AS (

    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY source_event_id, park_code
                ORDER BY TRY_CAST(ingestion_timestamp AS TIMESTAMP) DESC
            ) AS rn
        FROM bronze_raw_gate_events
    )
    WHERE rn = 1

),

cleaned_raw_gate_events AS (

    SELECT

        -- Identity
        g.source_event_id,
        g.source_system,
        g.park_code,
        g.source_region,
        g.gate_id,
        INITCAP(TRIM(g.gate_location))                              AS gate_location,
        UPPER(TRIM(g.zone_code))                                    AS zone_code,
        UPPER(TRIM(g.event_type))                                   AS event_type,

        -- Customer reference
        g.customer_id                                               AS source_customer_id,

        -- Timestamps
        TRY_CAST(g.event_datetime AS TIMESTAMP)                     AS event_datetime,

        -- Ticket
        UPPER(TRIM(g.ticket_barcode))                               AS ticket_barcode,

        -- Rejection reason: '-' means null
        CASE
            WHEN TRIM(g.rejection_reason) = '-' THEN NULL
            WHEN TRIM(g.rejection_reason) = ''  THEN NULL
            ELSE TRIM(g.rejection_reason)
        END                                                         AS rejection_reason,

        -- ✅ customer_sk via JOIN — never rehash
        c.customer_sk                                               AS customer_sk,

        -- Source timestamp
        TRY_CAST(g.ingestion_timestamp AS TIMESTAMP)                AS source_ingested_at

    FROM deduped_raw_gate_events                    AS g
    LEFT JOIN {{ ref('silver_customers') }}         AS c
        ON  g.customer_id   = c.source_customer_id

),

silver_gate_events AS (

    SELECT

        -- Surrogate key (silver PK)
        REGEXP_REPLACE(
            {{ dbt_utils.generate_surrogate_key(
                ['source_event_id']
            ) }},
            '^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$',
            '$1-$2-$3-$4-$5'
        ) AS gate_event_sk,

        -- Customer FK (from JOIN)
        REGEXP_REPLACE(
            {{ dbt_utils.generate_surrogate_key(
                ['source_customer_id']
            ) }},
            '^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$',
            '$1-$2-$3-$4-$5'
        ) AS customer_sk,

        -- Identity
        source_event_id,
        source_customer_id,
        source_system,
        park_code,
        source_region,
        gate_id,
        gate_location,
        zone_code,

        -- Event
        event_type,
        event_datetime,

        -- Derived from event_datetime
        CAST(event_datetime AS DATE)                                AS event_date,
        DATE_FORMAT(event_datetime, 'EEEE')                         AS event_day_of_week,
        HOUR(event_datetime)                                        AS event_hour,

        -- Derived event classification
        CASE
            WHEN HOUR(event_datetime) BETWEEN 6  AND 11 THEN 'Morning'
            WHEN HOUR(event_datetime) BETWEEN 12 AND 16 THEN 'Afternoon'
            WHEN HOUR(event_datetime) BETWEEN 17 AND 20 THEN 'Evening'
            ELSE 'Off-Hours'
        END                                                         AS time_of_day,

        -- Is entry or exit flag
        CASE
            WHEN event_type = 'ENTRY' THEN TRUE
            WHEN event_type = 'EXIT'  THEN FALSE
            ELSE NULL
        END                                                         AS is_entry,

        -- Ticket
        ticket_barcode,

        -- Rejection
        rejection_reason,
        CASE
            WHEN rejection_reason IS NULL THEN FALSE
            ELSE TRUE
        END                                                         AS is_rejected,

        -- Lineage
        source_ingested_at,

        -- Silver audit
        CURRENT_TIMESTAMP()                                         AS silver_loaded_at

    FROM cleaned_raw_gate_events

)

SELECT * FROM silver_gate_events
WHERE LEFT(source_customer_id,2) = 'AQ' AND RIGHT(source_customer_id,7) = '0000017'
