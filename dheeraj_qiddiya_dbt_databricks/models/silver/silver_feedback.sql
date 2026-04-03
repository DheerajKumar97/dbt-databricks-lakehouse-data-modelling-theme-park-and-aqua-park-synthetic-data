{{
    config(
        materialized     = 'incremental',
        unique_key       = 'feedback_sk',
        schema           = 'silver',
        tags             = ['silver', 'feedback', 'daily'],
        on_schema_change = 'sync_all_columns'
    )
}}

WITH bronze_raw_feedback AS (

    SELECT *
    FROM {{ ref('raw_feedback') }}

    {% if is_incremental() %}
        WHERE TRY_CAST(ingestion_timestamp AS TIMESTAMP) > (
            SELECT MAX(silver_loaded_at) FROM {{ this }}
        )
    {% endif %}

),

deduped_raw_feedback AS (

    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY source_feedback_id, park_code
                ORDER BY TRY_CAST(ingestion_timestamp AS TIMESTAMP) DESC
            ) AS rn
        FROM bronze_raw_feedback
    )
    WHERE rn = 1

),

cleaned_raw_feedback AS (

    SELECT

        -- Identity
        source_feedback_id,
        park_code,
        source_region,

        -- Customer reference (FK to silver_customers)
        customer_id                                                 AS source_customer_id,

        -- Feedback date
        TRY_CAST(feedback_date AS DATE)                             AS feedback_date,

        -- Scores (validate range 0-10)
        CASE
            WHEN CAST(overall_score AS INT) BETWEEN 0 AND 10
            THEN CAST(overall_score AS INT)
            ELSE NULL
        END                                                         AS overall_score,

        CASE
            WHEN CAST(nps_raw AS INT) BETWEEN 0 AND 10
            THEN CAST(nps_raw AS INT)
            ELSE NULL
        END                                                         AS nps_raw,

        CASE
            WHEN CAST(cleanliness_score AS INT) BETWEEN 0 AND 10
            THEN CAST(cleanliness_score AS INT)
            ELSE NULL
        END                                                         AS cleanliness_score,

        CASE
            WHEN CAST(staff_score AS INT) BETWEEN 0 AND 10
            THEN CAST(staff_score AS INT)
            ELSE NULL
        END                                                         AS staff_score,

        -- Boolean: has_complaint → 'true'/'false'
        CASE
            WHEN LOWER(TRIM(has_complaint)) = 'true'  THEN TRUE
            WHEN LOWER(TRIM(has_complaint)) = 'false' THEN FALSE
            ELSE NULL
        END                                                         AS has_complaint,

        -- Complaint text: '-' means null in your data
        CASE
            WHEN TRIM(complaint_text) = '-' THEN NULL
            WHEN TRIM(complaint_text) = ''  THEN NULL
            ELSE TRIM(complaint_text)
        END                                                         AS complaint_text,

        -- Source timestamp
        TRY_CAST(ingestion_timestamp AS TIMESTAMP)                  AS source_ingested_at

    FROM deduped_raw_feedback

),

silver_feedback AS (

    SELECT

        -- Surrogate key (silver PK)
            REGEXP_REPLACE(
                CAST({{ dbt_utils.generate_surrogate_key(['source_feedback_id']) }} AS STRING),
                '^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$',
                '$1-$2-$3-$4-$5'
            ) AS feedback_sk,

        -- Customer surrogate key (FK → silver_customers)
            REGEXP_REPLACE(
                CAST({{ dbt_utils.generate_surrogate_key(['source_customer_id']) }} AS STRING),
                '^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$',
                '$1-$2-$3-$4-$5'
            ) AS customer_sk,

        -- Identity
        source_feedback_id,
        park_code,
        source_region,
        source_customer_id,

        -- Feedback date
        feedback_date,

        -- Scores
        overall_score,
        nps_raw,
        cleanliness_score,
        staff_score,

        -- Derived score columns
        ROUND(
            (overall_score + cleanliness_score + staff_score) / 3.0,
            2
        )                                                           AS avg_score,

        -- NPS Category
        CASE
            WHEN nps_raw BETWEEN 9 AND 10 THEN 'Promoter'
            WHEN nps_raw BETWEEN 7 AND 8  THEN 'Passive'
            WHEN nps_raw BETWEEN 0 AND 6  THEN 'Detractor'
            ELSE NULL
        END                                                         AS nps_category,

        -- Overall score category
        CASE
            WHEN overall_score BETWEEN 9 AND 10 THEN 'Excellent'
            WHEN overall_score BETWEEN 7 AND 8  THEN 'Good'
            WHEN overall_score BETWEEN 5 AND 6  THEN 'Average'
            WHEN overall_score BETWEEN 0 AND 4  THEN 'Poor'
            ELSE NULL
        END                                                         AS overall_score_category,

        -- Complaint fields
        has_complaint,
        complaint_text,

        -- Lineage
        source_ingested_at,

        -- Silver audit
        CURRENT_TIMESTAMP()                                         AS silver_loaded_at

    FROM cleaned_raw_feedback

)

SELECT * FROM silver_feedback