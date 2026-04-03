WITH raw_staff AS (
    
    SELECT 
        *,
        'SF' AS source_region
    FROM {{ source('landing_zone', 'sf_raw_staff') }}

    UNION ALL

    SELECT 
        *,
        'AQ' AS source_region
    FROM {{ source('landing_zone', 'aq_raw_staff') }}

)

SELECT *
FROM raw_staff