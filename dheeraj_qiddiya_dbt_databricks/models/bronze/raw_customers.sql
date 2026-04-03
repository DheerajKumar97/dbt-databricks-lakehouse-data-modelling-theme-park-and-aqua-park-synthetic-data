WITH raw_customers AS (
    
    SELECT 
        *,
        'SF' AS source_region
    FROM {{ source('landing_zone', 'sf_raw_customers') }}

    UNION ALL

    SELECT 
        *,
        'AQ' AS source_region
    FROM {{ source('landing_zone', 'aq_raw_customers') }}

)

SELECT * 
FROM raw_customers