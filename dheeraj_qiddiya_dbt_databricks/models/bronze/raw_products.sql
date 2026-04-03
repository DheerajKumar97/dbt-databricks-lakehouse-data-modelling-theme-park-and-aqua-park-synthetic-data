WITH raw_products AS (
    
    SELECT 
        *,
        'SF' AS source_region
    FROM {{ source('landing_zone', 'sf_raw_products') }}

    UNION ALL

    SELECT 
        *,
        'AQ' AS source_region
    FROM {{ source('landing_zone', 'aq_raw_products') }}

)

SELECT *
FROM raw_products