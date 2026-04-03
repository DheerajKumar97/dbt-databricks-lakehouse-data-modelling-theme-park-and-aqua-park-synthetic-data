WITH raw_weather AS (
    
    SELECT 
        *,
        'SF' AS source_region
    FROM {{ source('landing_zone', 'sf_raw_weather') }}

    UNION ALL

    SELECT 
        *,
        'AQ' AS source_region
    FROM {{ source('landing_zone', 'aq_raw_weather') }}

)

SELECT * 
FROM raw_weather