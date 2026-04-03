WITH raw_ride_operations AS (
    
    SELECT 
        *,
        'SF' AS source_region
    FROM {{ source('landing_zone', 'sf_raw_ride_operations') }}

    UNION ALL

    SELECT 
        *,
        'AQ' AS source_region
    FROM {{ source('landing_zone', 'aq_raw_ride_operations') }}

)

SELECT *
FROM raw_ride_operations