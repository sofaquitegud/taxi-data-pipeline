-- Location/Zone dimension table

{{ config(
    materialized='table',
    schema='core'
) }}

-- NYC Taxi zone lookup data (seed or static)
WITH zones AS (
    SELECT 
        location_id,
        zone_name,
        borough,
        service_zone
    FROM {{ ref('taxi_zones') }}
)

SELECT
    location_id,
    zone_name,
    borough,
    service_zone,
    
    -- Derived categories
    CASE 
        WHEN borough = 'Manhattan' THEN 'Core'
        WHEN borough IN ('Brooklyn', 'Queens') THEN 'Outer Borough'
        WHEN borough = 'Bronx' THEN 'North'
        WHEN borough = 'Staten Island' THEN 'South'
        ELSE 'Other'
    END AS borough_category

FROM zones
