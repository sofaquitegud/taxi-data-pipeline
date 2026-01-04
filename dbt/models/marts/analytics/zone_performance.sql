 -- Zone performance metrics

{{ config(
    materialized='table',
    schema='analytics'
) }}

WITH trips AS (
    SELECT * FROM {{ ref('fact_trips') }}
),

zone_metrics AS (
    SELECT
        pickup_location_id AS location_id,
        taxi_type,
        
        -- Pickup metrics
        COUNT(*) AS pickup_count,
        SUM(total_amount) AS pickup_revenue,
        AVG(total_amount) AS avg_pickup_fare,
        AVG(trip_distance_miles) AS avg_trip_distance,
        AVG(tip_percentage) AS avg_tip_percentage
        
    FROM trips
    GROUP BY pickup_location_id, taxi_type
),

dropoff_metrics AS (
    SELECT
        dropoff_location_id AS location_id,
        taxi_type,
        COUNT(*) AS dropoff_count
    FROM trips
    GROUP BY dropoff_location_id, taxi_type
),

combined AS (
    SELECT
        COALESCE(z.location_id, d.location_id) AS location_id,
        COALESCE(z.taxi_type, d.taxi_type) AS taxi_type,
        COALESCE(z.pickup_count, 0) AS pickup_count,
        COALESCE(d.dropoff_count, 0) AS dropoff_count,
        COALESCE(z.pickup_revenue, 0) AS total_revenue,
        z.avg_pickup_fare,
        z.avg_trip_distance,
        z.avg_tip_percentage,
        
        -- Net flow (positive = more pickups, negative = more dropoffs)
        COALESCE(z.pickup_count, 0) - COALESCE(d.dropoff_count, 0) AS net_flow
        
    FROM zone_metrics z
    FULL OUTER JOIN dropoff_metrics d 
        ON z.location_id = d.location_id 
        AND z.taxi_type = d.taxi_type
)

SELECT * FROM combined
