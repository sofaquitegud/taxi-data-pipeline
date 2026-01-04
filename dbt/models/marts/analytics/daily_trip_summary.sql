-- Daily trip summary aggregation

{{ config(
    materialized='table',
    schema='analytics'
) }}

WITH trips AS (
    SELECT * FROM {{ ref('fact_trips') }}
),

daily_agg AS (
    SELECT
        DATE(pickup_datetime) AS trip_date,
        taxi_type,
        pickup_location_id,
        
        -- Volume metrics
        COUNT(*) AS total_trips,
        SUM(passenger_count) AS total_passengers,
        
        -- Distance metrics
        SUM(trip_distance_miles) AS total_distance,
        AVG(trip_distance_miles) AS avg_distance,
        
        -- Duration metrics
        AVG(trip_duration_minutes) AS avg_duration_minutes,
        
        -- Revenue metrics
        SUM(fare_amount) AS total_fare_amount,
        SUM(tip_amount) AS total_tips,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_fare,
        
        -- Tip metrics
        AVG(tip_percentage) AS avg_tip_percentage,
        
        -- Speed metrics
        AVG(avg_speed_mph) AS avg_speed
        
    FROM trips
    GROUP BY 
        DATE(pickup_datetime),
        taxi_type,
        pickup_location_id
)

SELECT * FROM daily_agg
