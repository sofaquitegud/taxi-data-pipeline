-- Hourly demand patterns

{{ config(
    materialized='table',
    schema='analytics'
) }}

WITH trips AS (
    SELECT * FROM {{ ref('fact_trips') }}
),

hourly_patterns AS (
    SELECT
        EXTRACT(HOUR FROM pickup_datetime)::INTEGER AS hour_of_day,
        EXTRACT(DOW FROM pickup_datetime)::INTEGER AS day_of_week,
        taxi_type,
        
        -- Volume
        COUNT(*) AS trip_count,
        SUM(passenger_count) AS total_passengers,
        
        -- Averages
        AVG(trip_distance_miles) AS avg_distance,
        AVG(trip_duration_minutes) AS avg_duration,
        AVG(total_amount) AS avg_fare,
        AVG(tip_percentage) AS avg_tip_pct,
        
        -- Revenue
        SUM(total_amount) AS total_revenue
        
    FROM trips
    GROUP BY 
        EXTRACT(HOUR FROM pickup_datetime),
        EXTRACT(DOW FROM pickup_datetime),
        taxi_type
),

with_labels AS (
    SELECT
        *,
        
        -- Hour labels
        CASE hour_of_day
            WHEN 0 THEN '12 AM'
            WHEN 12 THEN '12 PM'
            WHEN 1 THEN '1 AM'
            WHEN 13 THEN '1 PM'
            ELSE 
                CASE WHEN hour_of_day < 12 
                    THEN hour_of_day || ' AM' 
                    ELSE (hour_of_day - 12) || ' PM' 
                END
        END AS hour_label,
        
        -- Day labels
        CASE day_of_week
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_label,
        
        -- Weekend flag
        CASE WHEN day_of_week IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
        
    FROM hourly_patterns
)

SELECT * FROM with_labels
