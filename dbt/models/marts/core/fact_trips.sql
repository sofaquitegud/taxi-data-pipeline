-- Fact table for taxi trips

{{ config(
    materialized='incremental',
    schema='core',
    unique_key='trip_id',
    on_schema_change='sync_all_columns'
) }}

WITH yellow_trips AS (
    SELECT * FROM {{ ref('stg_yellow_taxi') }}
),

green_trips AS (
    SELECT * FROM {{ ref('stg_green_taxi') }}
),

all_trips AS (
    SELECT * FROM yellow_trips
    UNION ALL
    SELECT * FROM green_trips
),

enriched AS (
    SELECT
        -- Keys
        t.trip_id,
        t.pickup_location_id,
        t.dropoff_location_id,
        t.payment_type_id,
        t.rate_code_id,
        
        -- Timestamps
        t.pickup_datetime,
        t.dropoff_datetime,
        
        -- Trip metrics
        t.passenger_count,
        t.trip_distance_miles,
        
        -- Calculated metrics
        EXTRACT(EPOCH FROM (t.dropoff_datetime - t.pickup_datetime)) / 60 AS trip_duration_minutes,
        
        CASE 
            WHEN EXTRACT(EPOCH FROM (t.dropoff_datetime - t.pickup_datetime)) > 0 
            THEN t.trip_distance_miles / (EXTRACT(EPOCH FROM (t.dropoff_datetime - t.pickup_datetime)) / 3600)
            ELSE 0 
        END AS avg_speed_mph,
        
        -- Fare breakdown
        t.fare_amount,
        t.extra_charges,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.total_amount,
        
        -- Tip percentage
        CASE 
            WHEN t.fare_amount > 0 
            THEN (t.tip_amount / t.fare_amount) * 100 
            ELSE 0 
        END AS tip_percentage,
        
        -- Metadata
        t.taxi_type,
        t.loaded_at
        
    FROM all_trips t
)

SELECT * FROM enriched

{% if is_incremental() %}
WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
{% endif %}
