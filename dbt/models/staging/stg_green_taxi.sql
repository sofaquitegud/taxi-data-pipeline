-- Staging model for green taxi trips

{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source AS (
    SELECT * FROM {{ source('taxi_raw', 'green_taxi_trips') }}
),

renamed AS (
    SELECT
        -- IDs
        {{ dbt_utils.generate_surrogate_key(['pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID']) }} AS trip_id,
        
        -- Timestamps
        pickup_datetime,
        dropoff_datetime,
        
        -- Location IDs
        CAST(PULocationID AS INTEGER) AS pickup_location_id,
        CAST(DOLocationID AS INTEGER) AS dropoff_location_id,
        
        -- Trip details
        CAST(passenger_count AS INTEGER) AS passenger_count,
        CAST(trip_distance AS NUMERIC(10,2)) AS trip_distance_miles,
        
        -- Fares
        CAST(fare_amount AS NUMERIC(10,2)) AS fare_amount,
        CAST(extra AS NUMERIC(10,2)) AS extra_charges,
        CAST(mta_tax AS NUMERIC(10,2)) AS mta_tax,
        CAST(tip_amount AS NUMERIC(10,2)) AS tip_amount,
        CAST(tolls_amount AS NUMERIC(10,2)) AS tolls_amount,
        CAST(total_amount AS NUMERIC(10,2)) AS total_amount,
        
        -- Categoricals
        CAST(payment_type AS INTEGER) AS payment_type_id,
        CAST(RatecodeID AS INTEGER) AS rate_code_id,
        
        -- Metadata
        'green' AS taxi_type,
        CURRENT_TIMESTAMP AS loaded_at
        
    FROM source
    WHERE pickup_datetime IS NOT NULL
      AND dropoff_datetime IS NOT NULL
      AND dropoff_datetime > pickup_datetime
)

SELECT * FROM renamed
