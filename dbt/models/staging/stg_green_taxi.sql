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
        -- IDs (generate simple surrogate key)
        md5(
            coalesce(cast(pickup_datetime as text), '') || '-' ||
            coalesce(cast(dropoff_datetime as text), '') || '-' ||
            coalesce(cast("PULocationID" as text), '') || '-' ||
            coalesce(cast("DOLocationID" as text), '')
        ) AS trip_id,
        
        -- Timestamps
        pickup_datetime,
        dropoff_datetime,
        
        -- Location IDs
        "PULocationID"::INTEGER AS pickup_location_id,
        "DOLocationID"::INTEGER AS dropoff_location_id,
        
        -- Trip details
        passenger_count::INTEGER AS passenger_count,
        trip_distance::NUMERIC(10,2) AS trip_distance_miles,
        
        -- Fares
        fare_amount::NUMERIC(10,2) AS fare_amount,
        extra::NUMERIC(10,2) AS extra_charges,
        mta_tax::NUMERIC(10,2) AS mta_tax,
        tip_amount::NUMERIC(10,2) AS tip_amount,
        tolls_amount::NUMERIC(10,2) AS tolls_amount,
        total_amount::NUMERIC(10,2) AS total_amount,
        
        -- Categoricals
        payment_type::INTEGER AS payment_type_id,
        "RatecodeID"::INTEGER AS rate_code_id,
        
        -- Metadata
        taxi_type,
        CURRENT_TIMESTAMP AS loaded_at
        
    FROM source
    WHERE pickup_datetime IS NOT NULL
      AND dropoff_datetime IS NOT NULL
      AND dropoff_datetime > pickup_datetime
)

SELECT * FROM renamed
