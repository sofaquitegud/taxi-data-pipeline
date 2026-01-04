-- Date/Time dimension table

{{ config(
    materialized='table',
    schema='core'
) }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="hour",
        start_date="cast('2023-01-01' as date)",
        end_date="cast('2024-01-01' as date)"
    ) }}
),

enriched AS (
    SELECT
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['date_hour']) }} AS datetime_id,
        
        -- Date components
        date_hour AS full_datetime,
        DATE(date_hour) AS date_day,
        EXTRACT(YEAR FROM date_hour) AS year,
        EXTRACT(MONTH FROM date_hour) AS month,
        EXTRACT(DAY FROM date_hour) AS day,
        EXTRACT(HOUR FROM date_hour) AS hour,
        EXTRACT(DOW FROM date_hour) AS day_of_week,
        EXTRACT(WEEK FROM date_hour) AS week_of_year,
        EXTRACT(QUARTER FROM date_hour) AS quarter,
        
        -- Derived flags
        CASE 
            WHEN EXTRACT(DOW FROM date_hour) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        
        -- Time of day
        CASE
            WHEN EXTRACT(HOUR FROM date_hour) BETWEEN 6 AND 11 THEN 'morning'
            WHEN EXTRACT(HOUR FROM date_hour) BETWEEN 12 AND 17 THEN 'afternoon'
            WHEN EXTRACT(HOUR FROM date_hour) BETWEEN 18 AND 21 THEN 'evening'
            ELSE 'night'
        END AS time_of_day,
        
        -- Month name
        TO_CHAR(date_hour, 'Month') AS month_name,
        
        -- Day name
        TO_CHAR(date_hour, 'Day') AS day_name
        
    FROM date_spine
)

SELECT * FROM enriched
