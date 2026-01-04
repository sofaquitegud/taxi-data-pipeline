-- Payment type dimension table

{{ config(
    materialized='table',
    schema='core'
) }}

SELECT
    payment_type_id,
    payment_type,
    description
FROM (
    VALUES 
        (1, 'Credit card', 'Credit card payment'),
        (2, 'Cash', 'Cash payment'),
        (3, 'No charge', 'No charge for trip'),
        (4, 'Dispute', 'Payment disputed'),
        (5, 'Unknown', 'Unknown payment type'),
        (6, 'Voided trip', 'Trip was voided')
) AS t(payment_type_id, payment_type, description)
