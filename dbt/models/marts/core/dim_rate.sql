-- Rate code dimension table

{{ config(
    materialized='table',
    schema='core'
) }}

SELECT
    rate_code_id,
    rate_code,
    description
FROM (
    VALUES 
        (1, 'Standard rate', 'Standard metered fare'),
        (2, 'JFK', 'JFK Airport flat rate'),
        (3, 'Newark', 'Newark Airport negotiated fare'),
        (4, 'Nassau or Westchester', 'Nassau or Westchester fare'),
        (5, 'Negotiated fare', 'Negotiated flat fare'),
        (6, 'Group ride', 'Group ride fare')
) AS t(rate_code_id, rate_code, description)
