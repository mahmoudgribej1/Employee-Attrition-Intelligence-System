{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'star_schema', 'dimension']
    )
}}

/*
    Dimension: Business Travel
    Grain: 1 row per travel frequency level
    Source: silver_ibm_hr_cleaned
*/

WITH travel AS (
    SELECT DISTINCT
        business_travel,
        business_travel_encoded
    FROM {{ ref('silver_ibm_hr_cleaned') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY business_travel_encoded) AS business_travel_key,
    business_travel AS business_travel_name,
    business_travel_encoded AS travel_frequency_ordinal
FROM travel
