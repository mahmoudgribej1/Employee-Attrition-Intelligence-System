{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'star_schema', 'dimension']
    )
}}

/*
    Dimension: Marital Status
    Grain: 1 row per unique marital status
    Source: silver_ibm_hr_cleaned
*/

WITH statuses AS (
    SELECT DISTINCT marital_status
    FROM {{ ref('silver_ibm_hr_cleaned') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY marital_status) AS marital_status_key,
    marital_status AS marital_status_name
FROM statuses
