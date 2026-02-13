{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'star_schema', 'dimension']
    )
}}

/*
    Dimension: Department
    Grain: 1 row per unique department
    Source: silver_ibm_hr_cleaned
*/

WITH departments AS (
    SELECT DISTINCT department
    FROM {{ ref('silver_ibm_hr_cleaned') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY department) AS department_key,
    department AS department_name
FROM departments
