{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'star_schema', 'dimension']
    )
}}

/*
    Dimension: Job Role
    Grain: 1 row per unique job role
    Source: silver_ibm_hr_cleaned
*/

WITH job_roles AS (
    SELECT DISTINCT
        job_role,
        department
    FROM {{ ref('silver_ibm_hr_cleaned') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY department, job_role) AS job_role_key,
    job_role AS job_role_name,
    department AS department_name
FROM job_roles
