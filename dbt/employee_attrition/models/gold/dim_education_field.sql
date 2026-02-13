{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'star_schema', 'dimension']
    )
}}

/*
    Dimension: Education Field
    Grain: 1 row per unique education field
    Includes education level labels (1=Below College ... 5=Doctor)
    Source: silver_ibm_hr_cleaned
*/

WITH education_fields AS (
    SELECT DISTINCT education_field
    FROM {{ ref('silver_ibm_hr_cleaned') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY education_field) AS education_field_key,
    education_field AS education_field_name
FROM education_fields
