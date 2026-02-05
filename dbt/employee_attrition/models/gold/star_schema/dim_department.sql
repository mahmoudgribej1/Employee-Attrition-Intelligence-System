{{
    config(
        materialized='table',
        file_format='delta',
        tags=['gold', 'star_schema', 'dimension']
    )
}}

/*
Dimension: Department
Grain: One row per unique department
Type: Type 1 SCD
*/

WITH department_hierarchy AS (
    SELECT DISTINCT
        DepartmentType,
        BusinessUnit,
        Division
    FROM {{ ref('silver_employee_cleaned') }}
    WHERE DepartmentType IS NOT NULL
)

SELECT
    -- Surrogate Key
    ROW_NUMBER() OVER (ORDER BY DepartmentType) AS department_key,
    
    -- Attributes
    DepartmentType AS department_name,
    BusinessUnit AS business_unit,
    Division AS division,
    
    -- Hierarchy levels
    COALESCE(Division, 'Unknown') AS level_1_division,
    COALESCE(BusinessUnit, 'Unknown') AS level_2_business_unit,
    COALESCE(DepartmentType, 'Unknown') AS level_3_department,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS last_updated

FROM department_hierarchy