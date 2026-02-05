{{
    config(
        materialized='table',
        file_format='delta',
        tags=['gold', 'star_schema', 'dimension']
    )
}}

/*
Dimension: Employee
Grain: One row per employee
Type: Type 1 SCD (current state only)
*/

SELECT
    -- Surrogate Key
    EmpID AS employee_key,
    
    -- Natural Key
    EmpID AS employee_id,
    
    -- Attributes
    FirstName AS first_name,
    LastName AS last_name,
    CONCAT(FirstName, ' ', LastName) AS full_name,
    ADEmail AS email,
    
    -- Demographics
    age,
    age_group,
    GenderCode AS gender,
    RaceDesc AS race,
    MaritalDesc AS marital_status,
    
    -- Job Info (current)
    Title AS job_title,
    EmployeeType AS employee_type,
    PayZone AS pay_zone,
    
    -- Location
    State AS work_state,
    
    -- Status
    is_active,
    employee_status,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS last_updated

FROM {{ ref('silver_employee_cleaned') }}