{{
    config(
        materialized='table',
        file_format='delta',
        partition_by=['hire_year'],
        tags=['silver', 'employee', 'cleaned']
    )
}}

/*
================================================================================
SILVER EMPLOYEE CLEANED
================================================================================
Purpose: Clean and validate employee data from bronze layer
Issues Fixed:
  - 991 status mismatches (Active employees with ExitDate)
  - Date string to DATE conversion
  - Proper is_active flag based on ExitDate (not EmployeeStatus)
  - Missing TerminationDescription handling
  - Calculated tenure and age fields

Created: {{ run_started_at.strftime('%Y-%m-%d') }}
================================================================================
*/

WITH source_data AS (
    SELECT *
    FROM {{ source('bronze', 'employee_data') }}
),

cleaned_dates AS (
    SELECT
        -- Primary Keys
        EmpID,
        
        -- Personal Information
        FirstName,
        LastName,
        ADEmail,
        TO_DATE(DOB, 'dd-MMM-yy') AS date_of_birth,
        GenderCode,
        RaceDesc,
        MaritalDesc,
        State,
        
        -- Employment Dates - Convert strings to proper DATE type
        TO_DATE(StartDate, 'dd-MMM-yy') AS start_date,
        CASE 
            WHEN TRIM(ExitDate) = '' OR ExitDate IS NULL THEN NULL
            ELSE TO_DATE(ExitDate, 'dd-MMM-yy')
        END AS exit_date,
        
        -- Job Information
        Title,
        Supervisor,
        BusinessUnit,
        DepartmentType,
        Division,
        JobFunctionDescription,
        PayZone,
        EmployeeType,
        EmployeeClassificationType,
        
        -- Employment Status - Keep original for reference
        EmployeeStatus AS original_employee_status,
        
        -- Performance
        Performance_Score AS performance_score,
        Current_Employee_Rating AS current_employee_rating,
        
        -- Termination Information
        TerminationType,
        CASE 
            WHEN TRIM(TerminationDescription) = '' OR TerminationDescription IS NULL 
            THEN 'Not Applicable - Active Employee'
            ELSE TerminationDescription
        END AS termination_description,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS processed_at
        
    FROM source_data
),

calculated_fields AS (
    SELECT
        *,
        
        -- FIX: Create is_active flag based on ExitDate (NOT EmployeeStatus)
        -- This fixes the 991 status mismatch issue
        CASE 
            WHEN exit_date IS NULL THEN 1
            ELSE 0
        END AS is_active,
        
        -- Corrected employment status based on exit_date
        CASE 
            WHEN exit_date IS NULL THEN 'Active'
            ELSE 'Terminated'
        END AS corrected_employee_status,
        
        -- Calculate tenure in days
        DATEDIFF(
            COALESCE(exit_date, CURRENT_DATE()),
            start_date
        ) AS tenure_days,
        
        -- Calculate age
        FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), date_of_birth) / 12) AS age,
        
        -- Extract year and month from exit date for trend analysis
        YEAR(exit_date) AS exit_year,
        MONTH(exit_date) AS exit_month,
        
        -- Extract year from start date for cohort analysis
        YEAR(start_date) AS hire_year
        
    FROM cleaned_dates
),

final_transformations AS (
    SELECT
        *,
        
        -- Calculate tenure in months (using 30.44 days average per month)
        CAST(tenure_days / 30.44 AS INTEGER) AS tenure_months,
        
        -- Calculate tenure in years (with decimal precision)
        ROUND(tenure_days / 365.25, 1) AS tenure_years,
        
        -- Age group categorization for demographic analysis
        CASE 
            WHEN age < 25 THEN 'Under 25'
            WHEN age >= 25 AND age < 35 THEN '25-34'
            WHEN age >= 35 AND age < 45 THEN '35-44'
            WHEN age >= 45 AND age < 55 THEN '45-54'
            ELSE '55+'
        END AS age_group,
        
        -- Tenure category for analysis
        CASE 
            WHEN tenure_months <= 3 THEN '0-3 months (New Hire)'
            WHEN tenure_months <= 12 THEN '3-12 months (First Year)'
            WHEN tenure_months <= 24 THEN '1-2 years'
            WHEN tenure_months <= 60 THEN '2-5 years'
            ELSE '5+ years (Long Tenure)'
        END AS tenure_category,
        
        -- Termination type classification
        CASE 
            WHEN is_active = 1 THEN 'Not Applicable'
            WHEN LOWER(TerminationType) LIKE '%voluntary%' THEN 'Voluntary'
            WHEN LOWER(TerminationType) LIKE '%involuntary%' THEN 'Involuntary'
            ELSE 'Other'
        END AS termination_type_clean,
        
        -- Flag for status mismatch (for data quality monitoring)
        CASE 
            WHEN original_employee_status = 'Active' AND exit_date IS NOT NULL THEN 1
            ELSE 0
        END AS had_status_mismatch
        
    FROM calculated_fields
)

SELECT
    -- Primary Key
    EmpID,
    
    -- Personal Information
    FirstName,
    LastName,
    ADEmail,
    date_of_birth,
    age,
    age_group,
    GenderCode,
    RaceDesc,
    MaritalDesc,
    State,
    
    -- Employment Status (CORRECTED)
    is_active,
    corrected_employee_status AS employee_status,
    original_employee_status,  -- Keep for data quality tracking
    had_status_mismatch,        -- Flag to track the 991 records that were fixed
    
    -- Employment Dates
    start_date,
    exit_date,
    hire_year,
    exit_year,
    exit_month,
    
    -- Tenure Metrics
    tenure_days,
    tenure_months,
    tenure_years,
    tenure_category,
    
    -- Job Information
    Title,
    Supervisor,
    BusinessUnit,
    DepartmentType,
    Division,
    JobFunctionDescription,
    PayZone,
    EmployeeType,
    EmployeeClassificationType,
    
    -- Performance
    performance_score,
    current_employee_rating,
    
    -- Termination Information
    TerminationType,
    termination_type_clean,
    termination_description,
    
    -- Metadata
    processed_at,
    CURRENT_TIMESTAMP() AS last_updated
    
FROM final_transformations

-- Data Quality Check: Log count of status mismatches fixed
-- Run: SELECT COUNT(*) FROM silver.silver_employee_cleaned WHERE had_status_mismatch = 1
-- Expected: 991 records