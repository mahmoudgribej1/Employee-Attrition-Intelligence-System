{{
    config(
        materialized='table',
        file_format='delta',
        partition_by=['training_year'],
        tags=['silver', 'training', 'cleaned']
    )
}}

/*
================================================================================
SILVER TRAINING CLEANED
================================================================================
Purpose: Clean and validate training & development data
Issues Fixed:
  - Standardize training outcomes
  - Validate training cost (no negatives)
  - Validate training duration (no negatives, reasonable max)
  - Convert date strings to proper DATE type
  - Link to employee data for context

Created: {{ run_started_at.strftime('%Y-%m-%d') }}
================================================================================
*/

WITH source_data AS (
    SELECT
        `Employee_ID` AS employee_id,
        `Training_Date` AS training_date_raw,
        `Training_Program_Name` AS training_program_name,
        `Training_Type` AS training_type,
        `Training_Outcome` AS training_outcome,
        Location AS training_location,
        Trainer,
        `Training_Duration_Days_` AS training_duration_days,
        `Training_Cost` AS training_cost
    FROM {{ source('bronze', 'training_and_development_data') }}
),

cleaned_dates AS (
    SELECT
        employee_id,
        
        -- Convert training date to proper DATE type
        TO_DATE(training_date_raw, 'dd-MMM-yy') AS training_date,
        YEAR(TO_DATE(training_date_raw, 'dd-MMM-yy')) AS training_year,
        MONTH(TO_DATE(training_date_raw, 'dd-MMM-yy')) AS training_month,
        QUARTER(TO_DATE(training_date_raw, 'dd-MMM-yy')) AS training_quarter,
        
        -- Training details - cleaned
        TRIM(training_program_name) AS training_program_name,
        TRIM(training_type) AS training_type,
        TRIM(training_outcome) AS training_outcome,
        TRIM(training_location) AS training_location,
        TRIM(Trainer) AS trainer,
        
        training_duration_days,
        training_cost,
        
        CURRENT_TIMESTAMP() AS processed_at
        
    FROM source_data
),

validated_data AS (
    SELECT
        *,
        
        -- VALIDATE: Training duration (no negatives, max 90 days)
        CASE 
            WHEN training_duration_days < 0 THEN 0
            WHEN training_duration_days > 90 THEN 90  -- Cap at 90 days (3 months)
            ELSE training_duration_days
        END AS training_duration_days_validated,
        
        -- VALIDATE: Training cost (no negatives, reasonable max)
        CASE 
            WHEN training_cost < 0 THEN NULL
            WHEN training_cost > 50000 THEN NULL  -- Flag unrealistic costs
            ELSE training_cost
        END AS training_cost_validated,
        
        -- STANDARDIZE: Training outcome (title case, common values)
        CASE 
            WHEN LOWER(training_outcome) LIKE '%complet%' THEN 'Completed'
            WHEN LOWER(training_outcome) LIKE '%pass%' THEN 'Completed'
            WHEN LOWER(training_outcome) LIKE '%fail%' THEN 'Failed'
            WHEN LOWER(training_outcome) LIKE '%progress%' THEN 'In Progress'
            WHEN LOWER(training_outcome) LIKE '%withdraw%' THEN 'Withdrawn'
            ELSE INITCAP(TRIM(training_outcome))
        END AS training_outcome_standardized,
        
        -- Data quality flags
        CASE WHEN training_duration_days < 0 OR training_duration_days > 90 THEN 1 ELSE 0 END AS had_invalid_duration,
        CASE WHEN training_cost < 0 OR training_cost > 50000 THEN 1 ELSE 0 END AS had_invalid_cost
        
    FROM cleaned_dates
),

employee_context AS (
    SELECT
        EmpID,
        is_active,
        employee_status,
        DepartmentType,
        BusinessUnit,
        Title,
        performance_score,
        tenure_years
    FROM {{ ref('silver_employee_cleaned') }}
),

enriched_training AS (
    SELECT
        -- Training Information
        t.employee_id,
        t.training_date,
        t.training_year,
        t.training_month,
        t.training_quarter,
        t.training_program_name,
        t.training_type,
        t.training_outcome_standardized AS training_outcome,
        t.training_location,
        t.trainer,
        
        -- Validated metrics
        t.training_duration_days_validated AS training_duration_days,
        t.training_cost_validated AS training_cost,
        
        -- Employee Context
        e.is_active AS employee_is_active,
        e.employee_status,
        e.DepartmentType,
        e.BusinessUnit,
        e.Title,
        e.performance_score,
        e.tenure_years,
        
        -- Data Quality
        t.had_invalid_duration,
        t.had_invalid_cost,
        
        -- Metadata
        t.processed_at
        
    FROM validated_data t
    LEFT JOIN employee_context e
        ON t.employee_id = e.EmpID
),

final_transformations AS (
    SELECT
        *,
        
        -- Training duration category
        CASE 
            WHEN training_duration_days = 0 THEN 'Same Day'
            WHEN training_duration_days = 1 THEN '1 Day'
            WHEN training_duration_days <= 3 THEN '2-3 Days'
            WHEN training_duration_days <= 5 THEN '4-5 Days'
            WHEN training_duration_days <= 10 THEN '1-2 Weeks'
            ELSE '2+ Weeks'
        END AS training_duration_category,
        
        -- Training cost category
        CASE 
            WHEN training_cost < 100 THEN 'Low Cost (<$100)'
            WHEN training_cost < 500 THEN 'Medium Cost ($100-$500)'
            WHEN training_cost < 1000 THEN 'High Cost ($500-$1K)'
            ELSE 'Very High Cost ($1K+)'
        END AS training_cost_category,
        
        -- Training type standardized grouping
        CASE 
            WHEN LOWER(training_type) LIKE '%online%' OR LOWER(training_type) LIKE '%virtual%' THEN 'Online/Virtual'
            WHEN LOWER(training_type) LIKE '%classroom%' OR LOWER(training_type) LIKE '%person%' THEN 'In-Person/Classroom'
            WHEN LOWER(training_type) LIKE '%workshop%' THEN 'Workshop'
            WHEN LOWER(training_type) LIKE '%certif%' THEN 'Certification'
            ELSE training_type
        END AS training_type_category,
        
        -- Success flag
        CASE 
            WHEN training_outcome = 'Completed' THEN 1
            ELSE 0
        END AS training_completed_flag,
        
        -- Calculate cost per day
        CASE 
            WHEN training_duration_days > 0 THEN ROUND(training_cost / training_duration_days, 2)
            ELSE training_cost
        END AS cost_per_training_day,
        
        CURRENT_TIMESTAMP() AS last_updated
        
    FROM enriched_training
)

SELECT
    -- Training Keys
    employee_id,
    training_date,
    training_year,
    training_month,
    training_quarter,
    
    -- Training Details
    training_program_name,
    training_type,
    training_type_category,
    training_outcome,
    training_completed_flag,
    training_location,
    trainer,
    
    -- Training Metrics (VALIDATED)
    training_duration_days,
    training_duration_category,
    training_cost,
    training_cost_category,
    cost_per_training_day,
    
    -- Employee Context
    employee_is_active,
    employee_status,
    DepartmentType,
    BusinessUnit,
    Title,
    performance_score,
    tenure_years,
    
    -- Data Quality Flags
    had_invalid_duration,
    had_invalid_cost,
    
    -- Metadata
    processed_at,
    last_updated
    
FROM final_transformations

-- Data Quality Checks:
-- 1. Invalid duration: SELECT COUNT(*) FROM silver.silver_training_cleaned WHERE had_invalid_duration = 1
-- 2. Invalid cost: SELECT COUNT(*) FROM silver.silver_cleaned WHERE had_invalid_cost = 1
-- 3. Completion rate: SELECT training_outcome, COUNT(*) FROM silver.silver_training_cleaned GROUP BY training_outcome
-- 4. Total investment validation: SELECT SUM(training_cost) FROM silver.silver_training_cleaned
--    Expected: ~$1,675,886.09 (from EDA)