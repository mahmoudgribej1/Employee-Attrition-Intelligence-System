{{
    config(
        materialized='table',
        file_format='delta',
        partition_by=['survey_year'],
        tags=['silver', 'engagement', 'cleaned']
    )
}}

/*
================================================================================
SILVER ENGAGEMENT CLEANED
================================================================================
Purpose: Clean and validate engagement survey data
Issues Fixed:
  - Validate score ranges (should be 1-5)
  - Join with employee data for context
  - Flag low engagement (94% at-risk identified in EDA)
  - Add department and employment status context

Created: {{ run_started_at.strftime('%Y-%m-%d') }}
================================================================================
*/

WITH source_engagement AS (
    SELECT
        `Employee_ID` AS employee_id,
        TO_DATE(`Survey_Date`, 'yyyy-MM-dd') AS survey_date,
        `Engagement_Score` AS engagement_score,
        `Satisfaction_Score` AS satisfaction_score,
        `Work-Life_Balance_Score` AS worklife_balance_score
    FROM {{ source('bronze', 'employee_engagement_survey_data') }}
),

validated_scores AS (
    SELECT
        employee_id,
        survey_date,
        
        -- Validate engagement score (should be 1-5)
        CASE 
            WHEN engagement_score < 1 THEN NULL
            WHEN engagement_score > 5 THEN NULL
            ELSE engagement_score
        END AS engagement_score,
        
        -- Validate satisfaction score
        CASE 
            WHEN satisfaction_score < 1 THEN NULL
            WHEN satisfaction_score > 5 THEN NULL
            ELSE satisfaction_score
        END AS satisfaction_score,
        
        -- Validate work-life balance score
        CASE 
            WHEN worklife_balance_score < 1 THEN NULL
            WHEN worklife_balance_score > 5 THEN NULL
            ELSE worklife_balance_score
        END AS worklife_balance_score,
        
        -- Track if any scores were invalid
        CASE 
            WHEN engagement_score < 1 OR engagement_score > 5 
              OR satisfaction_score < 1 OR satisfaction_score > 5
              OR worklife_balance_score < 1 OR worklife_balance_score > 5
            THEN 1
            ELSE 0
        END AS had_invalid_scores
        
    FROM source_engagement
),

employee_context AS (
    SELECT
        EmpID,
        is_active,
        employee_status,
        DepartmentType,
        BusinessUnit,
        Title,
        Supervisor,
        performance_score,
        tenure_months,
        tenure_years,
        age_group
    FROM {{ ref('silver_employee_cleaned') }}
),

enriched_engagement AS (
    SELECT
        -- Survey Information
        e.employee_id,
        e.survey_date,
        YEAR(e.survey_date) AS survey_year,
        MONTH(e.survey_date) AS survey_month,
        
        -- Validated Scores
        e.engagement_score,
        e.satisfaction_score,
        e.worklife_balance_score,
        
        -- Calculate average score across all three dimensions
        ROUND((e.engagement_score + e.satisfaction_score + e.worklife_balance_score) / 3.0, 2) AS overall_engagement_avg,
        
        -- Employee Context from cleaned employee table
        emp.is_active,
        emp.employee_status,
        emp.DepartmentType,
        emp.BusinessUnit,
        emp.Title,
        emp.Supervisor,
        emp.performance_score,
        emp.tenure_months,
        emp.tenure_years,
        emp.age_group,
        
        -- Data Quality
        e.had_invalid_scores,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS processed_at
        
    FROM validated_scores e
    LEFT JOIN employee_context emp
        ON e.employee_id = emp.EmpID
),

final_with_flags AS (
    SELECT
        *,
        
        -- FLAG: Low engagement (threshold = 3 from EDA analysis)
        -- This captures the 94% at-risk employees identified
        CASE 
            WHEN engagement_score <= {{ var('low_engagement_threshold') }}
              OR satisfaction_score <= {{ var('low_engagement_threshold') }}
              OR worklife_balance_score <= {{ var('low_engagement_threshold') }}
            THEN 1
            ELSE 0
        END AS is_at_risk,
        
        -- Engagement risk level
        CASE 
            WHEN overall_engagement_avg <= 2 THEN 'Critical'
            WHEN overall_engagement_avg <= 3 THEN 'High Risk'
            WHEN overall_engagement_avg <= 4 THEN 'Moderate'
            ELSE 'Healthy'
        END AS engagement_risk_level,
        
        -- Flag for immediate attention (multiple low scores + active employee)
        CASE 
            WHEN is_active = 1
              AND engagement_score <= 2
              AND satisfaction_score <= 2
            THEN 1
            ELSE 0
        END AS needs_immediate_attention,
        
        -- Sentiment categorization
        CASE 
            WHEN engagement_score >= 4 AND satisfaction_score >= 4 THEN 'Highly Engaged'
            WHEN engagement_score <= 2 OR satisfaction_score <= 2 THEN 'Disengaged'
            ELSE 'Neutral'
        END AS employee_sentiment,
        
        CURRENT_TIMESTAMP() AS last_updated
        
    FROM enriched_engagement
)

SELECT
    -- Survey Keys
    employee_id,
    survey_date,
    survey_year,
    survey_month,
    
    -- Engagement Scores
    engagement_score,
    satisfaction_score,
    worklife_balance_score,
    overall_engagement_avg,
    
    -- Risk Flags (Critical for dashboards!)
    is_at_risk,
    engagement_risk_level,
    needs_immediate_attention,
    employee_sentiment,
    
    -- Employee Context
    is_active,
    employee_status,
    DepartmentType,
    BusinessUnit,
    Title,
    Supervisor,
    performance_score,
    tenure_months,
    tenure_years,
    age_group,
    
    -- Data Quality
    had_invalid_scores,
    
    -- Metadata
    processed_at,
    last_updated
    
FROM final_with_flags

-- Data Quality Checks:
-- 1. At-risk count: SELECT COUNT(*) FROM silver.silver_engagement_cleaned WHERE is_at_risk = 1 AND is_active = 1
--    Expected: ~1,381 (94.14% of 1,467 active employees)
-- 2. Invalid scores: SELECT COUNT(*) FROM silver.silver_engagement_cleaned WHERE had_invalid_scores = 1
--    Expected: 0 (clean data)