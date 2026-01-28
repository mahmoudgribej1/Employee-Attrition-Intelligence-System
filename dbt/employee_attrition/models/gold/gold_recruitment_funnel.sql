{{
    config(
        materialized='table',
        file_format='delta',
        tags=['gold', 'recruitment', 'business_metrics']
    )
}}

/*
================================================================================
GOLD RECRUITMENT FUNNEL
================================================================================
Purpose: Business-ready recruitment funnel metrics for dashboards

Metrics Included:
  - Funnel stage distribution and conversion rates
  - Average time in each stage (if timestamps available)
  - Applications by job title, education, experience
  - Geographic distribution
  - Rejection analysis

Dashboard Use Cases:
  - Recruitment funnel visualization (Applied → Offered)
  - Hiring pipeline health
  - Candidate quality metrics
  - Recruitment efficiency KPIs

Created: {{ run_started_at.strftime('%Y-%m-%d') }}
================================================================================
*/

WITH recruitment_base AS (
    SELECT
        applicant_id,
        application_date,
        application_year,
        application_month,
        recruitment_status,
        hiring_stage,
        funnel_stage_number,
        job_title,
        education_level,
        years_of_experience,
        experience_category,
        desired_salary,
        salary_range,
        gender,
        age_at_application,
        state,
        city
    FROM {{ ref('silver_recruitment_cleaned') }}
),

-- Overall funnel metrics
funnel_summary AS (
    SELECT
        'Overall' AS segment_type,
        'All Applications' AS segment_value,
        
        -- Stage counts
        COUNT(*) AS total_applications,
        SUM(CASE WHEN hiring_stage = 'Applied' THEN 1 ELSE 0 END) AS stage_applied,
        SUM(CASE WHEN hiring_stage = 'In Review' THEN 1 ELSE 0 END) AS stage_in_review,
        SUM(CASE WHEN hiring_stage = 'Interviewing' THEN 1 ELSE 0 END) AS stage_interviewing,
        SUM(CASE WHEN hiring_stage = 'Offered - Pending' THEN 1 ELSE 0 END) AS stage_offered,
        SUM(CASE WHEN hiring_stage = 'Rejected' THEN 1 ELSE 0 END) AS stage_rejected,
        
        -- Conversion rates (%)
        ROUND(SUM(CASE WHEN funnel_stage_number >= 2 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS conversion_to_review,
        ROUND(SUM(CASE WHEN funnel_stage_number >= 3 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN funnel_stage_number >= 2 THEN 1 ELSE 0 END), 0), 2) AS conversion_to_interview,
        ROUND(SUM(CASE WHEN funnel_stage_number >= 4 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN funnel_stage_number >= 3 THEN 1 ELSE 0 END), 0), 2) AS conversion_to_offer,
        
        -- Rejection metrics
        ROUND(SUM(CASE WHEN hiring_stage = 'Rejected' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rejection_rate,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM recruitment_base
),

-- Funnel by job title
funnel_by_job_title AS (
    SELECT
        'Job Title' AS segment_type,
        job_title AS segment_value,
        
        COUNT(*) AS total_applications,
        SUM(CASE WHEN hiring_stage = 'Applied' THEN 1 ELSE 0 END) AS stage_applied,
        SUM(CASE WHEN hiring_stage = 'In Review' THEN 1 ELSE 0 END) AS stage_in_review,
        SUM(CASE WHEN hiring_stage = 'Interviewing' THEN 1 ELSE 0 END) AS stage_interviewing,
        SUM(CASE WHEN hiring_stage = 'Offered - Pending' THEN 1 ELSE 0 END) AS stage_offered,
        SUM(CASE WHEN hiring_stage = 'Rejected' THEN 1 ELSE 0 END) AS stage_rejected,
        
        ROUND(SUM(CASE WHEN funnel_stage_number >= 2 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS conversion_to_review,
        ROUND(SUM(CASE WHEN funnel_stage_number >= 3 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN funnel_stage_number >= 2 THEN 1 ELSE 0 END), 0), 2) AS conversion_to_interview,
        ROUND(SUM(CASE WHEN funnel_stage_number >= 4 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN funnel_stage_number >= 3 THEN 1 ELSE 0 END), 0), 2) AS conversion_to_offer,
        ROUND(SUM(CASE WHEN hiring_stage = 'Rejected' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rejection_rate,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM recruitment_base
    GROUP BY job_title
    HAVING COUNT(*) >= 10  -- Only job titles with 10+ applications
),

-- Funnel by education level
funnel_by_education AS (
    SELECT
        'Education Level' AS segment_type,
        education_level AS segment_value,
        
        COUNT(*) AS total_applications,
        SUM(CASE WHEN hiring_stage = 'Applied' THEN 1 ELSE 0 END) AS stage_applied,
        SUM(CASE WHEN hiring_stage = 'In Review' THEN 1 ELSE 0 END) AS stage_in_review,
        SUM(CASE WHEN hiring_stage = 'Interviewing' THEN 1 ELSE 0 END) AS stage_interviewing,
        SUM(CASE WHEN hiring_stage = 'Offered - Pending' THEN 1 ELSE 0 END) AS stage_offered,
        SUM(CASE WHEN hiring_stage = 'Rejected' THEN 1 ELSE 0 END) AS stage_rejected,
        
        ROUND(SUM(CASE WHEN funnel_stage_number >= 2 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS conversion_to_review,
        ROUND(SUM(CASE WHEN funnel_stage_number >= 3 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN funnel_stage_number >= 2 THEN 1 ELSE 0 END), 0), 2) AS conversion_to_interview,
        ROUND(SUM(CASE WHEN funnel_stage_number >= 4 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN funnel_stage_number >= 3 THEN 1 ELSE 0 END), 0), 2) AS conversion_to_offer,
        ROUND(SUM(CASE WHEN hiring_stage = 'Rejected' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rejection_rate,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM recruitment_base
    GROUP BY education_level
),

-- Funnel by experience category
funnel_by_experience AS (
    SELECT
        'Experience Category' AS segment_type,
        experience_category AS segment_value,
        
        COUNT(*) AS total_applications,
        SUM(CASE WHEN hiring_stage = 'Applied' THEN 1 ELSE 0 END) AS stage_applied,
        SUM(CASE WHEN hiring_stage = 'In Review' THEN 1 ELSE 0 END) AS stage_in_review,
        SUM(CASE WHEN hiring_stage = 'Interviewing' THEN 1 ELSE 0 END) AS stage_interviewing,
        SUM(CASE WHEN hiring_stage = 'Offered - Pending' THEN 1 ELSE 0 END) AS stage_offered,
        SUM(CASE WHEN hiring_stage = 'Rejected' THEN 1 ELSE 0 END) AS stage_rejected,
        
        ROUND(SUM(CASE WHEN funnel_stage_number >= 2 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS conversion_to_review,
        ROUND(SUM(CASE WHEN funnel_stage_number >= 3 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN funnel_stage_number >= 2 THEN 1 ELSE 0 END), 0), 2) AS conversion_to_interview,
        ROUND(SUM(CASE WHEN funnel_stage_number >= 4 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN funnel_stage_number >= 3 THEN 1 ELSE 0 END), 0), 2) AS conversion_to_offer,
        ROUND(SUM(CASE WHEN hiring_stage = 'Rejected' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rejection_rate,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM recruitment_base
    GROUP BY experience_category
),

-- Combine all segments
combined_funnel AS (
    SELECT * FROM funnel_summary
    UNION ALL
    SELECT * FROM funnel_by_job_title
    UNION ALL
    SELECT * FROM funnel_by_education
    UNION ALL
    SELECT * FROM funnel_by_experience
)

SELECT
    segment_type AS metric_level,
    segment_value AS metric_segment,
    
    -- Application volumes
    total_applications,
    stage_applied,
    stage_in_review,
    stage_interviewing,
    stage_offered,
    stage_rejected,
    
    -- Conversion rates
    conversion_to_review,
    conversion_to_interview,
    conversion_to_offer,
    rejection_rate,
    
    -- Funnel health indicator
    CASE 
        WHEN conversion_to_offer >= 50 THEN 'Healthy'
        WHEN conversion_to_offer >= 30 THEN 'Moderate'
        ELSE 'Needs Improvement'
    END AS funnel_health,
    
    -- Metadata
    calculated_at,
    CURRENT_TIMESTAMP() AS last_updated
    
FROM combined_funnel
ORDER BY 
    CASE segment_type
        WHEN 'Overall' THEN 1
        WHEN 'Job Title' THEN 2
        WHEN 'Education Level' THEN 3
        WHEN 'Experience Category' THEN 4
    END,
    total_applications DESC

/*
POWER BI USAGE:
- Create funnel visualization filtered by segment_type
- Show conversion rates as % metrics
- Drill through from overall to specific segments
- Color code by funnel_health
*/