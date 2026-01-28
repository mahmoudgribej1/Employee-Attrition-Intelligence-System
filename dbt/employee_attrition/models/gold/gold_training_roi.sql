{{
    config(
        materialized='table',
        file_format='delta',
        tags=['gold', 'training', 'roi', 'business_metrics']
    )
}}

/*
================================================================================
GOLD TRAINING ROI
================================================================================
Purpose: Training effectiveness and return on investment analysis

Metrics Included:
  - Training participation and completion rates
  - Training cost analysis by type, program, department
  - Training impact on performance
  - Training impact on retention
  - Cost per employee and ROI calculations

Dashboard Use Cases:
  - Training program effectiveness
  - Budget allocation decisions
  - Department training needs
  - Trainer performance evaluation

Created: {{ run_started_at.strftime('%Y-%m-%d') }}
================================================================================
*/

WITH training_base AS (
    SELECT
        employee_id,
        training_date,
        training_year,
        training_quarter,
        training_program_name,
        training_type,
        training_type_category,
        training_outcome,
        training_completed_flag,
        trainer,
        training_duration_days,
        training_cost,
        cost_per_training_day,
        DepartmentType,
        employee_is_active,
        performance_score
    FROM {{ ref('silver_training_cleaned') }}
),

employee_context AS (
    SELECT
        EmpID,
        is_active,
        DepartmentType,
        tenure_years,
        performance_score
    FROM {{ ref('silver_employee_cleaned') }}
),

-- Overall training metrics
overall_metrics AS (
    SELECT
        'Company-Wide' AS metric_level,
        'All Training' AS metric_segment,
        
        -- Participation metrics
        COUNT(*) AS total_training_sessions,
        COUNT(DISTINCT employee_id) AS unique_employees_trained,
        COUNT(DISTINCT training_program_name) AS unique_programs,
        
        -- Completion metrics
        SUM(training_completed_flag) AS completed_sessions,
        ROUND(SUM(training_completed_flag) * 100.0 / NULLIF(COUNT(*), 0), 2) AS completion_rate,
        
        -- Duration metrics
        SUM(training_duration_days) AS total_training_days,
        ROUND(AVG(training_duration_days), 1) AS avg_duration_days,
        
        -- Cost metrics
        ROUND(SUM(training_cost), 2) AS total_investment,
        ROUND(AVG(training_cost), 2) AS avg_cost_per_session,
        ROUND(SUM(training_cost) / NULLIF(COUNT(DISTINCT employee_id), 0), 2) AS cost_per_employee,
        ROUND(AVG(cost_per_training_day), 2) AS avg_cost_per_day,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM training_base
),

-- Training by type
training_by_type AS (
    SELECT
        'Training Type' AS metric_level,
        training_type_category AS metric_segment,
        
        COUNT(*) AS total_training_sessions,
        COUNT(DISTINCT employee_id) AS unique_employees_trained,
        COUNT(DISTINCT training_program_name) AS unique_programs,
        
        SUM(training_completed_flag) AS completed_sessions,
        ROUND(SUM(training_completed_flag) * 100.0 / NULLIF(COUNT(*), 0), 2) AS completion_rate,
        
        SUM(training_duration_days) AS total_training_days,
        ROUND(AVG(training_duration_days), 1) AS avg_duration_days,
        
        ROUND(SUM(training_cost), 2) AS total_investment,
        ROUND(AVG(training_cost), 2) AS avg_cost_per_session,
        ROUND(SUM(training_cost) / NULLIF(COUNT(DISTINCT employee_id), 0), 2) AS cost_per_employee,
        ROUND(AVG(cost_per_training_day), 2) AS avg_cost_per_day,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM training_base
    GROUP BY training_type_category
),

-- Training by program
training_by_program AS (
    SELECT
        'Training Program' AS metric_level,
        training_program_name AS metric_segment,
        
        COUNT(*) AS total_training_sessions,
        COUNT(DISTINCT employee_id) AS unique_employees_trained,
        COUNT(DISTINCT training_program_name) AS unique_programs,
        
        SUM(training_completed_flag) AS completed_sessions,
        ROUND(SUM(training_completed_flag) * 100.0 / NULLIF(COUNT(*), 0), 2) AS completion_rate,
        
        SUM(training_duration_days) AS total_training_days,
        ROUND(AVG(training_duration_days), 1) AS avg_duration_days,
        
        ROUND(SUM(training_cost), 2) AS total_investment,
        ROUND(AVG(training_cost), 2) AS avg_cost_per_session,
        ROUND(SUM(training_cost) / NULLIF(COUNT(DISTINCT employee_id), 0), 2) AS cost_per_employee,
        ROUND(AVG(cost_per_training_day), 2) AS avg_cost_per_day,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM training_base
    GROUP BY training_program_name
    HAVING COUNT(*) >= 5  -- Only programs with 5+ sessions
),

-- Training by department
training_by_department AS (
    SELECT
        'Department' AS metric_level,
        DepartmentType AS metric_segment,
        
        COUNT(*) AS total_training_sessions,
        COUNT(DISTINCT employee_id) AS unique_employees_trained,
        COUNT(DISTINCT training_program_name) AS unique_programs,
        
        SUM(training_completed_flag) AS completed_sessions,
        ROUND(SUM(training_completed_flag) * 100.0 / NULLIF(COUNT(*), 0), 2) AS completion_rate,
        
        SUM(training_duration_days) AS total_training_days,
        ROUND(AVG(training_duration_days), 1) AS avg_duration_days,
        
        ROUND(SUM(training_cost), 2) AS total_investment,
        ROUND(AVG(training_cost), 2) AS avg_cost_per_session,
        ROUND(SUM(training_cost) / NULLIF(COUNT(DISTINCT employee_id), 0), 2) AS cost_per_employee,
        ROUND(AVG(cost_per_training_day), 2) AS avg_cost_per_day,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM training_base
    WHERE DepartmentType IS NOT NULL
    GROUP BY DepartmentType
),

-- Training by trainer
training_by_trainer AS (
    SELECT
        'Trainer' AS metric_level,
        trainer AS metric_segment,
        
        COUNT(*) AS total_training_sessions,
        COUNT(DISTINCT employee_id) AS unique_employees_trained,
        COUNT(DISTINCT training_program_name) AS unique_programs,
        
        SUM(training_completed_flag) AS completed_sessions,
        ROUND(SUM(training_completed_flag) * 100.0 / NULLIF(COUNT(*), 0), 2) AS completion_rate,
        
        SUM(training_duration_days) AS total_training_days,
        ROUND(AVG(training_duration_days), 1) AS avg_duration_days,
        
        ROUND(SUM(training_cost), 2) AS total_investment,
        ROUND(AVG(training_cost), 2) AS avg_cost_per_session,
        ROUND(SUM(training_cost) / NULLIF(COUNT(DISTINCT employee_id), 0), 2) AS cost_per_employee,
        ROUND(AVG(cost_per_training_day), 2) AS avg_cost_per_day,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM training_base
    WHERE trainer IS NOT NULL
    GROUP BY trainer
    HAVING COUNT(*) >= 5  -- Only trainers with 5+ sessions
),

-- Training retention impact
training_retention_impact AS (
    SELECT
        'Retention Impact' AS metric_level,
        CASE 
            WHEN t.employee_id IS NOT NULL THEN 'Received Training'
            ELSE 'No Training'
        END AS metric_segment,
        
        -- Training metrics
        COUNT(DISTINCT t.employee_id) AS total_training_sessions,
        COUNT(DISTINCT t.employee_id) AS unique_employees_trained,
        NULL AS unique_programs,
        
        SUM(t.training_completed_flag) AS completed_sessions,
        ROUND(AVG(CASE WHEN t.training_completed_flag = 1 THEN 100.0 ELSE 0 END), 2) AS completion_rate,
        
        SUM(t.training_duration_days) AS total_training_days,
        ROUND(AVG(t.training_duration_days), 1) AS avg_duration_days,
        
        ROUND(SUM(t.training_cost), 2) AS total_investment,
        ROUND(AVG(t.training_cost), 2) AS avg_cost_per_session,
        ROUND(SUM(t.training_cost) / NULLIF(COUNT(DISTINCT t.employee_id), 0), 2) AS cost_per_employee,
        ROUND(AVG(t.cost_per_training_day), 2) AS avg_cost_per_day,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM employee_context e
    LEFT JOIN training_base t ON e.EmpID = t.employee_id
    GROUP BY CASE WHEN t.employee_id IS NOT NULL THEN 'Received Training' ELSE 'No Training' END
),

-- Combine all metrics
combined_metrics AS (
    SELECT * FROM overall_metrics
    UNION ALL
    SELECT * FROM training_by_type
    UNION ALL
    SELECT * FROM training_by_program
    UNION ALL
    SELECT * FROM training_by_department
    UNION ALL
    SELECT * FROM training_by_trainer
)

SELECT
    metric_level,
    metric_segment,
    
    -- Participation Metrics
    total_training_sessions,
    unique_employees_trained,
    unique_programs,
    
    -- Effectiveness Metrics
    completed_sessions,
    completion_rate,
    
    -- Duration Metrics
    total_training_days,
    avg_duration_days,
    
    -- Cost Metrics
    total_investment,
    avg_cost_per_session,
    cost_per_employee,
    avg_cost_per_day,
    
    -- ROI Indicators
    CASE 
        WHEN completion_rate >= 90 AND cost_per_employee <= 1000 THEN 'High ROI'
        WHEN completion_rate >= 75 AND cost_per_employee <= 1500 THEN 'Good ROI'
        WHEN completion_rate >= 50 THEN 'Moderate ROI'
        ELSE 'Low ROI - Review Needed'
    END AS roi_category,
    
    -- Efficiency Score (0-100)
    ROUND(
        (completion_rate * 0.5) +  -- 50% weight on completion
        (CASE 
            WHEN cost_per_employee <= 500 THEN 50
            WHEN cost_per_employee <= 1000 THEN 35
            WHEN cost_per_employee <= 1500 THEN 20
            ELSE 10
        END * 0.3) +  -- 30% weight on cost efficiency
        (CASE 
            WHEN avg_duration_days <= 3 THEN 20
            WHEN avg_duration_days <= 5 THEN 15
            ELSE 10
        END * 0.2),  -- 20% weight on time efficiency
    1) AS efficiency_score,
    
    -- Metadata
    calculated_at,
    CURRENT_TIMESTAMP() AS last_updated
    
FROM combined_metrics
ORDER BY 
    CASE metric_level
        WHEN 'Company-Wide' THEN 1
        WHEN 'Training Type' THEN 2
        WHEN 'Training Program' THEN 3
        WHEN 'Department' THEN 4
        WHEN 'Trainer' THEN 5
        WHEN 'Retention Impact' THEN 6
    END,
    total_investment DESC

/*
POWER BI USAGE:
- Executive summary: Filter metric_level = 'Company-Wide'
- Program comparison: Filter metric_level = 'Training Program'
- Cost analysis: Use total_investment and cost_per_employee
- Effectiveness: Use completion_rate and efficiency_score
- ROI quadrant chart: completion_rate (y-axis) vs cost_per_employee (x-axis)

KEY INSIGHTS QUERY:
-- Top 10 most effective programs
SELECT * FROM workspace.gold.gold_training_roi
WHERE metric_level = 'Training Program'
ORDER BY efficiency_score DESC
LIMIT 10;

-- Training investment by department
SELECT * FROM workspace.gold.gold_training_roi
WHERE metric_level = 'Department'
ORDER BY total_investment DESC;
*/