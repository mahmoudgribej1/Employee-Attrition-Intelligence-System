{{
    config(
        materialized='table',
        file_format='delta',
        partition_by=['snapshot_date_key'],
        tags=['gold', 'star_schema', 'fact']
    )
}}

/*
Fact: Employee Snapshot
Grain: One row per employee per snapshot date
Type: Periodic snapshot (daily/monthly)
*/

WITH employee_snapshot AS (
    SELECT
        e.EmpID,
        e.DepartmentType,
        e.is_active,
        e.tenure_months,
        e.tenure_years,
        e.performance_score,
        e.current_employee_rating,
        
        -- Engagement data
        eng.engagement_score,
        eng.satisfaction_score,
        eng.worklife_balance_score,
        eng.overall_engagement_avg,
        eng.is_at_risk AS engagement_at_risk_flag,
        
        -- Training data
        t.total_training_sessions,
        t.total_training_investment,
        t.total_training_days,
        t.completed_training_sessions,
        
        -- Risk data
        r.total_risk_score,
        r.risk_level,
        
        -- Current date as snapshot date
        CURRENT_DATE() AS snapshot_date
        
    FROM {{ ref('silver_employee_cleaned') }} e
    LEFT JOIN {{ ref('silver_engagement_cleaned') }} eng 
        ON e.EmpID = eng.employee_id
    LEFT JOIN (
        SELECT 
            employee_id,
            COUNT(*) AS total_training_sessions,
            SUM(training_cost) AS total_training_investment,
            SUM(training_duration_days) AS total_training_days,
            SUM(training_completed_flag) AS completed_training_sessions
        FROM {{ ref('silver_training_cleaned') }}
        GROUP BY employee_id
    ) t ON e.EmpID = t.employee_id
    LEFT JOIN {{ ref('gold_at_risk_employees') }} r 
        ON e.EmpID = r.EmpID
),

with_keys AS (
    SELECT
        s.*,
        
        -- Get department key
        d.department_key,
        
        -- Create date key
        CAST(DATE_FORMAT(s.snapshot_date, 'yyyyMMdd') AS INT) AS snapshot_date_key
        
    FROM employee_snapshot s
    LEFT JOIN {{ ref('dim_department') }} d 
        ON s.DepartmentType = d.department_name
)

SELECT
    -- Foreign Keys
    EmpID AS employee_key,
    department_key,
    snapshot_date_key,
    
    -- Degenerate Dimensions
    performance_score,
    risk_level,
    
    -- Measures (Additive)
    1 AS employee_count,
    CASE WHEN is_active = 1 THEN 1 ELSE 0 END AS active_count,
    CASE WHEN is_active = 0 THEN 1 ELSE 0 END AS departed_count,
    CASE WHEN engagement_at_risk_flag = 1 THEN 1 ELSE 0 END AS at_risk_count,
    
    -- Measures (Semi-Additive - can sum across employees, not time)
    tenure_months,
    tenure_years,
    total_training_sessions,
    total_training_investment,
    total_training_days,
    completed_training_sessions,
    
    -- Measures (Non-Additive - must average)
    engagement_score,
    satisfaction_score,
    worklife_balance_score,
    overall_engagement_avg,
    current_employee_rating,
    total_risk_score,
    
    -- Calculated Measures
    CASE WHEN is_active = 0 THEN 75000 * 0.625 ELSE 0 END AS attrition_cost,
    CASE WHEN total_training_investment > 0 AND tenure_months > 0 THEN try_divide(total_training_investment, tenure_months) ELSE 0 END AS training_cost_per_tenure_month,
    
    -- Flags
    is_active,
    engagement_at_risk_flag,
    CASE WHEN tenure_months <= 3 THEN 1 ELSE 0 END AS is_new_hire,
    CASE WHEN total_risk_score >= 80 THEN 1 ELSE 0 END AS is_critical_risk,
    
    -- Metadata
    snapshot_date,
    CURRENT_TIMESTAMP() AS last_updated

FROM with_keys