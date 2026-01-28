{{
    config(
        materialized='table',
        file_format='delta',
        tags=['gold', 'at_risk', 'business_metrics', 'actionable']
    )
}}

/*
================================================================================
GOLD AT-RISK EMPLOYEES
================================================================================
Purpose: Identify and prioritize employees at risk of attrition

Risk Factors Considered:
  - Low engagement scores (from EDA: 94% of active have low engagement)
  - Low performance scores
  - Short tenure (high early attrition)
  - Department with high attrition rate
  - Supervisor with high attrition rate
  - Low training investment

Dashboard Use Cases:
  - HR intervention list (prioritized)
  - Manager alerts (employees needing attention)
  - Retention program targeting
  - Predictive attrition monitoring

Created: {{ run_started_at.strftime('%Y-%m-%d') }}
================================================================================
*/

WITH employee_base AS (
    SELECT
        e.EmpID,
        e.FirstName,
        e.LastName,
        e.ADEmail,
        e.is_active,
        e.DepartmentType,
        e.BusinessUnit,
        e.Title,
        e.Supervisor,
        e.tenure_months,
        e.tenure_years,
        e.tenure_category,
        e.age,
        e.age_group,
        e.performance_score,
        e.current_employee_rating,
        e.PayZone,
        e.EmployeeType
    FROM {{ ref('silver_employee_cleaned') }} e
    WHERE e.is_active = 1  -- Only analyze active employees
),

engagement_data AS (
    SELECT
        eng.employee_id,
        eng.engagement_score,
        eng.satisfaction_score,
        eng.worklife_balance_score,
        eng.overall_engagement_avg,
        eng.is_at_risk AS engagement_at_risk_flag,
        eng.engagement_risk_level,
        eng.needs_immediate_attention
    FROM {{ ref('silver_engagement_cleaned') }} eng
    WHERE eng.is_active = 1
),

training_data AS (
    SELECT
        employee_id,
        COUNT(*) AS total_training_sessions,
        SUM(training_cost) AS total_training_investment,
        SUM(training_duration_days) AS total_training_days,
        SUM(training_completed_flag) AS completed_training_sessions,
        MAX(training_date) AS last_training_date,
        DATEDIFF(CURRENT_DATE(), MAX(training_date)) AS days_since_last_training
    FROM {{ ref('silver_training_cleaned') }}
    GROUP BY employee_id
),

department_stats AS (
    SELECT
        DepartmentType,
        COUNT(*) AS dept_total_employees,
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS dept_attrition_rate,
        ROUND(AVG(tenure_months), 1) AS dept_avg_tenure_months
    FROM {{ ref('silver_employee_cleaned') }}
    GROUP BY DepartmentType
),

supervisor_stats AS (
    SELECT
        Supervisor,
        COUNT(*) AS supervisor_team_size,
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS supervisor_attrition_rate
    FROM {{ ref('silver_employee_cleaned') }}
    GROUP BY Supervisor
    HAVING COUNT(*) >= 3  -- Only supervisors with 3+ reports
),

combined_data AS (
    SELECT
        e.EmpID,
        e.FirstName,
        e.LastName,
        e.ADEmail,
        e.DepartmentType,
        e.BusinessUnit,
        e.Title,
        e.Supervisor,
        e.tenure_months,
        e.tenure_years,
        e.tenure_category,
        e.age,
        e.age_group,
        e.performance_score,
        e.current_employee_rating,
        e.PayZone,
        e.EmployeeType,
        
        -- Engagement metrics
        eng.engagement_score,
        eng.satisfaction_score,
        eng.worklife_balance_score,
        eng.overall_engagement_avg,
        eng.engagement_at_risk_flag,
        eng.engagement_risk_level,
        eng.needs_immediate_attention,
        
        -- Training metrics
        COALESCE(t.total_training_sessions, 0) AS total_training_sessions,
        COALESCE(t.total_training_investment, 0) AS total_training_investment,
        COALESCE(t.completed_training_sessions, 0) AS completed_training_sessions,
        t.last_training_date,
        COALESCE(t.days_since_last_training, 9999) AS days_since_last_training,
        
        -- Department context
        d.dept_total_employees,
        d.dept_attrition_rate,
        d.dept_avg_tenure_months,
        
        -- Supervisor context
        s.supervisor_team_size,
        COALESCE(s.supervisor_attrition_rate, 0) AS supervisor_attrition_rate
        
    FROM employee_base e
    LEFT JOIN engagement_data eng ON e.EmpID = eng.employee_id
    LEFT JOIN training_data t ON e.EmpID = t.employee_id
    LEFT JOIN department_stats d ON e.DepartmentType = d.DepartmentType
    LEFT JOIN supervisor_stats s ON e.Supervisor = s.Supervisor
),

risk_scoring AS (
    SELECT
        *,
        
        -- RISK FACTOR 1: Engagement Score (0-40 points)
        CASE 
            WHEN engagement_score IS NULL THEN 20  -- Missing data is moderate risk
            WHEN engagement_score <= 2 THEN 40
            WHEN engagement_score <= 3 THEN 30
            WHEN engagement_score <= 4 THEN 10
            ELSE 0
        END AS engagement_risk_points,
        
        -- RISK FACTOR 2: Satisfaction Score (0-30 points)
        CASE 
            WHEN satisfaction_score IS NULL THEN 15
            WHEN satisfaction_score <= 2 THEN 30
            WHEN satisfaction_score <= 3 THEN 20
            WHEN satisfaction_score <= 4 THEN 5
            ELSE 0
        END AS satisfaction_risk_points,
        
        -- RISK FACTOR 3: Performance Score (0-20 points)
        CASE 
            WHEN LOWER(performance_score) LIKE '%needs improvement%' THEN 20
            WHEN LOWER(performance_score) LIKE '%pip%' THEN 20
            WHEN LOWER(performance_score) LIKE '%fully meets%' THEN 5
            WHEN LOWER(performance_score) LIKE '%exceeds%' THEN 0
            ELSE 10
        END AS performance_risk_points,
        
        -- RISK FACTOR 4: Tenure (0-15 points) - New hires and first year at risk
        CASE 
            WHEN tenure_months <= 3 THEN 15  -- New hire critical period
            WHEN tenure_months <= 12 THEN 10  -- First year risk
            WHEN tenure_months <= 24 THEN 5   -- Second year
            ELSE 0
        END AS tenure_risk_points,
        
        -- RISK FACTOR 5: Department Attrition (0-15 points)
        CASE 
            WHEN dept_attrition_rate >= 70 THEN 15
            WHEN dept_attrition_rate >= 50 THEN 10
            WHEN dept_attrition_rate >= 30 THEN 5
            ELSE 0
        END AS department_risk_points,
        
        -- RISK FACTOR 6: Supervisor Attrition (0-10 points)
        CASE 
            WHEN supervisor_attrition_rate >= 60 THEN 10
            WHEN supervisor_attrition_rate >= 40 THEN 5
            ELSE 0
        END AS supervisor_risk_points,
        
        -- RISK FACTOR 7: Training Investment (0-10 points)
        CASE 
            WHEN total_training_sessions = 0 THEN 10
            WHEN days_since_last_training > 365 THEN 8
            WHEN days_since_last_training > 180 THEN 5
            ELSE 0
        END AS training_risk_points
        
    FROM combined_data
),

final_risk_assessment AS (
    SELECT
        *,
        
        -- Calculate total risk score (0-140 points)
        (engagement_risk_points + 
         satisfaction_risk_points + 
         performance_risk_points + 
         tenure_risk_points + 
         department_risk_points + 
         supervisor_risk_points + 
         training_risk_points) AS total_risk_score,
        
        CURRENT_TIMESTAMP() AS risk_calculated_at
        
    FROM risk_scoring
)

SELECT
    -- Employee Identification
    EmpID,
    FirstName,
    LastName,
    ADEmail,
    
    -- Job Information
    DepartmentType,
    BusinessUnit,
    Title,
    Supervisor,
    EmployeeType,
    PayZone,
    
    -- Tenure & Demographics
    tenure_months,
    tenure_years,
    tenure_category,
    age,
    age_group,
    
    -- Performance
    performance_score,
    current_employee_rating,
    
    -- Engagement Metrics
    engagement_score,
    satisfaction_score,
    worklife_balance_score,
    overall_engagement_avg,
    engagement_risk_level,
    needs_immediate_attention,
    
    -- Training Metrics
    total_training_sessions,
    total_training_investment,
    completed_training_sessions,
    days_since_last_training,
    
    -- Context Metrics
    dept_attrition_rate,
    supervisor_attrition_rate,
    
    -- Risk Scoring (Detailed Breakdown)
    engagement_risk_points,
    satisfaction_risk_points,
    performance_risk_points,
    tenure_risk_points,
    department_risk_points,
    supervisor_risk_points,
    training_risk_points,
    total_risk_score,
    
    -- Risk Classification
    CASE 
        WHEN total_risk_score >= 80 THEN 'Critical'
        WHEN total_risk_score >= 60 THEN 'High'
        WHEN total_risk_score >= 40 THEN 'Medium'
        ELSE 'Low'
    END AS risk_level,
    
    -- Priority Rank (within risk level)
    ROW_NUMBER() OVER (
        PARTITION BY CASE 
            WHEN total_risk_score >= 80 THEN 'Critical'
            WHEN total_risk_score >= 60 THEN 'High'
            WHEN total_risk_score >= 40 THEN 'Medium'
            ELSE 'Low'
        END
        ORDER BY total_risk_score DESC, tenure_months ASC
    ) AS priority_rank,
    
    -- Recommended Actions
    CASE 
        WHEN total_risk_score >= 80 THEN 'IMMEDIATE: Schedule 1-on-1, review compensation, development plan'
        WHEN total_risk_score >= 60 THEN 'URGENT: Manager check-in, engagement survey follow-up, training needs assessment'
        WHEN total_risk_score >= 40 THEN 'PROACTIVE: Quarterly review, career development discussion'
        ELSE 'MONITOR: Standard engagement activities'
    END AS recommended_action,
    
    -- Primary Risk Driver
    CASE 
        WHEN engagement_risk_points >= 30 THEN 'Low Engagement'
        WHEN satisfaction_risk_points >= 20 THEN 'Low Satisfaction'
        WHEN performance_risk_points >= 15 THEN 'Performance Issues'
        WHEN tenure_risk_points >= 10 THEN 'New Hire Risk'
        WHEN department_risk_points >= 10 THEN 'Department Issues'
        WHEN supervisor_risk_points >= 5 THEN 'Supervisor Issues'
        WHEN training_risk_points >= 8 THEN 'Lack of Development'
        ELSE 'Multiple Factors'
    END AS primary_risk_factor,
    
    -- Metadata
    risk_calculated_at,
    CURRENT_TIMESTAMP() AS last_updated
    
FROM final_risk_assessment
WHERE total_risk_score >= 40  -- Only show Medium, High, Critical risk employees
ORDER BY total_risk_score DESC, tenure_months ASC

/*
POWER BI USAGE:
- Filter by risk_level for executive summary
- Show priority_rank for action list
- Drill down by primary_risk_factor
- Create alerts for risk_level = 'Critical'
- Show recommended_action as tooltip
- Use total_risk_score for gauge visuals

IMMEDIATE ACTION LIST:
SELECT * FROM workspace.gold.gold_at_risk_employees
WHERE risk_level IN ('Critical', 'High')
ORDER BY total_risk_score DESC
LIMIT 20;
*/