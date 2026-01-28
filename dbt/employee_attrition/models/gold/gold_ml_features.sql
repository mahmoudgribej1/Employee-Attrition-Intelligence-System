{{
    config(
        materialized='table',
        file_format='delta',
        tags=['gold', 'ml', 'features', 'modeling']
    )
}}

/*
================================================================================
GOLD ML FEATURES
================================================================================
Purpose: Complete feature set for ML attrition prediction model

Feature Categories:
  1. Employee Demographics (age, gender, marital status)
  2. Job Characteristics (department, title, tenure, pay zone)
  3. Performance Metrics (performance score, rating)
  4. Engagement Scores (engagement, satisfaction, work-life balance)
  5. Training Features (sessions, investment, completion)
  6. Aggregate Features (dept attrition rate, supervisor attrition rate)
  7. Temporal Features (hire year, tenure categories)

Target Variable: is_active (1 = Active, 0 = Departed)

Model Use: Random Forest or Logistic Regression for binary classification

Created: {{ run_started_at.strftime('%Y-%m-%d') }}
================================================================================
*/

WITH employee_base AS (
    SELECT
        EmpID,
        is_active,  -- TARGET VARIABLE
        
        -- Demographics
        age,
        age_group,
        GenderCode,
        RaceDesc,
        MaritalDesc,
        
        -- Job Characteristics
        DepartmentType,
        BusinessUnit,
        Division,
        Title,
        Supervisor,
        PayZone,
        EmployeeType,
        EmployeeClassificationType,
        
        -- Tenure Features
        tenure_days,
        tenure_months,
        tenure_years,
        tenure_category,
        hire_year,
        
        -- Performance
        performance_score,
        current_employee_rating,
        
        -- Termination context (for feature engineering)
        termination_type_clean,
        
        -- Location
        State
        
    FROM {{ ref('silver_employee_cleaned') }}
),

engagement_features AS (
    SELECT
        employee_id,
        engagement_score,
        satisfaction_score,
        worklife_balance_score,
        overall_engagement_avg,
        engagement_risk_level,
        is_at_risk AS engagement_at_risk_flag,
        needs_immediate_attention
    FROM {{ ref('silver_engagement_cleaned') }}
),

training_features AS (
    SELECT
        employee_id,
        COUNT(*) AS total_training_sessions,
        SUM(training_cost) AS total_training_investment,
        SUM(training_duration_days) AS total_training_days,
        SUM(training_completed_flag) AS completed_training_count,
        ROUND(SUM(training_completed_flag) * 100.0 / NULLIF(COUNT(*), 0), 2) AS training_completion_rate,
        
        -- Most recent training
        MAX(training_date) AS last_training_date,
        DATEDIFF(CURRENT_DATE(), MAX(training_date)) AS days_since_last_training,
        
        -- Training diversity
        COUNT(DISTINCT training_type) AS unique_training_types,
        COUNT(DISTINCT trainer) AS unique_trainers
        
    FROM {{ ref('silver_training_cleaned') }}
    GROUP BY employee_id
),

-- Department-level aggregate features
department_aggregates AS (
    SELECT
        DepartmentType,
        COUNT(*) AS dept_size,
        ROUND(AVG(tenure_months), 1) AS dept_avg_tenure_months,
        ROUND(AVG(age), 1) AS dept_avg_age,
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS dept_attrition_rate
    FROM employee_base
    GROUP BY DepartmentType
),

-- Supervisor-level aggregate features
supervisor_aggregates AS (
    SELECT
        Supervisor,
        COUNT(*) AS supervisor_team_size,
        ROUND(AVG(tenure_months), 1) AS supervisor_avg_team_tenure,
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS supervisor_attrition_rate
    FROM employee_base
    GROUP BY Supervisor
),

-- Business unit aggregates
business_unit_aggregates AS (
    SELECT
        BusinessUnit,
        COUNT(*) AS bu_size,
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS bu_attrition_rate
    FROM employee_base
    GROUP BY BusinessUnit
),

-- Join all features
complete_feature_set AS (
    SELECT
        e.EmpID,
        
        -- TARGET VARIABLE (for training)
        e.is_active,
        
        -- DEMOGRAPHIC FEATURES
        e.age,
        CASE 
            WHEN e.age_group = 'Under 25' THEN 0
            WHEN e.age_group = '25-34' THEN 1
            WHEN e.age_group = '35-44' THEN 2
            WHEN e.age_group = '45-54' THEN 3
            ELSE 4
        END AS age_group_encoded,
        
        CASE WHEN e.GenderCode = 'M' THEN 1 ELSE 0 END AS is_male,
        CASE WHEN e.GenderCode = 'F' THEN 1 ELSE 0 END AS is_female,
        
        CASE 
            WHEN LOWER(e.MaritalDesc) LIKE '%married%' THEN 1 
            ELSE 0 
        END AS is_married,
        
        -- JOB CHARACTERISTIC FEATURES
        e.DepartmentType,
        e.BusinessUnit,
        e.Title,
        e.PayZone,
        e.EmployeeType,
        
        -- TENURE FEATURES
        e.tenure_days,
        e.tenure_months,
        e.tenure_years,
        
        CASE 
            WHEN e.tenure_months <= 3 THEN 1 ELSE 0 
        END AS is_new_hire,
        
        CASE 
            WHEN e.tenure_months <= 12 THEN 1 ELSE 0 
        END AS is_first_year,
        
        CASE 
            WHEN e.tenure_years >= 5 THEN 1 ELSE 0 
        END AS is_long_tenure,
        
        e.hire_year,
        YEAR(CURRENT_DATE()) - e.hire_year AS years_since_hire,
        
        -- PERFORMANCE FEATURES
        e.performance_score,
        e.current_employee_rating,
        
        CASE 
            WHEN LOWER(e.performance_score) LIKE '%exceeds%' THEN 1
            ELSE 0
        END AS is_high_performer,
        
        CASE 
            WHEN LOWER(e.performance_score) LIKE '%needs improvement%' 
              OR LOWER(e.performance_score) LIKE '%pip%' THEN 1
            ELSE 0
        END AS is_low_performer,
        
        -- ENGAGEMENT FEATURES
        COALESCE(eng.engagement_score, 3) AS engagement_score,
        COALESCE(eng.satisfaction_score, 3) AS satisfaction_score,
        COALESCE(eng.worklife_balance_score, 3) AS worklife_balance_score,
        COALESCE(eng.overall_engagement_avg, 3) AS overall_engagement_avg,
        COALESCE(eng.engagement_at_risk_flag, 0) AS engagement_at_risk_flag,
        COALESCE(eng.needs_immediate_attention, 0) AS needs_immediate_attention,
        
        -- TRAINING FEATURES
        COALESCE(t.total_training_sessions, 0) AS total_training_sessions,
        COALESCE(t.total_training_investment, 0) AS total_training_investment,
        COALESCE(t.completed_training_count, 0) AS completed_training_count,
        COALESCE(t.training_completion_rate, 0) AS training_completion_rate,
        COALESCE(t.days_since_last_training, 9999) AS days_since_last_training,
        COALESCE(t.unique_training_types, 0) AS unique_training_types,
        
        CASE WHEN t.total_training_sessions > 0 THEN 1 ELSE 0 END AS has_received_training,
        CASE WHEN t.days_since_last_training <= 180 THEN 1 ELSE 0 END AS recent_training_flag,
        
        -- DEPARTMENT AGGREGATE FEATURES
        d.dept_size,
        d.dept_avg_tenure_months,
        d.dept_avg_age,
        d.dept_attrition_rate,
        
        CASE WHEN d.dept_attrition_rate >= 50 THEN 1 ELSE 0 END AS high_attrition_dept_flag,
        
        -- SUPERVISOR AGGREGATE FEATURES
        COALESCE(s.supervisor_team_size, 1) AS supervisor_team_size,
        COALESCE(s.supervisor_avg_team_tenure, e.tenure_months) AS supervisor_avg_team_tenure,
        COALESCE(s.supervisor_attrition_rate, 0) AS supervisor_attrition_rate,
        
        CASE WHEN s.supervisor_attrition_rate >= 50 THEN 1 ELSE 0 END AS high_attrition_supervisor_flag,
        
        -- BUSINESS UNIT AGGREGATE FEATURES
        bu.bu_size,
        bu.bu_attrition_rate,
        
        -- DERIVED FEATURES
        -- Relative tenure (compared to department average)
        CASE 
            WHEN e.tenure_months < d.dept_avg_tenure_months * 0.5 THEN 'Below Average'
            WHEN e.tenure_months > d.dept_avg_tenure_months * 1.5 THEN 'Above Average'
            ELSE 'Average'
        END AS relative_tenure_to_dept,
        
        -- Engagement gap from department average (would need dept engagement avg - simplified here)
        CASE 
            WHEN eng.overall_engagement_avg <= 2.5 THEN 'Low'
            WHEN eng.overall_engagement_avg >= 4 THEN 'High'
            ELSE 'Medium'
        END AS engagement_level,
        
        -- Training investment relative to peers
        CASE 
            WHEN t.total_training_investment >= 1000 THEN 'High Investment'
            WHEN t.total_training_investment >= 500 THEN 'Medium Investment'
            WHEN t.total_training_investment > 0 THEN 'Low Investment'
            ELSE 'No Investment'
        END AS training_investment_level,
        
        -- METADATA
        CURRENT_TIMESTAMP() AS features_created_at
        
    FROM employee_base e
    LEFT JOIN engagement_features eng ON e.EmpID = eng.employee_id
    LEFT JOIN training_features t ON e.EmpID = t.employee_id
    LEFT JOIN department_aggregates d ON e.DepartmentType = d.DepartmentType
    LEFT JOIN supervisor_aggregates s ON e.Supervisor = s.Supervisor
    LEFT JOIN business_unit_aggregates bu ON e.BusinessUnit = bu.BusinessUnit
)

SELECT
    -- Identifier
    EmpID,
    
    -- TARGET VARIABLE
    is_active,
    
    -- NUMERICAL FEATURES (20)
    age,
    tenure_days,
    tenure_months,
    tenure_years,
    years_since_hire,
    current_employee_rating,
    engagement_score,
    satisfaction_score,
    worklife_balance_score,
    overall_engagement_avg,
    total_training_sessions,
    total_training_investment,
    completed_training_count,
    training_completion_rate,
    days_since_last_training,
    unique_training_types,
    dept_size,
    dept_avg_tenure_months,
    dept_attrition_rate,
    supervisor_team_size,
    supervisor_attrition_rate,
    bu_attrition_rate,
    
    -- BINARY FEATURES (11)
    is_male,
    is_female,
    is_married,
    is_new_hire,
    is_first_year,
    is_long_tenure,
    is_high_performer,
    is_low_performer,
    engagement_at_risk_flag,
    needs_immediate_attention,
    has_received_training,
    recent_training_flag,
    high_attrition_dept_flag,
    high_attrition_supervisor_flag,
    
    -- CATEGORICAL FEATURES (to be encoded in ML pipeline)
    DepartmentType,
    BusinessUnit,
    Title,
    PayZone,
    EmployeeType,
    performance_score,
    age_group_encoded,
    relative_tenure_to_dept,
    engagement_level,
    training_investment_level,
    
    -- METADATA
    features_created_at,
    CURRENT_TIMESTAMP() AS last_updated
    
FROM complete_feature_set

/*
ML MODEL USAGE:

1. FEATURE PREPARATION:
   - Numerical features: Standard scaling
   - Binary features: Use as-is (0/1)
   - Categorical features: One-hot encoding or label encoding

2. FEATURE IMPORTANCE ANALYSIS:
   Run Random Forest to identify top predictors

3. MODEL TRAINING:
   - Target: is_active (1 = Retained, 0 = Attrition)
   - Train/Test Split: 80/20
   - Algorithms: Random Forest, Logistic Regression, XGBoost

4. FEATURE SELECTION:
   Use this query to get feature correlation with target:
   
   SELECT 
       CORR(is_active, engagement_score) as engagement_corr,
       CORR(is_active, tenure_months) as tenure_corr,
       CORR(is_active, dept_attrition_rate) as dept_attrition_corr
   FROM workspace.gold.gold_ml_features;

EXPECTED TOP FEATURES (from EDA):
- engagement_score (94% at-risk have low engagement)
- dept_attrition_rate (Executive Office: 79.2%)
- tenure_months (departed avg: 1.3 years vs active: 4.9 years)
- is_new_hire (27.75% leave in 90 days)
- supervisor_attrition_rate
*/