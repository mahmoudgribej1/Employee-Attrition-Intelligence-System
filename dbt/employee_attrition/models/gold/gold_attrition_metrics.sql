{{
    config(
        materialized='table',
        file_format='delta',
        tags=['gold', 'attrition', 'business_metrics']
    )
}}

/*
================================================================================
GOLD ATTRITION METRICS
================================================================================
Purpose: Comprehensive attrition analysis by department, demographics, tenure

Metrics Included:
  - Attrition rates by department, business unit, demographics
  - Voluntary vs involuntary breakdown
  - Tenure analysis (who leaves when)
  - Performance correlation with attrition
  - Cost of attrition calculations

Dashboard Use Cases:
  - Executive attrition overview
  - Department comparison
  - Demographic equity analysis
  - Retention trends

Created: {{ run_started_at.strftime('%Y-%m-%d') }}
================================================================================
*/

WITH employee_base AS (
    SELECT
        EmpID,
        is_active,
        employee_status,
        DepartmentType,
        BusinessUnit,
        Division,
        Title,
        Supervisor,
        tenure_months,
        tenure_years,
        tenure_category,
        age,
        age_group,
        GenderCode,
        RaceDesc,
        MaritalDesc,
        performance_score,
        current_employee_rating,
        termination_type_clean,
        PayZone,
        EmployeeType,
        hire_year,
        exit_year,
        exit_month
    FROM {{ ref('silver_employee_cleaned') }}
),

-- Overall company metrics
overall_metrics AS (
    SELECT
        'Company-Wide' AS metric_level,
        'All Employees' AS metric_segment,
        
        COUNT(*) AS total_employees,
        SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_employees,
        SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) AS departed_employees,
        
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS attrition_rate,
        
        -- Voluntary vs involuntary
        SUM(CASE WHEN termination_type_clean = 'Voluntary' THEN 1 ELSE 0 END) AS voluntary_departures,
        SUM(CASE WHEN termination_type_clean = 'Involuntary' THEN 1 ELSE 0 END) AS involuntary_departures,
        
        ROUND(SUM(CASE WHEN termination_type_clean = 'Voluntary' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END), 0), 2) AS voluntary_percentage,
        
        -- Tenure metrics
        ROUND(AVG(CASE WHEN is_active = 1 THEN tenure_years ELSE NULL END), 1) AS avg_tenure_active,
        ROUND(AVG(CASE WHEN is_active = 0 THEN tenure_years ELSE NULL END), 1) AS avg_tenure_departed,
        
        -- Cost estimate (6-9 months salary, using 7.5 average)
        -- Assuming average salary of $75,000 for calculation
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 75000 * 0.625, 2) AS estimated_attrition_cost,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM employee_base
),

-- Department metrics
department_metrics AS (
    SELECT
        'Department' AS metric_level,
        DepartmentType AS metric_segment,
        
        COUNT(*) AS total_employees,
        SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_employees,
        SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) AS departed_employees,
        
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS attrition_rate,
        
        SUM(CASE WHEN termination_type_clean = 'Voluntary' THEN 1 ELSE 0 END) AS voluntary_departures,
        SUM(CASE WHEN termination_type_clean = 'Involuntary' THEN 1 ELSE 0 END) AS involuntary_departures,
        
        ROUND(SUM(CASE WHEN termination_type_clean = 'Voluntary' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END), 0), 2) AS voluntary_percentage,
        
        ROUND(AVG(CASE WHEN is_active = 1 THEN tenure_years ELSE NULL END), 1) AS avg_tenure_active,
        ROUND(AVG(CASE WHEN is_active = 0 THEN tenure_years ELSE NULL END), 1) AS avg_tenure_departed,
        
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 75000 * 0.625, 2) AS estimated_attrition_cost,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM employee_base
    WHERE DepartmentType IS NOT NULL
    GROUP BY DepartmentType
),

-- Business unit metrics
business_unit_metrics AS (
    SELECT
        'Business Unit' AS metric_level,
        BusinessUnit AS metric_segment,
        
        COUNT(*) AS total_employees,
        SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_employees,
        SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) AS departed_employees,
        
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS attrition_rate,
        
        SUM(CASE WHEN termination_type_clean = 'Voluntary' THEN 1 ELSE 0 END) AS voluntary_departures,
        SUM(CASE WHEN termination_type_clean = 'Involuntary' THEN 1 ELSE 0 END) AS involuntary_departures,
        
        ROUND(SUM(CASE WHEN termination_type_clean = 'Voluntary' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END), 0), 2) AS voluntary_percentage,
        
        ROUND(AVG(CASE WHEN is_active = 1 THEN tenure_years ELSE NULL END), 1) AS avg_tenure_active,
        ROUND(AVG(CASE WHEN is_active = 0 THEN tenure_years ELSE NULL END), 1) AS avg_tenure_departed,
        
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 75000 * 0.625, 2) AS estimated_attrition_cost,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM employee_base
    WHERE BusinessUnit IS NOT NULL
    GROUP BY BusinessUnit
),

-- Age group metrics
age_group_metrics AS (
    SELECT
        'Age Group' AS metric_level,
        age_group AS metric_segment,
        
        COUNT(*) AS total_employees,
        SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_employees,
        SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) AS departed_employees,
        
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS attrition_rate,
        
        SUM(CASE WHEN termination_type_clean = 'Voluntary' THEN 1 ELSE 0 END) AS voluntary_departures,
        SUM(CASE WHEN termination_type_clean = 'Involuntary' THEN 1 ELSE 0 END) AS involuntary_departures,
        
        ROUND(SUM(CASE WHEN termination_type_clean = 'Voluntary' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END), 0), 2) AS voluntary_percentage,
        
        ROUND(AVG(CASE WHEN is_active = 1 THEN tenure_years ELSE NULL END), 1) AS avg_tenure_active,
        ROUND(AVG(CASE WHEN is_active = 0 THEN tenure_years ELSE NULL END), 1) AS avg_tenure_departed,
        
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 75000 * 0.625, 2) AS estimated_attrition_cost,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM employee_base
    WHERE age_group IS NOT NULL
    GROUP BY age_group
),

-- Gender metrics
gender_metrics AS (
    SELECT
        'Gender' AS metric_level,
        GenderCode AS metric_segment,
        
        COUNT(*) AS total_employees,
        SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_employees,
        SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) AS departed_employees,
        
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS attrition_rate,
        
        SUM(CASE WHEN termination_type_clean = 'Voluntary' THEN 1 ELSE 0 END) AS voluntary_departures,
        SUM(CASE WHEN termination_type_clean = 'Involuntary' THEN 1 ELSE 0 END) AS involuntary_departures,
        
        ROUND(SUM(CASE WHEN termination_type_clean = 'Voluntary' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END), 0), 2) AS voluntary_percentage,
        
        ROUND(AVG(CASE WHEN is_active = 1 THEN tenure_years ELSE NULL END), 1) AS avg_tenure_active,
        ROUND(AVG(CASE WHEN is_active = 0 THEN tenure_years ELSE NULL END), 1) AS avg_tenure_departed,
        
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 75000 * 0.625, 2) AS estimated_attrition_cost,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM employee_base
    WHERE GenderCode IS NOT NULL
    GROUP BY GenderCode
),

-- Tenure category metrics
tenure_metrics AS (
    SELECT
        'Tenure Category' AS metric_level,
        tenure_category AS metric_segment,
        
        COUNT(*) AS total_employees,
        SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_employees,
        SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) AS departed_employees,
        
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS attrition_rate,
        
        SUM(CASE WHEN termination_type_clean = 'Voluntary' THEN 1 ELSE 0 END) AS voluntary_departures,
        SUM(CASE WHEN termination_type_clean = 'Involuntary' THEN 1 ELSE 0 END) AS involuntary_departures,
        
        ROUND(SUM(CASE WHEN termination_type_clean = 'Voluntary' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END), 0), 2) AS voluntary_percentage,
        
        ROUND(AVG(CASE WHEN is_active = 1 THEN tenure_years ELSE NULL END), 1) AS avg_tenure_active,
        ROUND(AVG(CASE WHEN is_active = 0 THEN tenure_years ELSE NULL END), 1) AS avg_tenure_departed,
        
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 75000 * 0.625, 2) AS estimated_attrition_cost,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM employee_base
    WHERE tenure_category IS NOT NULL
    GROUP BY tenure_category
),

-- Performance score metrics
performance_metrics AS (
    SELECT
        'Performance Score' AS metric_level,
        performance_score AS metric_segment,
        
        COUNT(*) AS total_employees,
        SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_employees,
        SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) AS departed_employees,
        
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS attrition_rate,
        
        SUM(CASE WHEN termination_type_clean = 'Voluntary' THEN 1 ELSE 0 END) AS voluntary_departures,
        SUM(CASE WHEN termination_type_clean = 'Involuntary' THEN 1 ELSE 0 END) AS involuntary_departures,
        
        ROUND(SUM(CASE WHEN termination_type_clean = 'Voluntary' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END), 0), 2) AS voluntary_percentage,
        
        ROUND(AVG(CASE WHEN is_active = 1 THEN tenure_years ELSE NULL END), 1) AS avg_tenure_active,
        ROUND(AVG(CASE WHEN is_active = 0 THEN tenure_years ELSE NULL END), 1) AS avg_tenure_departed,
        
        ROUND(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 75000 * 0.625, 2) AS estimated_attrition_cost,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM employee_base
    WHERE performance_score IS NOT NULL
    GROUP BY performance_score
),

-- Combine all metrics
combined_metrics AS (
    SELECT * FROM overall_metrics
    UNION ALL
    SELECT * FROM department_metrics
    UNION ALL
    SELECT * FROM business_unit_metrics
    UNION ALL
    SELECT * FROM age_group_metrics
    UNION ALL
    SELECT * FROM gender_metrics
    UNION ALL
    SELECT * FROM tenure_metrics
    UNION ALL
    SELECT * FROM performance_metrics
)

SELECT
    metric_level,
    metric_segment,
    
    -- Employee counts
    total_employees,
    active_employees,
    departed_employees,
    
    -- Attrition metrics
    attrition_rate,
    voluntary_departures,
    involuntary_departures,
    voluntary_percentage,
    
    -- Tenure comparison
    avg_tenure_active,
    avg_tenure_departed,
    
    -- Financial impact
    estimated_attrition_cost,
    
    -- Risk categorization
    CASE 
        WHEN attrition_rate >= 60 THEN 'Critical'
        WHEN attrition_rate >= 40 THEN 'High Risk'
        WHEN attrition_rate >= 20 THEN 'Moderate'
        ELSE 'Healthy'
    END AS attrition_risk_level,
    
    -- Metadata
    calculated_at,
    CURRENT_TIMESTAMP() AS last_updated
    
FROM combined_metrics
ORDER BY 
    CASE metric_level
        WHEN 'Company-Wide' THEN 1
        WHEN 'Department' THEN 2
        WHEN 'Business Unit' THEN 3
        WHEN 'Age Group' THEN 4
        WHEN 'Gender' THEN 5
        WHEN 'Tenure Category' THEN 6
        WHEN 'Performance Score' THEN 7
    END,
    attrition_rate DESC

/*
POWER BI USAGE:
- Executive dashboard: Filter metric_level = 'Company-Wide'
- Department comparison: Filter metric_level = 'Department'
- Drill through from high-level to specific segments
- Color code by attrition_risk_level
- Show estimated_attrition_cost as financial KPI
*/