{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'star_schema', 'fact']
    )
}}

/*
    Fact Table: Employee Attrition
    Grain: 1 row per employee (1,470 rows)
    
    Star schema design for Power BI:
    - Surrogate keys link to 5 dimension tables
    - All numeric measures preserved for flexible aggregation
    - Human-readable labels included for quick tooltips
    - Derived bands (age, tenure, income) for slicer convenience
    
    Dimension FKs:
        → dim_department.department_key
        → dim_job_role.job_role_key
        → dim_education_field.education_field_key
        → dim_marital_status.marital_status_key
        → dim_business_travel.business_travel_key
*/

WITH silver AS (
    SELECT * FROM {{ ref('silver_ibm_hr_cleaned') }}
),

dept AS (
    SELECT department_key, department_name
    FROM {{ ref('dim_department') }}
),

role AS (
    SELECT job_role_key, job_role_name
    FROM {{ ref('dim_job_role') }}
),

edu AS (
    SELECT education_field_key, education_field_name
    FROM {{ ref('dim_education_field') }}
),

marital AS (
    SELECT marital_status_key, marital_status_name
    FROM {{ ref('dim_marital_status') }}
),

travel AS (
    SELECT business_travel_key, business_travel_name
    FROM {{ ref('dim_business_travel') }}
)

SELECT
    -- Surrogate key
    ROW_NUMBER() OVER (ORDER BY s.department, s.job_role, s.age, s.monthly_income) AS employee_key,

    -- ===== Dimension foreign keys =====
    d.department_key,
    r.job_role_key,
    e.education_field_key,
    m.marital_status_key,
    t.business_travel_key,

    -- ===== Target =====
    s.attrition,
    CASE WHEN s.attrition = 1 THEN 'Yes' ELSE 'No' END AS attrition_label,

    -- ===== Demographics =====
    s.age,
    CASE
        WHEN s.age < 25 THEN 'Under 25'
        WHEN s.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN s.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN s.age BETWEEN 45 AND 54 THEN '45-54'
        ELSE '55+'
    END AS age_band,
    s.gender,
    CASE WHEN s.gender = 1 THEN 'Male' ELSE 'Female' END AS gender_label,
    s.distance_from_home,

    -- ===== Job characteristics =====
    s.department,
    s.job_role,
    s.job_level,
    s.overtime,
    CASE WHEN s.overtime = 1 THEN 'Yes' ELSE 'No' END AS overtime_label,
    s.business_travel,
    s.business_travel_encoded,

    -- ===== Education =====
    s.education,
    CASE
        WHEN s.education = 1 THEN 'Below College'
        WHEN s.education = 2 THEN 'College'
        WHEN s.education = 3 THEN 'Bachelor'
        WHEN s.education = 4 THEN 'Master'
        WHEN s.education = 5 THEN 'Doctor'
    END AS education_label,
    s.education_field,

    -- ===== Compensation =====
    s.monthly_income,
    CASE
        WHEN s.monthly_income <= 2911 THEN 'Q1 (≤2,911)'
        WHEN s.monthly_income <= 4919 THEN 'Q2 (2,912-4,919)'
        WHEN s.monthly_income <= 8379 THEN 'Q3 (4,920-8,379)'
        ELSE 'Q4 (>8,379)'
    END AS income_quartile,
    s.monthly_rate,
    s.daily_rate,
    s.hourly_rate,
    s.percent_salary_hike,
    s.stock_option_level,

    -- ===== Tenure & experience =====
    s.total_working_years,
    s.years_at_company,
    CASE
        WHEN s.years_at_company <= 2 THEN '0-2 years'
        WHEN s.years_at_company <= 5 THEN '3-5 years'
        WHEN s.years_at_company <= 10 THEN '6-10 years'
        ELSE '10+ years'
    END AS tenure_band,
    s.years_in_current_role,
    s.years_since_last_promotion,
    s.years_with_curr_manager,
    s.num_companies_worked,

    -- ===== Satisfaction & engagement (1-4 scale) =====
    s.environment_satisfaction,
    s.job_satisfaction,
    s.relationship_satisfaction,
    s.work_life_balance,
    s.job_involvement,

    -- ===== Performance =====
    s.performance_rating,
    s.training_times_last_year,

    -- ===== Derived measures for Power BI =====
    ROUND(s.monthly_income / NULLIF(s.total_working_years, 0), 2) AS income_per_experience_year,
    ROUND(s.years_at_company * 1.0 / NULLIF(s.total_working_years, 0), 4) AS company_loyalty_ratio,
    s.years_at_company - s.years_in_current_role AS years_before_current_role

FROM silver s
LEFT JOIN dept d ON s.department = d.department_name
LEFT JOIN role r ON s.job_role = r.job_role_name
LEFT JOIN edu e ON s.education_field = e.education_field_name
LEFT JOIN marital m ON s.marital_status = m.marital_status_name
LEFT JOIN travel t ON s.business_travel = t.business_travel_name
