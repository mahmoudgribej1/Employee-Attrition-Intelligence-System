{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'ml', 'features']
    )
}}

/*
    Gold Layer — ML Feature Store
    
    Purpose: Production-ready feature table for scikit-learn model training.
    
    Feature engineering:
    1. Engineered ratio features (income_per_year, promo_stagnation, career_ratio)
    2. Satisfaction composite scores
    3. One-hot encoding for high-cardinality categoricals
    4. Department-level aggregates (NO attrition rates - prevents leakage)
    5. Early career / tenure band flags
    
    Anti-leakage rules:
    ✅ Do NOT compute features FROM the target (attrition)
    ✅ Do NOT use COALESCE with target-dependent logic
    ✅ Do NOT freeze tenure at exit date (use as-is from silver)
    
    Expected output: 1,470 rows × ~80 columns (with one-hot encoding)
*/

WITH silver AS (
    SELECT * FROM {{ ref('silver_ibm_hr_cleaned') }}
),

-- Engineered numeric features
engineered_features AS (
    SELECT
        *,
        
        -- Income efficiency ratios
        monthly_income / NULLIF(total_working_years + 1, 0) AS income_per_year,
        monthly_income / NULLIF(age, 0) AS income_per_age,
        monthly_income / NULLIF(years_at_company + 1, 0) AS income_per_company_year,
        
        -- Tenure & promotion patterns
        years_since_last_promotion / NULLIF(years_at_company + 1, 0) AS promo_stagnation,
        years_in_current_role / NULLIF(years_at_company + 1, 0) AS role_tenure_ratio,
        years_with_curr_manager / NULLIF(years_at_company + 1, 0) AS manager_stability,
        
        -- Career trajectory
        total_working_years / NULLIF(age, 0) AS career_ratio,
        years_at_company / NULLIF(total_working_years + 1, 0) AS company_loyalty_ratio,
        
        -- Satisfaction composite scores (average of 1-4 scales)
        (environment_satisfaction + job_satisfaction + relationship_satisfaction) / 3.0 AS avg_satisfaction,
        (environment_satisfaction + job_satisfaction + relationship_satisfaction + work_life_balance) / 4.0 AS overall_wellbeing,
        
        -- Binary flags for risk segments
        CASE WHEN total_working_years <= 3 THEN 1 ELSE 0 END AS is_early_career,
        CASE WHEN years_at_company <= 2 THEN 1 ELSE 0 END AS is_new_hire,
        CASE WHEN years_since_last_promotion >= 5 THEN 1 ELSE 0 END AS long_time_since_promo,
        CASE WHEN age <= 30 THEN 1 ELSE 0 END AS is_young_employee,
        CASE WHEN job_level = 1 THEN 1 ELSE 0 END AS is_entry_level,
        CASE WHEN stock_option_level = 0 THEN 1 ELSE 0 END AS no_stock_options,
        
        -- Satisfaction flags (low = 1 or 2)
        CASE WHEN environment_satisfaction <= 2 THEN 1 ELSE 0 END AS low_env_satisfaction,
        CASE WHEN job_satisfaction <= 2 THEN 1 ELSE 0 END AS low_job_satisfaction,
        CASE WHEN work_life_balance <= 2 THEN 1 ELSE 0 END AS poor_work_life_balance,
        
        -- Interaction features (key combos from EDA)
        overtime * (CASE WHEN job_role = 'Sales Representative' THEN 1 ELSE 0 END) AS overtime_sales_rep,
        overtime * (CASE WHEN marital_status = 'Single' THEN 1 ELSE 0 END) AS overtime_single,
        (CASE WHEN job_level = 1 THEN 1 ELSE 0 END) * (CASE WHEN stock_option_level = 0 THEN 1 ELSE 0 END) AS entry_no_equity,
        distance_from_home * overtime AS distance_overtime_interaction
        
    FROM silver
),

-- Department-level aggregates (NO attrition rates — prevents leakage)
dept_aggregates AS (
    SELECT
        department,
        AVG(monthly_income) AS dept_avg_income,
        AVG(age) AS dept_avg_age,
        AVG(total_working_years) AS dept_avg_experience,
        COUNT(*) AS dept_headcount,
        AVG(overtime) AS dept_overtime_rate
    FROM silver
    GROUP BY department
),

-- Job role aggregates
role_aggregates AS (
    SELECT
        job_role,
        AVG(monthly_income) AS role_avg_income,
        AVG(total_working_years) AS role_avg_experience,
        COUNT(*) AS role_headcount
    FROM silver
    GROUP BY job_role
),

-- Join aggregates back
features_with_aggregates AS (
    SELECT
        ef.*,
        
        -- Department context
        da.dept_avg_income,
        da.dept_avg_age,
        da.dept_avg_experience,
        da.dept_headcount,
        da.dept_overtime_rate,
        
        -- Role context
        ra.role_avg_income,
        ra.role_avg_experience,
        ra.role_headcount,
        
        -- Relative position within department
        ef.monthly_income / NULLIF(da.dept_avg_income, 0) AS income_vs_dept_avg,
        ef.age / NULLIF(da.dept_avg_age, 0) AS age_vs_dept_avg
        
    FROM engineered_features ef
    LEFT JOIN dept_aggregates da ON ef.department = da.department
    LEFT JOIN role_aggregates ra ON ef.job_role = ra.job_role
),

-- One-hot encoding for categorical variables
final_features AS (
    SELECT
        -- Target variable (ALWAYS keep first for ML split)
        attrition,
        
        -- All numeric features from silver (keep originals)
        age,
        distance_from_home,
        education,
        job_level,
        monthly_income,
        num_companies_worked,
        percent_salary_hike,
        stock_option_level,
        total_working_years,
        training_times_last_year,
        years_at_company,
        years_in_current_role,
        years_since_last_promotion,
        years_with_curr_manager,
        environment_satisfaction,
        job_satisfaction,
        job_involvement,
        performance_rating,
        relationship_satisfaction,
        work_life_balance,
        
        -- Binary features from silver
        gender,
        overtime,
        business_travel_encoded,
        
        -- Engineered numeric features
        income_per_year,
        income_per_age,
        income_per_company_year,
        promo_stagnation,
        role_tenure_ratio,
        manager_stability,
        career_ratio,
        company_loyalty_ratio,
        avg_satisfaction,
        overall_wellbeing,
        
        -- Binary flags
        is_early_career,
        is_new_hire,
        long_time_since_promo,
        is_young_employee,
        is_entry_level,
        no_stock_options,
        low_env_satisfaction,
        low_job_satisfaction,
        poor_work_life_balance,
        
        -- Interaction features
        overtime_sales_rep,
        overtime_single,
        entry_no_equity,
        distance_overtime_interaction,
        
        -- Department/role aggregates
        dept_avg_income,
        dept_avg_age,
        dept_avg_experience,
        dept_headcount,
        dept_overtime_rate,
        role_avg_income,
        role_avg_experience,
        role_headcount,
        income_vs_dept_avg,
        age_vs_dept_avg,
        
        -- One-hot: Department
        CASE WHEN department = 'Sales' THEN 1 ELSE 0 END AS dept_sales,
        CASE WHEN department = 'Research & Development' THEN 1 ELSE 0 END AS dept_research_dev,
        CASE WHEN department = 'Human Resources' THEN 1 ELSE 0 END AS dept_hr,
        
        -- One-hot: Job Role  
        CASE WHEN job_role = 'Sales Executive' THEN 1 ELSE 0 END AS role_sales_executive,
        CASE WHEN job_role = 'Research Scientist' THEN 1 ELSE 0 END AS role_research_scientist,
        CASE WHEN job_role = 'Laboratory Technician' THEN 1 ELSE 0 END AS role_lab_technician,
        CASE WHEN job_role = 'Manufacturing Director' THEN 1 ELSE 0 END AS role_manufacturing_director,
        CASE WHEN job_role = 'Healthcare Representative' THEN 1 ELSE 0 END AS role_healthcare_rep,
        CASE WHEN job_role = 'Manager' THEN 1 ELSE 0 END AS role_manager,
        CASE WHEN job_role = 'Sales Representative' THEN 1 ELSE 0 END AS role_sales_rep,
        CASE WHEN job_role = 'Research Director' THEN 1 ELSE 0 END AS role_research_director,
        CASE WHEN job_role = 'Human Resources' THEN 1 ELSE 0 END AS role_hr,
        
        -- One-hot: Marital Status
        CASE WHEN marital_status = 'Single' THEN 1 ELSE 0 END AS marital_single,
        CASE WHEN marital_status = 'Married' THEN 1 ELSE 0 END AS marital_married,
        CASE WHEN marital_status = 'Divorced' THEN 1 ELSE 0 END AS marital_divorced,
        
        -- One-hot: Education Field
        CASE WHEN education_field = 'Life Sciences' THEN 1 ELSE 0 END AS edu_life_sciences,
        CASE WHEN education_field = 'Medical' THEN 1 ELSE 0 END AS edu_medical,
        CASE WHEN education_field = 'Marketing' THEN 1 ELSE 0 END AS edu_marketing,
        CASE WHEN education_field = 'Technical Degree' THEN 1 ELSE 0 END AS edu_technical,
        CASE WHEN education_field = 'Other' THEN 1 ELSE 0 END AS edu_other,
        CASE WHEN education_field = 'Human Resources' THEN 1 ELSE 0 END AS edu_hr
        
        -- Note: Drop monthly_rate, daily_rate, hourly_rate (EDA showed ~0.00 correlation — random noise)
        
    FROM features_with_aggregates
)

SELECT * FROM final_features

-- Expected output: 1,470 rows × ~80 features
-- Target distribution: ~16% attrition (237 Yes, 1,233 No)
