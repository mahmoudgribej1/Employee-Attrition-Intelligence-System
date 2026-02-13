{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'analytics', 'powerbi']
    )
}}

/*
    Gold Layer — Attrition Analytics for Power BI
    
    Purpose: Pre-aggregated attrition metrics by key dimensions.
    Human-readable labels, not encoded. Powers dashboard slicers and visuals.
    
    Output: Long-format table with one row per dimension-value combination.
    Columns: dimension_name, dimension_value, total_employees, attrition_count, attrition_rate
    
    Note: This table INCLUDES attrition rates because it's for post-hoc analysis,
    not for training features. It's safe to compute rates here because this table
    is not fed back into the ML model.
*/

WITH silver AS (
    SELECT * FROM {{ ref('silver_ibm_hr_cleaned') }}
),

-- UNION ALL different dimension cuts into a single long table

-- 1. Overall
overall AS (
    SELECT
        'Overall' AS dimension_name,
        'All Employees' AS dimension_value,
        COUNT(*) AS total_employees,
        SUM(attrition) AS attrition_count,
        AVG(attrition) * 100 AS attrition_rate,
        AVG(age) AS avg_age,
        AVG(monthly_income) AS avg_income,
        AVG(total_working_years) AS avg_experience,
        AVG(years_at_company) AS avg_tenure
    FROM silver
),

-- 2. By Department
by_department AS (
    SELECT
        'Department' AS dimension_name,
        department AS dimension_value,
        COUNT(*) AS total_employees,
        SUM(attrition) AS attrition_count,
        AVG(attrition) * 100 AS attrition_rate,
        AVG(age) AS avg_age,
        AVG(monthly_income) AS avg_income,
        AVG(total_working_years) AS avg_experience,
        AVG(years_at_company) AS avg_tenure
    FROM silver
    GROUP BY department
),

-- 3. By Job Role
by_job_role AS (
    SELECT
        'Job Role' AS dimension_name,
        job_role AS dimension_value,
        COUNT(*) AS total_employees,
        SUM(attrition) AS attrition_count,
        AVG(attrition) * 100 AS attrition_rate,
        AVG(age) AS avg_age,
        AVG(monthly_income) AS avg_income,
        AVG(total_working_years) AS avg_experience,
        AVG(years_at_company) AS avg_tenure
    FROM silver
    GROUP BY job_role
),

-- 4. By OverTime
by_overtime AS (
    SELECT
        'OverTime' AS dimension_name,
        CASE WHEN overtime = 1 THEN 'Yes' ELSE 'No' END AS dimension_value,
        COUNT(*) AS total_employees,
        SUM(attrition) AS attrition_count,
        AVG(attrition) * 100 AS attrition_rate,
        AVG(age) AS avg_age,
        AVG(monthly_income) AS avg_income,
        AVG(total_working_years) AS avg_experience,
        AVG(years_at_company) AS avg_tenure
    FROM silver
    GROUP BY overtime
),

-- 5. By Marital Status
by_marital AS (
    SELECT
        'Marital Status' AS dimension_name,
        marital_status AS dimension_value,
        COUNT(*) AS total_employees,
        SUM(attrition) AS attrition_count,
        AVG(attrition) * 100 AS attrition_rate,
        AVG(age) AS avg_age,
        AVG(monthly_income) AS avg_income,
        AVG(total_working_years) AS avg_experience,
        AVG(years_at_company) AS avg_tenure
    FROM silver
    GROUP BY marital_status
),

-- 6. By Gender
by_gender AS (
    SELECT
        'Gender' AS dimension_name,
        CASE WHEN gender = 1 THEN 'Male' ELSE 'Female' END AS dimension_value,
        COUNT(*) AS total_employees,
        SUM(attrition) AS attrition_count,
        AVG(attrition) * 100 AS attrition_rate,
        AVG(age) AS avg_age,
        AVG(monthly_income) AS avg_income,
        AVG(total_working_years) AS avg_experience,
        AVG(years_at_company) AS avg_tenure
    FROM silver
    GROUP BY gender
),

-- 7. By Job Level
by_job_level AS (
    SELECT
        'Job Level' AS dimension_name,
        CAST(job_level AS STRING) AS dimension_value,
        COUNT(*) AS total_employees,
        SUM(attrition) AS attrition_count,
        AVG(attrition) * 100 AS attrition_rate,
        AVG(age) AS avg_age,
        AVG(monthly_income) AS avg_income,
        AVG(total_working_years) AS avg_experience,
        AVG(years_at_company) AS avg_tenure
    FROM silver
    GROUP BY job_level
),

-- 8. By Age Band
by_age_band AS (
    SELECT
        'Age Band' AS dimension_name,
        CASE
            WHEN age < 25 THEN 'Under 25'
            WHEN age BETWEEN 25 AND 34 THEN '25-34'
            WHEN age BETWEEN 35 AND 44 THEN '35-44'
            WHEN age BETWEEN 45 AND 54 THEN '45-54'
            ELSE '55+'
        END AS dimension_value,
        COUNT(*) AS total_employees,
        SUM(attrition) AS attrition_count,
        AVG(attrition) * 100 AS attrition_rate,
        AVG(age) AS avg_age,
        AVG(monthly_income) AS avg_income,
        AVG(total_working_years) AS avg_experience,
        AVG(years_at_company) AS avg_tenure
    FROM silver
    GROUP BY 
        CASE
            WHEN age < 25 THEN 'Under 25'
            WHEN age BETWEEN 25 AND 34 THEN '25-34'
            WHEN age BETWEEN 35 AND 44 THEN '35-44'
            WHEN age BETWEEN 45 AND 54 THEN '45-54'
            ELSE '55+'
        END
),

-- 9. By Tenure Band
by_tenure_band AS (
    SELECT
        'Tenure Band' AS dimension_name,
        CASE
            WHEN years_at_company < 1 THEN 'Less than 1 year'
            WHEN years_at_company BETWEEN 1 AND 2 THEN '1-2 years'
            WHEN years_at_company BETWEEN 3 AND 5 THEN '3-5 years'
            WHEN years_at_company BETWEEN 6 AND 10 THEN '6-10 years'
            ELSE '10+ years'
        END AS dimension_value,
        COUNT(*) AS total_employees,
        SUM(attrition) AS attrition_count,
        AVG(attrition) * 100 AS attrition_rate,
        AVG(age) AS avg_age,
        AVG(monthly_income) AS avg_income,
        AVG(total_working_years) AS avg_experience,
        AVG(years_at_company) AS avg_tenure
    FROM silver
    GROUP BY 
        CASE
            WHEN years_at_company < 1 THEN 'Less than 1 year'
            WHEN years_at_company BETWEEN 1 AND 2 THEN '1-2 years'
            WHEN years_at_company BETWEEN 3 AND 5 THEN '3-5 years'
            WHEN years_at_company BETWEEN 6 AND 10 THEN '6-10 years'
            ELSE '10+ years'
        END
),

-- 10. By Income Quartile
by_income_quartile AS (
    SELECT
        'Income Quartile' AS dimension_name,
        CASE
            WHEN monthly_income <= 2911 THEN 'Q1 (Low)'
            WHEN monthly_income <= 4919 THEN 'Q2 (Medium-Low)'
            WHEN monthly_income <= 8379 THEN 'Q3 (Medium-High)'
            ELSE 'Q4 (High)'
        END AS dimension_value,
        COUNT(*) AS total_employees,
        SUM(attrition) AS attrition_count,
        AVG(attrition) * 100 AS attrition_rate,
        AVG(age) AS avg_age,
        AVG(monthly_income) AS avg_income,
        AVG(total_working_years) AS avg_experience,
        AVG(years_at_company) AS avg_tenure
    FROM silver
    GROUP BY 
        CASE
            WHEN monthly_income <= 2911 THEN 'Q1 (Low)'
            WHEN monthly_income <= 4919 THEN 'Q2 (Medium-Low)'
            WHEN monthly_income <= 8379 THEN 'Q3 (Medium-High)'
            ELSE 'Q4 (High)'
        END
),

-- 11. By Business Travel
by_business_travel AS (
    SELECT
        'Business Travel' AS dimension_name,
        business_travel AS dimension_value,
        COUNT(*) AS total_employees,
        SUM(attrition) AS attrition_count,
        AVG(attrition) * 100 AS attrition_rate,
        AVG(age) AS avg_age,
        AVG(monthly_income) AS avg_income,
        AVG(total_working_years) AS avg_experience,
        AVG(years_at_company) AS avg_tenure
    FROM silver
    GROUP BY business_travel
),

-- 12. By Education Level
by_education AS (
    SELECT
        'Education Level' AS dimension_name,
        CASE education
            WHEN 1 THEN 'Below College'
            WHEN 2 THEN 'College'
            WHEN 3 THEN 'Bachelor'
            WHEN 4 THEN 'Master'
            WHEN 5 THEN 'Doctor'
        END AS dimension_value,
        COUNT(*) AS total_employees,
        SUM(attrition) AS attrition_count,
        AVG(attrition) * 100 AS attrition_rate,
        AVG(age) AS avg_age,
        AVG(monthly_income) AS avg_income,
        AVG(total_working_years) AS avg_experience,
        AVG(years_at_company) AS avg_tenure
    FROM silver
    GROUP BY education
),

-- 13. By Stock Option Level
by_stock_option AS (
    SELECT
        'Stock Option Level' AS dimension_name,
        CAST(stock_option_level AS STRING) AS dimension_value,
        COUNT(*) AS total_employees,
        SUM(attrition) AS attrition_count,
        AVG(attrition) * 100 AS attrition_rate,
        AVG(age) AS avg_age,
        AVG(monthly_income) AS avg_income,
        AVG(total_working_years) AS avg_experience,
        AVG(years_at_company) AS avg_tenure
    FROM silver
    GROUP BY stock_option_level
),

-- 14. By Distance From Home Band
by_distance AS (
    SELECT
        'Distance From Home' AS dimension_name,
        CASE
            WHEN distance_from_home < 5 THEN 'Very Close (<5 mi)'
            WHEN distance_from_home BETWEEN 5 AND 10 THEN 'Close (5-10 mi)'
            WHEN distance_from_home BETWEEN 11 AND 20 THEN 'Moderate (11-20 mi)'
            ELSE 'Far (20+ mi)'
        END AS dimension_value,
        COUNT(*) AS total_employees,
        SUM(attrition) AS attrition_count,
        AVG(attrition) * 100 AS attrition_rate,
        AVG(age) AS avg_age,
        AVG(monthly_income) AS avg_income,
        AVG(total_working_years) AS avg_experience,
        AVG(years_at_company) AS avg_tenure
    FROM silver
    GROUP BY 
        CASE
            WHEN distance_from_home < 5 THEN 'Very Close (<5 mi)'
            WHEN distance_from_home BETWEEN 5 AND 10 THEN 'Close (5-10 mi)'
            WHEN distance_from_home BETWEEN 11 AND 20 THEN 'Moderate (11-20 mi)'
            ELSE 'Far (20+ mi)'
        END
)

-- Union all dimension cuts
SELECT * FROM overall
UNION ALL SELECT * FROM by_department
UNION ALL SELECT * FROM by_job_role
UNION ALL SELECT * FROM by_overtime
UNION ALL SELECT * FROM by_marital
UNION ALL SELECT * FROM by_gender
UNION ALL SELECT * FROM by_job_level
UNION ALL SELECT * FROM by_age_band
UNION ALL SELECT * FROM by_tenure_band
UNION ALL SELECT * FROM by_income_quartile
UNION ALL SELECT * FROM by_business_travel
UNION ALL SELECT * FROM by_education
UNION ALL SELECT * FROM by_stock_option
UNION ALL SELECT * FROM by_distance

-- Power BI will filter on dimension_name to create slicers and charts
-- Each row = one dashboard data point
