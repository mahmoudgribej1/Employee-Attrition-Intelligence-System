{{
    config(
        materialized='table',
        schema='silver',
        tags=['silver', 'ibm_hr']
    )
}}

/*
    Silver Layer — IBM HR Employee Attrition Dataset
    
    Transformation logic:
    1. Drop constant columns: EmployeeCount, Over18, StandardHours
    2. Drop ID column: EmployeeNumber (not a feature)
    3. Standardize column names to snake_case
    4. Convert Yes/No text to binary integers
    5. Preserve all other columns as-is for Gold layer feature engineering
    
    Source: workspace.bronze.ibm_hr_employee_attrition
    Rows: 1,470 (no filtering)
    Columns: 31 (dropped 4 from original 35)
*/

WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'ibm_hr_employee_attrition') }}
)

SELECT
    -- Target variable
    CASE WHEN Attrition = 'Yes' THEN 1 ELSE 0 END AS attrition,
    
    -- Demographics
    Age AS age,
    CASE WHEN Gender = 'Male' THEN 1 ELSE 0 END AS gender,
    MaritalStatus AS marital_status,
    DistanceFromHome AS distance_from_home,
    
    -- Job characteristics
    Department AS department,
    JobRole AS job_role,
    JobLevel AS job_level,
    CASE WHEN OverTime = 'Yes' THEN 1 ELSE 0 END AS overtime,
    CASE 
        WHEN BusinessTravel = 'Non-Travel' THEN 0
        WHEN BusinessTravel = 'Travel_Rarely' THEN 1
        WHEN BusinessTravel = 'Travel_Frequently' THEN 2
    END AS business_travel_encoded,
    BusinessTravel AS business_travel,
    
    -- Education
    Education AS education,
    EducationField AS education_field,
    
    -- Compensation
    MonthlyIncome AS monthly_income,
    MonthlyRate AS monthly_rate,
    DailyRate AS daily_rate,
    HourlyRate AS hourly_rate,
    PercentSalaryHike AS percent_salary_hike,
    StockOptionLevel AS stock_option_level,
    
    -- Tenure & experience
    TotalWorkingYears AS total_working_years,
    YearsAtCompany AS years_at_company,
    YearsInCurrentRole AS years_in_current_role,
    YearsSinceLastPromotion AS years_since_last_promotion,
    YearsWithCurrManager AS years_with_curr_manager,
    NumCompaniesWorked AS num_companies_worked,
    
    -- Satisfaction & engagement (ordinal scales 1-4)
    EnvironmentSatisfaction AS environment_satisfaction,
    JobSatisfaction AS job_satisfaction,
    RelationshipSatisfaction AS relationship_satisfaction,
    WorkLifeBalance AS work_life_balance,
    JobInvolvement AS job_involvement,
    
    -- Performance
    PerformanceRating AS performance_rating,
    TrainingTimesLastYear AS training_times_last_year

FROM source_data

-- Data quality check: ensure we haven't lost rows
-- Expected: 1,470 rows
