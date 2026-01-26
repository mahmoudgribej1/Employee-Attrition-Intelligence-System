{{
    config(
        materialized='table',
        file_format='delta',
        partition_by=['application_year'],
        tags=['silver', 'recruitment', 'cleaned']
    )
}}

/*
================================================================================
SILVER RECRUITMENT CLEANED
================================================================================
Purpose: Clean and validate recruitment/application data
Issues Fixed:
  - Standardize application status values
  - Clean and validate salary data
  - Validate years of experience (no negatives)
  - Convert date strings to proper DATE type
  - Clean contact information

Author: Data Engineering Team
Created: {{ run_started_at.strftime('%Y-%m-%d') }}
================================================================================
*/

WITH source_data AS (
    SELECT
        `Applicant_ID` AS applicant_id,
        `Application_Date` AS application_date_raw,
        `First_Name` AS first_name,
        `Last_Name` AS last_name,
        Gender,
        `Date_of_Birth` AS date_of_birth_raw,
        `Phone_Number` AS phone_number,
        Email,
        Address,
        City,
        State,
        `Zip_Code` AS zip_code,
        Country,
        `Education_Level` AS education_level,
        `Years_of_Experience` AS years_of_experience,
        `Desired_Salary` AS desired_salary,
        `Job_Title` AS job_title,
        Status
    FROM {{ source('bronze', 'recruitment_data') }}
),

cleaned_dates_and_contacts AS (
    SELECT
        -- Primary Key
        applicant_id,
        
        -- Dates - Convert to proper DATE type
        TO_DATE(application_date_raw, 'dd-MMM-yy') AS application_date,
        TO_DATE(date_of_birth_raw, 'yyyy-MM-dd') AS date_of_birth,
        YEAR(TO_DATE(application_date_raw, 'dd-MMM-yy')) AS application_year,
        MONTH(TO_DATE(application_date_raw, 'dd-MMM-yy')) AS application_month,
        
        -- Personal Information - Cleaned
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        UPPER(TRIM(Gender)) AS gender,
        
        -- Calculate age at application
        FLOOR(MONTHS_BETWEEN(
            TO_DATE(application_date_raw, 'dd-MMM-yy'),
            TO_DATE(date_of_birth_raw, 'yyyy-MM-dd')
        ) / 12) AS age_at_application,
        
        -- Contact Information - Cleaned
        LOWER(TRIM(Email)) AS email,
        REGEXP_REPLACE(phone_number, '[^0-9]', '') AS phone_number_clean,  -- Remove non-numeric
        TRIM(Address) AS address,
        TRIM(City) AS city,
        TRIM(State) AS state,
        zip_code,
        TRIM(Country) AS country,
        
        -- Job Application Details
        TRIM(education_level) AS education_level,
        years_of_experience,
        desired_salary,
        TRIM(job_title) AS job_title,
        Status,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS processed_at
        
    FROM source_data
),

validated_data AS (
    SELECT
        *,
        
        -- STANDARDIZE: Application status (trim and title case)
        INITCAP(TRIM(Status)) AS status_standardized,
        
        -- VALIDATE: Years of experience (no negatives, reasonable max of 50)
        CASE 
            WHEN years_of_experience < 0 THEN 0
            WHEN years_of_experience > 50 THEN 50  -- Cap at 50 years
            ELSE years_of_experience
        END AS years_of_experience_validated,
        
        -- VALIDATE: Desired salary (no negatives, reasonable range)
        CASE 
            WHEN desired_salary < 0 THEN NULL
            WHEN desired_salary > 1000000 THEN NULL  -- Flag unrealistic salaries
            ELSE desired_salary
        END AS desired_salary_validated,
        
        -- Data quality flags
        CASE WHEN years_of_experience < 0 OR years_of_experience > 50 THEN 1 ELSE 0 END AS had_invalid_experience,
        CASE WHEN desired_salary < 0 OR desired_salary > 1000000 THEN 1 ELSE 0 END AS had_invalid_salary,
        CASE WHEN Email IS NULL OR Email = '' OR NOT Email LIKE '%@%' THEN 1 ELSE 0 END AS has_invalid_email
        
    FROM cleaned_dates_and_contacts
),

final_transformations AS (
    SELECT
        *,
        
        -- Experience categorization
        CASE 
            WHEN years_of_experience_validated = 0 THEN 'Entry Level'
            WHEN years_of_experience_validated <= 2 THEN '0-2 years'
            WHEN years_of_experience_validated <= 5 THEN '3-5 years'
            WHEN years_of_experience_validated <= 10 THEN '6-10 years'
            ELSE '10+ years (Senior)'
        END AS experience_category,
        
        -- Education level standardized
        CASE 
            WHEN LOWER(education_level) LIKE '%high school%' THEN 'High School'
            WHEN LOWER(education_level) LIKE '%associate%' THEN 'Associate Degree'
            WHEN LOWER(education_level) LIKE '%bachelor%' THEN 'Bachelor Degree'
            WHEN LOWER(education_level) LIKE '%master%' THEN 'Master Degree'
            WHEN LOWER(education_level) LIKE '%phd%' OR LOWER(education_level) LIKE '%doctor%' THEN 'Doctorate'
            ELSE education_level
        END AS education_level_standardized,
        
        -- Hiring funnel stage
        CASE 
            WHEN status_standardized = 'Hired' THEN 'Converted'
            WHEN status_standardized = 'Rejected' THEN 'Not Converted'
            WHEN status_standardized IN ('Applied', 'Interviewed', 'Offered') THEN 'In Progress'
            ELSE 'Unknown'
        END AS hiring_stage,
        
        -- Salary expectation category (for analysis)
        CASE 
            WHEN desired_salary_validated < 50000 THEN 'Under 50K'
            WHEN desired_salary_validated < 75000 THEN '50K-75K'
            WHEN desired_salary_validated < 100000 THEN '75K-100K'
            WHEN desired_salary_validated < 150000 THEN '100K-150K'
            ELSE '150K+'
        END AS salary_range,
        
        CURRENT_TIMESTAMP() AS last_updated
        
    FROM validated_data
)

SELECT
    -- Primary Key
    applicant_id,
    
    -- Application Information
    application_date,
    application_year,
    application_month,
    status_standardized AS status,
    hiring_stage,
    
    -- Personal Information
    first_name,
    last_name,
    gender,
    date_of_birth,
    age_at_application,
    
    -- Contact Information
    email,
    phone_number_clean AS phone_number,
    address,
    city,
    state,
    zip_code,
    country,
    
    -- Qualifications (VALIDATED)
    education_level_standardized AS education_level,
    years_of_experience_validated AS years_of_experience,
    experience_category,
    
    -- Salary (VALIDATED)
    desired_salary_validated AS desired_salary,
    salary_range,
    
    -- Job Information
    job_title,
    
    -- Data Quality Flags
    had_invalid_experience,
    had_invalid_salary,
    has_invalid_email,
    
    -- Metadata
    processed_at,
    last_updated
    
FROM final_transformations

-- Data Quality Checks:
-- 1. Invalid experience: SELECT COUNT(*) FROM silver.silver_recruitment_cleaned WHERE had_invalid_experience = 1
-- 2. Invalid salary: SELECT COUNT(*) FROM silver.silver_recruitment_cleaned WHERE had_invalid_salary = 1
-- 3. Status distribution: SELECT status, COUNT(*) FROM silver.silver_recruitment_cleaned GROUP BY status
--    Note from EDA: 0 hired candidates in this dataset