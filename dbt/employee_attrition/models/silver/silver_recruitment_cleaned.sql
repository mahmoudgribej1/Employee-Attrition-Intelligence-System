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

BUSINESS LOGIC DISCOVERY:
  - Recruitment system tracks: Applied → In Review → Interviewing → Offered
  - "Hired" status does NOT exist in recruitment_data
  - Actual hires are identified by joining with employee_data (StartDate)
  - This is common: separate recruitment and HRIS systems

Issues Fixed:
  - Standardize application status values (5 stages: Applied, In Review, Interviewing, Offered, Rejected)
  - Clean and validate salary data
  - Validate years of experience
  - Convert date strings to proper DATE type
  - JOIN with employee_data to identify who was actually hired

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
        REGEXP_REPLACE(phone_number, '[^0-9]', '') AS phone_number_clean,
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
        
        -- STANDARDIZE: Application status
        -- Map all 5 actual statuses: Applied, In Review, Interviewing, Offered, Rejected
        CASE 
            WHEN LOWER(TRIM(Status)) = 'applied' THEN 'Applied'
            WHEN LOWER(TRIM(Status)) = 'in review' THEN 'In Review'
            WHEN LOWER(TRIM(Status)) = 'interviewing' THEN 'Interviewing'
            WHEN LOWER(TRIM(Status)) = 'offered' THEN 'Offered'
            WHEN LOWER(TRIM(Status)) = 'rejected' THEN 'Rejected'
            ELSE INITCAP(TRIM(Status))
        END AS status_standardized,
        
        -- VALIDATE: Years of experience (no negatives, reasonable max of 50)
        CASE 
            WHEN years_of_experience < 0 THEN 0
            WHEN years_of_experience > 50 THEN 50
            ELSE years_of_experience
        END AS years_of_experience_validated,
        
        -- VALIDATE: Desired salary (no negatives, reasonable range)
        CASE 
            WHEN desired_salary < 0 THEN NULL
            WHEN desired_salary > 1000000 THEN NULL
            ELSE desired_salary
        END AS desired_salary_validated,
        
        -- Data quality flags
        CASE WHEN years_of_experience < 0 OR years_of_experience > 50 THEN 1 ELSE 0 END AS had_invalid_experience,
        CASE WHEN desired_salary < 0 OR desired_salary > 1000000 THEN 1 ELSE 0 END AS had_invalid_salary,
        CASE WHEN Email IS NULL OR Email = '' OR NOT Email LIKE '%@%' THEN 1 ELSE 0 END AS has_invalid_email
        
    FROM cleaned_dates_and_contacts
),

-- NEW: Join with employee data to identify who was actually hired
hiring_verification AS (
    SELECT 
        v.*,
        e.EmpID,
        e.start_date AS employee_start_date,
        
        -- Flag: Was this applicant actually hired?
        CASE 
            WHEN e.EmpID IS NOT NULL THEN 1
            ELSE 0
        END AS was_actually_hired,
        
        -- Calculate days from application to hire
        CASE 
            WHEN e.EmpID IS NOT NULL THEN DATEDIFF(e.start_date, v.application_date)
            ELSE NULL
        END AS days_to_hire
        
    FROM validated_data v
    LEFT JOIN {{ ref('silver_employee_cleaned') }} e
        ON LOWER(TRIM(v.first_name)) = LOWER(TRIM(e.FirstName))
        AND LOWER(TRIM(v.last_name)) = LOWER(TRIM(e.LastName))
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
        
        -- UPDATED: Hiring funnel stage (includes actual hiring status)
        CASE 
            -- Rejected path
            WHEN status_standardized = 'Rejected' THEN 'Rejected'
            
            -- Hired path (cross-referenced with employee_data)
            WHEN was_actually_hired = 1 THEN 'Hired'
            
            -- In-progress stages
            WHEN status_standardized = 'Applied' THEN 'Applied'
            WHEN status_standardized = 'In Review' THEN 'In Review'
            WHEN status_standardized = 'Interviewing' THEN 'Interviewing'
            WHEN status_standardized = 'Offered' THEN 'Offered - Pending'
            
            ELSE 'Unknown'
        END AS hiring_stage,
        
        -- Hiring funnel position (for funnel charts)
        CASE 
            WHEN status_standardized = 'Applied' THEN 1
            WHEN status_standardized = 'In Review' THEN 2
            WHEN status_standardized = 'Interviewing' THEN 3
            WHEN status_standardized = 'Offered' THEN 4
            WHEN was_actually_hired = 1 THEN 5
            WHEN status_standardized = 'Rejected' THEN 99
            ELSE 0
        END AS funnel_stage_number,
        
        -- Salary expectation category
        CASE 
            WHEN desired_salary_validated < 50000 THEN 'Under 50K'
            WHEN desired_salary_validated < 75000 THEN '50K-75K'
            WHEN desired_salary_validated < 100000 THEN '75K-100K'
            WHEN desired_salary_validated < 150000 THEN '100K-150K'
            ELSE '150K+'
        END AS salary_range,
        
        -- Time to hire category
        CASE 
            WHEN days_to_hire IS NULL THEN 'Not Hired'
            WHEN days_to_hire <= 30 THEN '0-30 days'
            WHEN days_to_hire <= 60 THEN '31-60 days'
            WHEN days_to_hire <= 90 THEN '61-90 days'
            ELSE '90+ days'
        END AS time_to_hire_category,
        
        CURRENT_TIMESTAMP() AS last_updated
        
    FROM hiring_verification
)

SELECT
    -- Primary Key
    applicant_id,
    
    -- Application Information
    application_date,
    application_year,
    application_month,
    
    -- Recruitment System Status
    status_standardized AS recruitment_status,
    
    -- Actual Hiring Status (cross-referenced with employee_data)
    hiring_stage,
    funnel_stage_number,
    was_actually_hired,
    days_to_hire,
    time_to_hire_category,
    
    -- Employee ID (if hired)
    EmpID,
    employee_start_date,
    
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

/*
DATA QUALITY VALIDATION QUERIES:

1. Check hiring funnel distribution:
   SELECT hiring_stage, COUNT(*) 
   FROM workspace.silver.silver_recruitment_cleaned 
   GROUP BY hiring_stage
   
   Expected results:
   - Applied: ~611
   - In Review: ~595
   - Interviewing: ~590
   - Offered - Pending: ~604 (610 - hired)
   - Hired: ~6-10 (those who appear in employee_data)
   - Rejected: ~594

2. Verify hired applicants:
   SELECT COUNT(*) 
   FROM workspace.silver.silver_recruitment_cleaned 
   WHERE was_actually_hired = 1
   
   Expected: 6-10 matches

3. Average time to hire:
   SELECT AVG(days_to_hire) 
   FROM workspace.silver.silver_recruitment_cleaned 
   WHERE was_actually_hired = 1
*/