{{
    config(
        materialized='table',
        file_format='delta',
        tags=['gold', 'star_schema', 'dimension']
    )
}}

/*
Dimension: Date
Grain: One row per day
Type: Fixed dimension
Range: 2018 to 2026 (covering employee snapshot period)
*/

WITH date_range AS (
    SELECT 1 FROM (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) t1
    CROSS JOIN (SELECT 1 FROM (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) t2) t2
    CROSS JOIN (SELECT 1 FROM (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) t3) t3
    CROSS JOIN (SELECT 1 FROM (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) t4) t4 -- Added 4th join
),

sequence_numbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY NULL) - 1 AS seq
    FROM date_range
    LIMIT 3287  -- 2018-01-01 to 2026-12-31
),

date_spine AS (
    SELECT 
        DATE_ADD('2018-01-01', seq) AS date_day
    FROM sequence_numbers
)

SELECT
    -- Surrogate Key
    CAST(DATE_FORMAT(date_day, 'yyyyMMdd') AS INT) AS date_key,
    
    -- Natural Key
    date_day AS date,
    
    -- Date parts
    YEAR(date_day) AS year,
    QUARTER(date_day) AS quarter,
    MONTH(date_day) AS month,
    DAY(date_day) AS day,
    DAYOFWEEK(date_day) AS day_of_week,
    DAYOFYEAR(date_day) AS day_of_year,
    WEEKOFYEAR(date_day) AS week_of_year,
    
    -- Formatted strings
    DATE_FORMAT(date_day, 'MMMM') AS month_name,
    DATE_FORMAT(date_day, 'MMM') AS month_name_short,
    DATE_FORMAT(date_day, 'EEEE') AS day_name,
    DATE_FORMAT(date_day, 'EEE') AS day_name_short,
    CONCAT('Q', QUARTER(date_day), '-', YEAR(date_day)) AS quarter_name,
    DATE_FORMAT(date_day, 'yyyy-MM') AS year_month,
    
    -- Flags
    CASE WHEN DAYOFWEEK(date_day) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
    CASE WHEN MONTH(date_day) IN (1, 2, 3) THEN 'Q1'
         WHEN MONTH(date_day) IN (4, 5, 6) THEN 'Q2'
         WHEN MONTH(date_day) IN (7, 8, 9) THEN 'Q3'
         ELSE 'Q4' END AS quarter_label,
    
    -- Relative periods
    CASE WHEN date_day = CURRENT_DATE() THEN 1 ELSE 0 END AS is_today,
    CASE WHEN YEAR(date_day) = YEAR(CURRENT_DATE()) THEN 1 ELSE 0 END AS is_current_year,
    CASE WHEN YEAR(date_day) = YEAR(CURRENT_DATE()) - 1 THEN 1 ELSE 0 END AS is_prior_year,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS last_updated

FROM date_spine