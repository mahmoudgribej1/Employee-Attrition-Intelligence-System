# Data Quality Report - Recruitment Data

## Issue Summary
**Severity:** HIGH  
**Impact:** Cannot reliably link recruitment applications to actual hires  
**Status:** DOCUMENTED - Awaiting source data correction

## Detailed Findings

### Issue 1: Recruitment and Employee Datasets Not Linked
- **Evidence:** Name-based matching produced 7 matches, but 6/7 have impossible timelines
- **Example:** Applicant "Randall Miller" applied 2023-06-21, but employee record shows hire date 2018-10-04 (5 years earlier)
- **Root Cause:** Two independent synthetic datasets with no common keys

### Issue 2: Missing Applicant Tracking System (ATS) Integration
- **Current State:** Recruitment data only tracks up to "Offered" status
- **Gap:** No "Hired" status in source data
- **Industry Standard:** ATS systems maintain applicant_id → employee_id mapping

### Issue 3: Timeline Validation Failures
| Name | Application Date | Employee Start Date | Days Difference | Valid? |
|------|-----------------|---------------------|-----------------|--------|
| Randall Miller | 2023-06-21 | 2018-10-04 | -1,721 days | ❌ NO |
| Daniel Davis | 2023-06-03 | 2021-04-24 | -770 days | ❌ NO |
| Brian Kelly | 2023-07-22 | 2021-07-06 | -746 days | ❌ NO |
| Dale Mendoza | 2023-07-09 | 2020-01-28 | -1,258 days | ❌ NO |
| Jason Smith | 2023-07-08 | 2023-06-19 | -19 days | ❌ NO |

## Impact on Analysis

### ❌ Cannot Analyze:
- True hiring conversion rates
- Time-to-hire metrics
- New hire retention linked to recruitment source
- Offer acceptance rates

### ✅ Can Still Analyze:
- Recruitment funnel (Applied → Offered)
- Candidate demographics
- Application volumes by job title
- Rejection reasons and timing

## Recommendations

### For Production System:
1. Implement proper ATS integration with employee_id mapping
2. Add "Hired" status to recruitment tracking
3. Use unique applicant_id (not names) for matching
4. Implement data quality checks at ingestion

### For Current Project:
1. ✅ Document limitation in all dashboards
2. ✅ Focus analysis on recruitment funnel (not hiring outcomes)
3. ✅ Use employee_data independently for retention analysis
4. ✅ Flag this in interviews as "data quality issue I identified and handled"

## SQL to Reproduce Findings
```sql
-- Show impossible timelines
SELECT 
    first_name, last_name,
    application_date,
    employee_start_date,
    DATEDIFF(employee_start_date, application_date) as days_diff
FROM workspace.silver.silver_recruitment_cleaned
WHERE EmpID IS NOT NULL
ORDER BY days_diff;
```

---
**Analyst:** Mahmoud Gribej  
**Date:** 2026-01-27  
**Status:** Documented for stakeholder review