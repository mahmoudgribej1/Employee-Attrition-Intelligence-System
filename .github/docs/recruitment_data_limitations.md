# Recruitment Data Analysis Limitations

## Executive Summary
The recruitment and employee datasets in this project are synthetic and independent. While valuable for demonstrating data engineering skills, they cannot be used to analyze end-to-end hiring outcomes.

## What We Can Trust
✅ Recruitment funnel metrics (Applied → Offered)
✅ Candidate demographics and qualifications
✅ Application volumes and trends
✅ Rejection analysis

## What We Cannot Trust
❌ Hiring conversion rates
❌ Offer acceptance rates
❌ Time-to-hire metrics
❌ New hire retention from recruitment source

## Validation Results
- Name matches attempted: 7
- Timeline validation failures: 7 (100%)
- Earliest impossible timeline: -1,721 days
- Valid hiring records: 0

## Impact on Dashboards
**Recruitment Dashboard:**
- Shows funnel up to "Offered - Pending" ✅
- Note added: "Hiring outcomes tracked separately in HRIS"

**Employee Retention Dashboard:**
- Analyzes retention independently ✅
- Does not claim recruitment source attribution

## Lessons Learned
This exercise demonstrates real-world data engineering challenges:
1. Not all data integrations are straightforward
2. Timeline validation is critical
3. Documentation prevents misinterpretation
4. Knowing what you DON'T know is as important as what you DO know