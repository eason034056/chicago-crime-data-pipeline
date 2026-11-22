-- Daily counts by offense category and police district.
-- The name starts with mart_ because this table exists for the dashboard. The app should not scan the fact table for every chart.
-- Grain: one row per occurred_date, primary_type, and district.

{{ config(cluster_by=['occurred_date']) }}

SELECT
    facts.occurred_date,
    dates.year,
    dates.quarter,
    dates.month,
    dates.month_name,
    dates.day_of_week,
    dates.day_of_week_num,
    dates.is_weekend,
    offenses.primary_type,
    facts.district,
    districts.district_name,
    COUNT(*) AS crime_count,
    SUM(IFF(facts.was_arrest, 1, 0)) AS arrest_count,
    SUM(IFF(facts.is_domestic, 1, 0)) AS domestic_count
FROM {{ ref('fct_crimes') }} AS facts
INNER JOIN {{ ref('dim_dates') }} AS dates
    ON facts.occurred_date = dates.date
INNER JOIN {{ ref('dim_offenses') }} AS offenses
    ON facts.offense_code = offenses.offense_code
LEFT JOIN {{ ref('dim_districts') }} AS districts
    ON facts.district = districts.district
GROUP BY ALL
