-- One row per calendar day from 2025-01-01 through the latest report.
-- The name starts with dim_ because this is a dimension: it describes the date, it is not an event.
-- GENERATOR builds the days that had zero reports, so a trend line does not skip them.
-- The start date is fixed. MIN(occurred_date) is not used, because an incremental
-- load can bring a revised report from 2001, and that one row would pull the calendar backward.
-- ROWCOUNT is only an upper bound: 4000 days from 2025-01-01 reaches 2035.
-- The last WHERE keeps dates through the latest report.

WITH bounds AS (
    SELECT
        DATE '2025-01-01' AS start_date,
        MAX(occurred_date) AS end_date
    FROM {{ ref('stg_chicago_crimes') }}
),

day_offsets AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS day_offset
    FROM TABLE(GENERATOR(ROWCOUNT => 4000))
),

spine AS (
    SELECT DATEADD(DAY, day_offsets.day_offset, bounds.start_date) AS date
    FROM bounds
    CROSS JOIN day_offsets
    WHERE bounds.start_date IS NOT NULL
)

SELECT
    spine.date,
    YEAR(spine.date) AS year,
    QUARTER(spine.date) AS quarter,
    MONTH(spine.date) AS month,
    MONTHNAME(spine.date) AS month_name,
    DAYNAME(spine.date) AS day_of_week,
    DAYOFWEEKISO(spine.date) AS day_of_week_num,
    DAYOFWEEKISO(spine.date) IN (6, 7) AS is_weekend
FROM spine
CROSS JOIN bounds
WHERE spine.date <= bounds.end_date
