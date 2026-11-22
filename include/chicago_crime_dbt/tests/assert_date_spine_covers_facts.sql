-- A singular test fails when it returns rows.
-- This returns any crime date that the calendar dimension missed.
-- The spine generator has a fixed upper bound, so this test catches a bound that is too short.

SELECT facts.occurred_date
FROM {{ ref('fct_crimes') }} AS facts
LEFT JOIN {{ ref('dim_dates') }} AS dates
    ON facts.occurred_date = dates.date
WHERE dates.date IS NULL
