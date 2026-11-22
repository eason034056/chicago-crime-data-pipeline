-- One row per police district that appears in the reports.
-- The seed police_districts supplies the district name. A code the seed does not know still appears, with a null name.

WITH observed AS (
    SELECT DISTINCT district
    FROM {{ ref('stg_chicago_crimes') }}
    WHERE district IS NOT NULL
)

SELECT
    observed.district,
    police_districts.district_name
FROM observed
LEFT JOIN {{ ref('police_districts') }} AS police_districts
    ON observed.district = LPAD(TO_VARCHAR(police_districts.district), 3, '0')
