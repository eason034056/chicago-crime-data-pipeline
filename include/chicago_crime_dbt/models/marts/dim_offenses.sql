-- One row per IUCR offense code.
-- IUCR is the Illinois Uniform Crime Reporting code. primary_type is the broader category.
-- QUALIFY keeps the latest description when the portal spelled the same code more than one way.

WITH coded AS (
    SELECT
        offense_code,
        primary_type,
        description,
        fbi_code,
        occurred_at
    FROM {{ ref('stg_chicago_crimes') }}
),

latest AS (
    SELECT
        offense_code,
        primary_type,
        description,
        fbi_code
    FROM coded
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY offense_code
        ORDER BY occurred_at DESC
    ) = 1
)

SELECT
    offense_code,
    primary_type,
    description,
    fbi_code
FROM latest

UNION ALL

SELECT
    'UNKNOWN' AS offense_code,
    'UNKNOWN' AS primary_type,
    'No IUCR on the source report' AS description,
    NULL AS fbi_code
WHERE NOT EXISTS (
    SELECT 1
    FROM latest
    WHERE offense_code = 'UNKNOWN'
)
