-- Arrests and domestic flags are subsets of reports, so neither count can exceed crime_count.

SELECT
    occurred_date,
    primary_type,
    district,
    crime_count,
    arrest_count,
    domestic_count
FROM {{ ref('mart_daily_crime') }}
WHERE crime_count < 0
   OR arrest_count < 0
   OR domestic_count < 0
   OR arrest_count > crime_count
   OR domestic_count > crime_count
