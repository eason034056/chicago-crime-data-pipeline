-- One cleaned crime report.
-- Raw stores text. This view casts types and drops coordinates that are not in Chicago.
-- The name starts with stg_ because it is a staging model: cleaned, but not yet a fact or a dimension.

SELECT
    id AS crime_id,
    NULLIF(case_number, '') AS case_number,
    TRY_TO_TIMESTAMP_NTZ(crime_date) AS occurred_at,
    TO_DATE(TRY_TO_TIMESTAMP_NTZ(crime_date)) AS occurred_date,
    NULLIF(block, '') AS block,
    COALESCE(NULLIF(iucr, ''), 'UNKNOWN') AS offense_code,
    COALESCE(NULLIF(UPPER(TRIM(primary_type)), ''), 'UNKNOWN') AS primary_type,
    NULLIF(description, '') AS description,
    NULLIF(location_description, '') AS location_description,
    COALESCE(TRY_TO_BOOLEAN(arrest), FALSE) AS was_arrest,
    COALESCE(TRY_TO_BOOLEAN(domestic), FALSE) AS is_domestic,
    NULLIF(beat, '') AS beat,
    LPAD(NULLIF(district, ''), 3, '0') AS district,
    TRY_TO_NUMBER(ward) AS ward,
    TRY_TO_NUMBER(community_area) AS community_area,
    NULLIF(fbi_code, '') AS fbi_code,
    CASE
        WHEN TRY_TO_DOUBLE(latitude) BETWEEN 41.64 AND 42.03
         AND TRY_TO_DOUBLE(longitude) BETWEEN -87.94 AND -87.52
        THEN TRY_TO_DOUBLE(latitude)
    END AS latitude,
    CASE
        WHEN TRY_TO_DOUBLE(latitude) BETWEEN 41.64 AND 42.03
         AND TRY_TO_DOUBLE(longitude) BETWEEN -87.94 AND -87.52
        THEN TRY_TO_DOUBLE(longitude)
    END AS longitude,
    TRY_TO_TIMESTAMP_NTZ(updated_on) AS updated_at,
    TRY_TO_TIMESTAMP_NTZ(_loaded_at) AS loaded_at
FROM {{ source('chicago_crime', 'crimes') }}
WHERE id IS NOT NULL
  AND TRY_TO_TIMESTAMP_NTZ(crime_date) IS NOT NULL
  AND TO_DATE(TRY_TO_TIMESTAMP_NTZ(crime_date)) >= DATE '2025-01-01'
