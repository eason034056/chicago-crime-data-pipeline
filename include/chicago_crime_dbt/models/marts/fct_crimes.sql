-- One row per crime report.
-- The name starts with fct_ because this is a fact table: each row is an event, and the keys point at dimensions.
-- primary_type is not stored here. It lives on dim_offenses and is reached through offense_code.

{{ config(cluster_by=['occurred_date']) }}

SELECT
    crime_id,
    case_number,
    occurred_at,
    occurred_date,
    offense_code,
    district,
    beat,
    ward,
    community_area,
    block,
    location_description,
    was_arrest,
    is_domestic,
    latitude,
    longitude
FROM {{ ref('stg_chicago_crimes') }}
