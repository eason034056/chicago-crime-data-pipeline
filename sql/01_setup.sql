-- Raw landing zone for Chicago crime reports.
-- Python loads text only. dbt does the typing, renaming, and business rules.
-- Run automatically by pipeline/load.py. Safe to run more than once.

CREATE DATABASE IF NOT EXISTS CHICAGO_CRIME;
CREATE SCHEMA IF NOT EXISTS CHICAGO_CRIME.RAW;
CREATE SCHEMA IF NOT EXISTS CHICAGO_CRIME.ANALYTICS;

CREATE TABLE IF NOT EXISTS CHICAGO_CRIME.RAW.CRIMES (
    id VARCHAR,
    case_number VARCHAR,
    crime_date VARCHAR,
    block VARCHAR,
    iucr VARCHAR,
    primary_type VARCHAR,
    description VARCHAR,
    location_description VARCHAR,
    arrest VARCHAR,
    domestic VARCHAR,
    beat VARCHAR,
    district VARCHAR,
    ward VARCHAR,
    community_area VARCHAR,
    fbi_code VARCHAR,
    x_coordinate VARCHAR,
    y_coordinate VARCHAR,
    year VARCHAR,
    updated_on VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR,
    _loaded_at VARCHAR
);
