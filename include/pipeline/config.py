"""Shared settings for the extract, load, dbt, and Streamlit steps.

The name `config` is short for configuration: values that change between
computers (account, key path) live in `.env`. Values that define this project
(API URL, column list, database name) live here.
"""

import os
import re
from pathlib import Path

from dotenv import load_dotenv

# This file is include/pipeline/config.py.
# parents[0] = pipeline, parents[1] = include, parents[2] = repository root.
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

# Crimes - 2001 to Present. Dataset id ijzp-q8t2 on the Chicago Data Portal.
API_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
# Socrata allows 50000 rows per request on this portal.
PAGE_SIZE = 50000
# First load starts here. Later Airflow runs only pull recently updated rows.
DEFAULT_START_DATE = "2025-01-01"
LOOKBACK_DAYS = 14

DATABASE = os.getenv("SNOWFLAKE_DATABASE", "CHICAGO_CRIME")
RAW_SCHEMA = "RAW"
ANALYTICS_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS")

DBT_PROJECT_DIR = ROOT / "include" / "chicago_crime_dbt"
SETUP_SQL = ROOT / "sql" / "01_setup.sql"

# Column order is the raw-table contract. sql/01_setup.sql must list the same names.
# `date` from the API is renamed to crime_date because DATE is a confusing column name.
RAW_COLUMNS = [
    "id",
    "case_number",
    "crime_date",
    "block",
    "iucr",
    "primary_type",
    "description",
    "location_description",
    "arrest",
    "domestic",
    "beat",
    "district",
    "ward",
    "community_area",
    "fbi_code",
    "x_coordinate",
    "y_coordinate",
    "year",
    "updated_on",
    "latitude",
    "longitude",
    "_loaded_at",
]

# Fields requested from the API. `date` is renamed after download.
API_SELECT = ",".join([
    "id",
    "case_number",
    "date",
    "block",
    "iucr",
    "primary_type",
    "description",
    "location_description",
    "arrest",
    "domestic",
    "beat",
    "district",
    "ward",
    "community_area",
    "fbi_code",
    "x_coordinate",
    "y_coordinate",
    "year",
    "updated_on",
    "latitude",
    "longitude",
])


def require_env(name: str) -> str:
    """Return one required environment variable, or stop with a clear message.

    Called when a connection is opened. Importing this module does not require
    the variables, so Airflow can parse the DAG on a machine that has no .env yet.
    """
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Missing environment variable {name}. Copy .env.example to .env and fill it in."
        )
    return value


def private_key_path() -> str:
    """Pick the Snowflake private key that exists on this machine.

    Inside the Airflow container the key is mounted at the Astro include path.
    On a laptop, .env points at the course key. The project copy is the fallback
    used after scripts/prepare_airflow_key.sh.
    """
    airflow_key = Path("/usr/local/airflow/include/rsa_key.p8")
    if airflow_key.exists():
        return str(airflow_key)

    configured = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if configured and Path(configured).exists():
        return configured

    project_key = ROOT / "include" / "rsa_key.p8"
    if project_key.exists():
        return str(project_key)

    raise RuntimeError(
        "Snowflake private key not found. Set SNOWFLAKE_PRIVATE_KEY_PATH in .env, "
        "or copy the key to include/rsa_key.p8."
    )


def snowflake_settings() -> dict:
    """Bundle the connection settings once, for the loader and the Streamlit app."""
    return {
        "account": require_env("SNOWFLAKE_ACCOUNT"),
        "user": require_env("SNOWFLAKE_USER"),
        "private_key_file": private_key_path(),
        "role": require_env("SNOWFLAKE_ROLE"),
        "warehouse": require_env("SNOWFLAKE_WAREHOUSE"),
        "database": DATABASE,
        "schema": ANALYTICS_SCHEMA,
    }


def safe_ident(value: str) -> str:
    """Allow only plain SQL identifiers before they are pasted into a query.

    Database and schema names come from the environment, not from a typed filter,
    but they are still checked so a bad .env cannot change the SQL statement.
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Refusing to use {value!r} as a SQL identifier.")
    return value.upper()
