# Chicago reported crime, 2025 to present

End-to-end warehouse project for MLDS 430. The question is: since 2025, which Chicago crime reports are most common, where do they concentrate, and which offense types actually end in an arrest?

The source is the Chicago Data Portal dataset **Crimes - 2001 to Present** (`ijzp-q8t2`). It is not the Open-Meteo or Wikipedia data used in class.

## Pipeline

```text
Chicago Data Portal API
        |
        |  Python extract, paged at 50,000 rows
        v
Snowflake CHICAGO_CRIME.RAW.CRIMES     one current row per report id
        |
        |  dbt
        v
CHICAGO_CRIME.ANALYTICS                star schema and a dashboard mart
        |
        v
Streamlit app                          filters, trend, rates, map
```

Airflow runs that path once a day. The first run backfills from 2025-01-01. Later runs pull reports the city has updated in the last 14 days and merge them on report id.

## What each piece is for

| Piece | Where | Rubric |
| --- | --- | --- |
| API extract | `include/pipeline/extract.py` | Fivetran / API |
| Snowflake load and merge | `include/pipeline/load.py`, `sql/01_setup.sql` | Snowflake |
| Staging, dimensions, fact, mart, tests | `include/chicago_crime_dbt/` | dbt, at least 2 models |
| Dashboard | `app/streamlit_app.py` | Streamlit, at least 2 components |
| Daily orchestration | `dags/chicago_crime_dag.py` | Extra credit, Airflow |
| This file | `README.md` | Documentation |

Raw columns stay text. Casting, the Chicago coordinate check, district names, and the arrest counts happen in dbt.

### dbt models

| Model | Grain | Why it exists |
| --- | --- | --- |
| `stg_chicago_crimes` | one report | Typed view over the raw text |
| `dim_dates` | one calendar day | Trend line, weekday, weekend |
| `dim_offenses` | one IUCR code | Offense category lives in one place |
| `dim_districts` | one police district | Code plus the district name |
| `fct_crimes` | one report | Fact table used by the map |
| `mart_daily_crime` | day, offense type, district | Pre-aggregated table the charts read |

IUCR is the Illinois Uniform Crime Reporting code. `primary_type` is the category on top of that code, such as `THEFT` or `BATTERY`.

The arrest rate in the app is the share of reports with an arrest. It is not a police clearance rate.

## Run it locally

Python 3.12 is required. From the repository root:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements-app.txt
cp .env.example .env
```

Edit `.env` with the course Snowflake account, user, role, warehouse, and the absolute path to `rsa_key.p8`. Do not commit `.env` or the key.

Backfill, build the models, and run the tests:

```bash
.venv/bin/python run_pipeline.py --start-date 2025-01-01
```

Open the dashboard:

```bash
.venv/bin/streamlit run app/streamlit_app.py
```

A later run with no date argument only reloads reports updated in the last 14 days:

```bash
.venv/bin/python run_pipeline.py
```

## Extra credit: Airflow

The project is an Astro project (`Dockerfile`, `dags/`, `requirements.txt`). Runtime image: `astrocrpublic.azurecr.io/runtime:3.3-12`.

```bash
bash scripts/prepare_airflow_key.sh
astro dev start
```

Trigger the DAG `chicago_crime_pipeline`. The grid is:

`extract_and_load` → `dbt_run` → `dbt_test` → `verify_analytics`

Do the local backfill first. After `RAW.CRIMES` has rows, the scheduled run only asks the API for the 14-day update window, which is what makes the pipeline continuous rather than a one-time file drop.

`astro dev stop` shuts the local Airflow down.

## Video script

About two and a half minutes:

> This project asks what reported crime in Chicago looks like since 2025, and which offense types end in an arrest.
>
> The source is the Chicago Data Portal, not the weather or Wikipedia data from class. Python pages through the Socrata API. Airflow runs that extract every day. An empty raw table backfills from January 2025. After that, the task only pulls reports updated in the last 14 days and merges them on crime id, so a revised report updates the same row instead of being inserted twice.
>
> The loader writes text into Snowflake, database CHICAGO_CRIME, schema RAW, table CRIMES. Cleaning is not done in Python. dbt builds a staging view, a date dimension, an offense dimension, a district dimension, a fact table at one row per report, and a daily mart for the dashboard. The tests check primary keys, foreign keys, and that an arrest count cannot be larger than the report count.
>
> The Streamlit app reads those tables. The filters are date, offense type, and police district. The trend shows volume. The two bars next to it separate a common offense from an offense that often ends in an arrest. The map shows the latest geocoded reports that match the filters, and the caption says it is a sample of 3,000, not every row.
>
> That is the whole path: API, Snowflake, dbt, Streamlit, and Airflow keeping it current.

## Submit

1. Push this repository to GitHub.
2. Grant **adashofdata** and **Hibb-bb** access.
3. Submit the repository link and a 2–3 minute recording on Canvas. Due Monday, December 7, 2026, 11:59pm.

## Repository map

```text
app/streamlit_app.py          dashboard
dags/chicago_crime_dag.py     Airflow DAG
include/pipeline/             extract, load, dbt commands
include/chicago_crime_dbt/    dbt project
sql/01_setup.sql              raw database, schema, and table
scripts/prepare_airflow_key.sh
run_pipeline.py               one local command for extract, load, dbt, and tests
.env.example                  Snowflake settings, with no secrets
```
