"""Chicago crime dashboard.

Run from the repository root:

    .venv/bin/streamlit run app/streamlit_app.py

The app reads the dbt mart for charts and the fact table for the map.
It does not transform data. Filters are applied in Snowflake.
"""

import sys
from pathlib import Path

# The pipeline package lives in include/, which is not on the default import path.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

import pandas as pd
import plotly.express as px
import streamlit as st

from pipeline.config import ANALYTICS_SCHEMA, DATABASE, safe_ident
from pipeline.load import connect

DATABASE_NAME = safe_ident(DATABASE)
SCHEMA_NAME = safe_ident(ANALYTICS_SCHEMA)
MART = f"{DATABASE_NAME}.{SCHEMA_NAME}.MART_DAILY_CRIME"
FACTS = f"{DATABASE_NAME}.{SCHEMA_NAME}.FCT_CRIMES"
OFFENSES = f"{DATABASE_NAME}.{SCHEMA_NAME}.DIM_OFFENSES"
DISTRICTS = f"{DATABASE_NAME}.{SCHEMA_NAME}.DIM_DISTRICTS"

VOLUME = "#9f2d2d"
ARREST = "#1f4b6e"


def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Run one SELECT and return a DataFrame with lowercase column names.

    A new connection is opened per call so a dropped Snowflake session does not
    get reused. Callers cache the DataFrame, not the connection.
    """
    conn = connect()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params or {})
        rows = cursor.fetchall()
        columns = [description[0].lower() for description in cursor.description]
        cursor.close()
        return pd.DataFrame(rows, columns=columns)
    finally:
        conn.close()


def sql_list(values: tuple[str, ...]) -> str:
    """Turn filter values into a quoted SQL list.

    The values originally came from our own dimension tables, and quotes inside
    a value are doubled so a name cannot break the statement.
    """
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


@st.cache_data(ttl=3600, show_spinner=False)
def filter_options() -> tuple[str, str, tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    """Load the date bounds and the two filter lists once an hour."""
    bounds = query_df(f"SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM {DATABASE_NAME}.{SCHEMA_NAME}.DIM_DATES")
    types = query_df(
        f"""
        SELECT primary_type
        FROM {OFFENSES}
        WHERE primary_type <> 'UNKNOWN'
        GROUP BY primary_type
        ORDER BY primary_type
        """
    )
    districts = query_df(
        f"""
        SELECT district, COALESCE(district_name, 'Unnamed district') AS district_name
        FROM {DISTRICTS}
        UNION
        SELECT '000', 'No district on report'
        ORDER BY district
        """
    )
    labels = tuple(f"{row.district}  {row.district_name}" for row in districts.itertuples(index=False))
    codes = tuple(districts["district"].tolist())
    return (
        str(bounds.loc[0, "min_date"]),
        str(bounds.loc[0, "max_date"]),
        tuple(types["primary_type"].tolist()),
        labels,
        codes,
    )


@st.cache_data(ttl=3600, show_spinner=False)
def daily_frame(start: str, end: str, types: tuple[str, ...], districts: tuple[str, ...]) -> pd.DataFrame:
    """One row per day after the sidebar filters."""
    sql = f"""
        SELECT
            occurred_date,
            SUM(crime_count) AS crime_count,
            SUM(arrest_count) AS arrest_count,
            SUM(domestic_count) AS domestic_count
        FROM {MART}
        WHERE occurred_date BETWEEN %(start)s AND %(end)s
          AND primary_type IN ({sql_list(types)})
          AND COALESCE(district, '000') IN ({sql_list(districts)})
        GROUP BY occurred_date
        ORDER BY occurred_date
    """
    return query_df(sql, {"start": start, "end": end})


@st.cache_data(ttl=3600, show_spinner=False)
def type_frame(start: str, end: str, types: tuple[str, ...], districts: tuple[str, ...]) -> pd.DataFrame:
    """One row per offense category after the sidebar filters."""
    sql = f"""
        SELECT
            primary_type,
            SUM(crime_count) AS crime_count,
            SUM(arrest_count) AS arrest_count
        FROM {MART}
        WHERE occurred_date BETWEEN %(start)s AND %(end)s
          AND primary_type IN ({sql_list(types)})
          AND COALESCE(district, '000') IN ({sql_list(districts)})
        GROUP BY primary_type
        ORDER BY crime_count DESC
    """
    frame = query_df(sql, {"start": start, "end": end})
    frame["arrest_rate"] = frame["arrest_count"] / frame["crime_count"]
    return frame


@st.cache_data(ttl=3600, show_spinner=False)
def district_frame(start: str, end: str, types: tuple[str, ...], districts: tuple[str, ...]) -> pd.DataFrame:
    """One row per police district after the sidebar filters."""
    sql = f"""
        SELECT
            COALESCE(district, '000') AS district,
            COALESCE(district_name, 'No district on report') AS district_name,
            SUM(crime_count) AS crime_count
        FROM {MART}
        WHERE occurred_date BETWEEN %(start)s AND %(end)s
          AND primary_type IN ({sql_list(types)})
          AND COALESCE(district, '000') IN ({sql_list(districts)})
        GROUP BY 1, 2
        ORDER BY crime_count DESC
    """
    return query_df(sql, {"start": start, "end": end})


@st.cache_data(ttl=3600, show_spinner=False)
def weekday_frame(start: str, end: str, types: tuple[str, ...], districts: tuple[str, ...]) -> pd.DataFrame:
    """One row per weekday after the sidebar filters."""
    sql = f"""
        SELECT
            day_of_week,
            day_of_week_num,
            SUM(crime_count) AS crime_count
        FROM {MART}
        WHERE occurred_date BETWEEN %(start)s AND %(end)s
          AND primary_type IN ({sql_list(types)})
          AND COALESCE(district, '000') IN ({sql_list(districts)})
        GROUP BY day_of_week, day_of_week_num
        ORDER BY day_of_week_num
    """
    return query_df(sql, {"start": start, "end": end})


@st.cache_data(ttl=3600, show_spinner=False)
def map_frame(start: str, end: str, types: tuple[str, ...], districts: tuple[str, ...]) -> pd.DataFrame:
    """The 3,000 most recent geocoded reports that match the filters.

    The map is a sample, not every report. Latitude and longitude are renamed
    lat and lon because st.map looks for those column names.
    """
    sql = f"""
        SELECT
            facts.latitude AS lat,
            facts.longitude AS lon,
            offenses.primary_type,
            facts.occurred_date
        FROM {FACTS} AS facts
        INNER JOIN {OFFENSES} AS offenses
            ON facts.offense_code = offenses.offense_code
        WHERE facts.occurred_date BETWEEN %(start)s AND %(end)s
          AND offenses.primary_type IN ({sql_list(types)})
          AND COALESCE(facts.district, '000') IN ({sql_list(districts)})
          AND facts.latitude IS NOT NULL
          AND facts.longitude IS NOT NULL
        ORDER BY facts.occurred_date DESC
        LIMIT 3000
    """
    return query_df(sql, {"start": start, "end": end})


def chart_layout(title: str) -> dict:
    """Shared Plotly layout so every chart uses the same type and margins."""
    return {
        "title": {"text": title, "font": {"size": 16, "color": "#1c1917"}},
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        "margin": {"l": 10, "r": 10, "t": 48, "b": 10},
        "font": {"color": "#1c1917"},
    }


def main() -> None:
    """Draw the sidebar filters and the six dashboard components."""
    st.set_page_config(page_title="Chicago reported crime", layout="wide")
    st.title("Chicago reported crime")
    st.caption(
        "Source: Chicago Data Portal, Crimes - 2001 to Present. "
        "This app shows reports since 2025. An arrest rate is the share of reports with an arrest, not a clearance rate."
    )

    try:
        min_date, max_date, type_options, district_labels, district_codes = filter_options()
    except Exception as exc:
        st.error("The analytics tables are not ready. Run `python run_pipeline.py --start-date 2025-01-01` first.")
        st.exception(exc)
        return
    label_to_code = dict(zip(district_labels, district_codes))

    st.sidebar.header("Filters")
    picked = st.sidebar.date_input(
        "Occurred",
        value=(pd.to_datetime(min_date).date(), pd.to_datetime(max_date).date()),
        min_value=pd.to_datetime(min_date).date(),
        max_value=pd.to_datetime(max_date).date(),
    )
    # An empty box means "all". Selecting every value would fill the sidebar with chips
    # and push the date grain off the screen.
    chosen_types = st.sidebar.multiselect("Offense type", type_options, placeholder="All types")
    chosen_labels = st.sidebar.multiselect("Police district", district_labels, placeholder="All districts")
    grain = st.sidebar.radio("Trend grain", ["Day", "Week", "Month"], horizontal=True)

    if not (isinstance(picked, tuple) and len(picked) == 2):
        st.info("Select both a start date and an end date.")
        return
    start, end = picked
    start_text = start.isoformat()
    end_text = end.isoformat()
    type_key = tuple(chosen_types or type_options)
    district_key = tuple(label_to_code[label] for label in (chosen_labels or district_labels))

    daily = daily_frame(start_text, end_text, type_key, district_key)
    by_type = type_frame(start_text, end_text, type_key, district_key)
    by_district = district_frame(start_text, end_text, type_key, district_key)
    by_weekday = weekday_frame(start_text, end_text, type_key, district_key)

    if daily.empty:
        st.info("No reports match these filters.")
        return

    reports = int(daily["crime_count"].sum())
    arrests = int(daily["arrest_count"].sum())
    domestic = int(daily["domestic_count"].sum())

    metric_cols = st.columns(4)
    metric_cols[0].metric("Reports", f"{reports:,}")
    metric_cols[1].metric("Arrest rate", f"{arrests / reports:.1%}")
    metric_cols[2].metric("Domestic share", f"{domestic / reports:.1%}")
    metric_cols[3].metric("Districts", f"{len(by_district):,}")

    trend = daily.copy()
    trend["occurred_date"] = pd.to_datetime(trend["occurred_date"])
    trend = trend.set_index("occurred_date")
    if grain == "Week":
        trend = trend.resample("W-MON").sum(numeric_only=True)
    elif grain == "Month":
        trend = trend.resample("MS").sum(numeric_only=True)
    trend = trend.reset_index()

    trend_chart = px.line(trend, x="occurred_date", y="crime_count", color_discrete_sequence=[VOLUME])
    trend_chart.update_layout(**chart_layout(f"Reports by {grain.lower()}"))
    trend_chart.update_xaxes(title=None)
    trend_chart.update_yaxes(title=None)
    st.plotly_chart(trend_chart, width="stretch")

    left, right = st.columns(2)
    volume = by_type.head(12).sort_values("crime_count")
    volume_chart = px.bar(volume, x="crime_count", y="primary_type", orientation="h", color_discrete_sequence=[VOLUME])
    volume_chart.update_layout(**chart_layout("Reports by offense type"))
    volume_chart.update_xaxes(title=None)
    volume_chart.update_yaxes(title=None)
    left.plotly_chart(volume_chart, width="stretch")

    # A rate on one or two reports is noise. The bar keeps categories with at least 50 reports.
    rates = by_type[by_type["crime_count"] >= 50].sort_values("arrest_rate")
    if rates.empty:
        right.info("No offense type in this filter has 50 or more reports.")
    else:
        rate_chart = px.bar(rates, x="arrest_rate", y="primary_type", orientation="h", color_discrete_sequence=[ARREST])
        rate_chart.update_layout(**chart_layout("Arrest rate, types with 50 or more reports"))
        rate_chart.update_xaxes(title=None, tickformat=".0%")
        rate_chart.update_yaxes(title=None)
        right.plotly_chart(rate_chart, width="stretch")

    lower_left, lower_right = st.columns(2)
    district_plot = by_district.head(12).sort_values("crime_count")
    district_chart = px.bar(
        district_plot,
        x="crime_count",
        y="district_name",
        orientation="h",
        color_discrete_sequence=[VOLUME],
    )
    district_chart.update_layout(**chart_layout("Reports by police district"))
    district_chart.update_xaxes(title=None)
    district_chart.update_yaxes(title=None)
    lower_left.plotly_chart(district_chart, width="stretch")

    weekday_chart = px.bar(by_weekday, x="day_of_week", y="crime_count", color_discrete_sequence=[ARREST])
    weekday_chart.update_layout(**chart_layout("Reports by weekday"))
    weekday_chart.update_xaxes(title=None)
    weekday_chart.update_yaxes(title=None)
    lower_right.plotly_chart(weekday_chart, width="stretch")

    st.subheader("Where the most recent reports were geocoded")
    st.caption("The map shows up to 3,000 of the most recent matching reports that have coordinates inside Chicago.")
    points = map_frame(start_text, end_text, type_key, district_key)
    if points.empty:
        st.info("None of the matching reports have coordinates.")
    else:
        st.map(points, latitude="lat", longitude="lon", size=4)


main()
