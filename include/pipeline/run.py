"""Command-line entry point for one full pipeline run.

Usage from the repository root:

    .venv/bin/python -m pipeline.run --start-date 2025-01-01

The module is called `run` because it is the thing you execute. Airflow does
not call this file. Airflow calls extract_and_load, then runs dbt itself, so
each step is a separate box in the DAG.
"""

import argparse
import logging
from datetime import datetime, timedelta, timezone

from pipeline.config import DEFAULT_START_DATE, LOOKBACK_DAYS
from pipeline.extract import extract_crimes
from pipeline.load import connect, load_crimes, raw_row_count
from pipeline.transform import run_dbt

logger = logging.getLogger(__name__)


def choose_window(conn, start_date: str | None, mode: str) -> tuple[str, str]:
    """Decide which API filter this run should use.

    An explicit --start-date always filters on the crime date.
    With no arguments, an empty raw table backfills from 2025-01-01.
    A raw table that already has rows asks for reports updated in the last 14 days.
    Those reports still have to have occurred on or after 2025-01-01.
    """
    if start_date:
        return start_date, "date" if mode == "auto" else mode
    if mode in {"date", "updated_on"}:
        raise ValueError("Pass --start-date when mode is date or updated_on.")

    if raw_row_count(conn) == 0:
        logger.info("RAW.CRIMES is empty. Backfilling from %s.", DEFAULT_START_DATE)
        return DEFAULT_START_DATE, "date"

    start = (datetime.now(timezone.utc).date() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    logger.info("RAW.CRIMES already has rows. Refreshing updates since %s.", start)
    return start, "updated_on"


def extract_and_load(start_date: str | None = None, mode: str = "auto") -> int:
    """Extract one window and merge it into Snowflake.

    This is the function the Airflow PythonOperator calls. It returns the number
    of staged rows. The name joins the two steps because they are one task:
    data is not useful in Snowflake until both have finished.
    """
    conn = connect()
    try:
        window_start, window_mode = choose_window(conn, start_date, mode)
        frame = extract_crimes(window_start, window_mode)
        return load_crimes(conn, frame)
    finally:
        conn.close()


def verify_analytics() -> None:
    """Fail the run when the fact table is missing or empty.

    Called verify because it does not transform anything. It answers one
    question: did the pipeline leave a fact table an analyst can query?
    """
    conn = connect()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*), MIN(occurred_date), MAX(occurred_date)
            FROM CHICAGO_CRIME.ANALYTICS.FCT_CRIMES
            """
        )
        count, first_date, last_date = cursor.fetchone()
        cursor.close()
    finally:
        conn.close()

    logger.info(
        "FCT_CRIMES rows=%s earliest=%s latest=%s",
        count,
        first_date,
        last_date,
    )
    if count == 0:
        raise RuntimeError("FCT_CRIMES is empty. The pipeline did not produce analytics rows.")


def main() -> None:
    """Parse CLI flags, load raw data, then build and test the dbt models."""
    parser = argparse.ArgumentParser(description="Extract, load, and transform Chicago crime data.")
    parser.add_argument(
        "--start-date",
        default=None,
        help="Backfill crimes that occurred on or after this YYYY-MM-DD date.",
    )
    parser.add_argument(
        "--mode",
        choices=["auto", "date", "updated_on"],
        default="auto",
        help="auto picks a backfill or a 14-day update. The other choices need --start-date.",
    )
    parser.add_argument(
        "--skip-dbt",
        action="store_true",
        help="Only extract and load. Leave the analytics tables untouched.",
    )
    args = parser.parse_args()

    staged = extract_and_load(args.start_date, args.mode)
    logger.info("Staged %s rows.", staged)
    if args.skip_dbt:
        return

    run_dbt("seed", "run", "test")
    verify_analytics()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
    main()
