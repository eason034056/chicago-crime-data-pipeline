"""Download crime reports from the Chicago Data Portal.

`extract` means the first letter of ELT: take data out of the source system
and land it, still raw, somewhere we control. This module only downloads and
normalizes text. It does not decide arrest rates or district names.
"""

import logging
import os
import re
import time
from datetime import datetime, timezone

import pandas as pd
import requests

from pipeline.config import API_SELECT, API_URL, DEFAULT_START_DATE, PAGE_SIZE, RAW_COLUMNS

logger = logging.getLogger(__name__)

# SoQL date filters only accept a date we validated. This blocks accidental SQL.
DATE_PATTERN = r"^\d{4}-\d{2}-\d{2}$"


def validate_start_date(start_date: str) -> str:
    """Reject anything that is not YYYY-MM-DD before it is placed in the API filter."""
    if not re.fullmatch(DATE_PATTERN, start_date):
        raise ValueError(f"start_date must look like 2025-01-01, got {start_date!r}.")
    datetime.strptime(start_date, "%Y-%m-%d")
    return start_date


def build_where(start_date: str, mode: str) -> str:
    """Build the Socrata filter for one extract window.

    mode "date" keeps reports that happened on or after start_date.
    That is the historical backfill.

    mode "updated_on" keeps reports the city changed on or after start_date.
    The crime date must still be on or after 2025-01-01. A revised 2001 report
    is outside this project, so it is not downloaded.
    """
    start_date = validate_start_date(start_date)
    if mode not in {"date", "updated_on"}:
        raise ValueError("mode must be 'date' or 'updated_on'.")
    if mode == "date":
        return f"date >= '{start_date}T00:00:00'"
    return (
        f"updated_on >= '{start_date}T00:00:00' "
        f"AND date >= '{DEFAULT_START_DATE}T00:00:00'"
    )


def fetch_page(session: requests.Session, where: str, offset: int) -> list[dict]:
    """Download one page of reports.

    The function is called fetch_page because Socrata will not return the whole
    table in one response. offset is how many rows to skip. limit is the page size.
    """
    params = {
        "$select": API_SELECT,
        "$where": where,
        "$order": "id",
        "$limit": PAGE_SIZE,
        "$offset": offset,
    }
    # Five tries. A 429 means the portal asked us to slow down.
    for attempt in range(5):
        response = session.get(API_URL, params=params, timeout=180)
        if response.status_code == 429 or response.status_code >= 500:
            wait_seconds = 2 ** attempt
            logger.warning("API status %s. Waiting %s seconds.", response.status_code, wait_seconds)
            time.sleep(wait_seconds)
            continue
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict):
            raise RuntimeError(f"Chicago API returned an error payload: {payload}")
        return payload
    raise RuntimeError(f"Chicago API failed after 5 tries at offset {offset}.")


def to_text(value):
    """Turn one API value into the text we store in the raw table.

    Booleans become the words true and false so the raw table can be all VARCHAR.
    Blank values become None, which Snowflake stores as NULL.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none", "<na>"}:
        return None
    return text


def normalize_crimes(frame: pd.DataFrame, loaded_at: str) -> pd.DataFrame:
    """Make every batch match RAW.CRIMES, column for column.

    Called normalize because different API rows omit different keys. After this
    function, every column in RAW_COLUMNS exists and every cell is text or None.
    """
    if frame.empty:
        return pd.DataFrame(columns=RAW_COLUMNS)

    renamed = frame.rename(columns={"date": "crime_date"})
    renamed["_loaded_at"] = loaded_at
    renamed = renamed.reindex(columns=RAW_COLUMNS)

    for column in RAW_COLUMNS:
        renamed[column] = renamed[column].map(to_text)

    # The same id can appear twice if a page boundary shifts. Keep one row.
    renamed = renamed.drop_duplicates(subset=["id"], keep="last")
    renamed = renamed[renamed["id"].notna()]
    return renamed.reset_index(drop=True)


def extract_crimes(start_date: str, mode: str) -> pd.DataFrame:
    """Download every page for one window and return a raw-ready DataFrame.

    Use it from extract_and_load. Pass mode="date" for the 2025 backfill and
    mode="updated_on" for the daily refresh.
    """
    where = build_where(start_date, mode)
    # One timestamp for the whole batch, so a load can be identified later.
    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    session = requests.Session()
    session.headers["User-Agent"] = "mlds430-chicago-crime-pipeline"
    token = os.getenv("SODA_APP_TOKEN")
    if token:
        session.headers["X-App-Token"] = token

    pages = []
    offset = 0
    while True:
        rows = fetch_page(session, where, offset)
        logger.info("Extracted %s rows at offset %s.", len(rows), offset)
        if not rows:
            break
        pages.append(pd.DataFrame(rows))
        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    if not pages:
        return pd.DataFrame(columns=RAW_COLUMNS)

    combined = pd.concat(pages, ignore_index=True)
    normalized = normalize_crimes(combined, loaded_at)
    logger.info("Extract finished with %s distinct crime ids.", len(normalized))
    return normalized
