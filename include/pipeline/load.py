"""Load raw crime rows into Snowflake and merge them by crime id.

`load` is the L in ELT. The table RAW.CRIMES keeps one current row per report.
Loading again does not append a second copy of the same id; the MERGE updates it.
"""

import logging

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from pipeline.config import (
    DATABASE,
    RAW_COLUMNS,
    RAW_SCHEMA,
    SETUP_SQL,
    safe_ident,
    snowflake_settings,
)

logger = logging.getLogger(__name__)


def connect():
    """Open one Snowflake session with the course key-pair.

    The name is `connect` because every later function needs a live session and
    should not read the private key itself. Call connect(), use it, then close it.
    Database and schema are omitted here on purpose. The first run creates
    CHICAGO_CRIME, so naming it at login would fail before it exists.
    Every statement in this project uses a fully qualified table name.
    """
    settings = snowflake_settings()
    settings.pop("database", None)
    settings.pop("schema", None)
    return snowflake.connector.connect(**settings)


def execute_sql_file(conn, path) -> None:
    """Run each statement in a .sql file.

    The file is split on semicolons. Our setup script has no semicolons inside
    strings, so the split is safe. CHICAGO_CRIME in the file is replaced with
    the database name from .env so the SQL and the Python stay on the same database.
    """
    script = path.read_text().replace("CHICAGO_CRIME", safe_ident(DATABASE))
    cursor = conn.cursor()
    try:
        for statement in script.split(";"):
            statement = statement.strip()
            if statement:
                cursor.execute(statement)
    finally:
        cursor.close()


def raw_row_count(conn) -> int:
    """Return how many rows are already in RAW.CRIMES.

    Zero means this Snowflake database has never had a successful load, so the
    caller should backfill instead of asking for a 14-day update window.
    """
    cursor = conn.cursor()
    try:
        database = safe_ident(DATABASE)
        schema = safe_ident(RAW_SCHEMA)
        cursor.execute(f"SHOW DATABASES LIKE '{database}'")
        if cursor.fetchone() is None:
            return 0
        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM {database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}'
              AND TABLE_NAME = 'CRIMES'
            """
        )
        if cursor.fetchone()[0] == 0:
            return 0
        cursor.execute(f"SELECT COUNT(*) FROM {database}.{schema}.CRIMES")
        return int(cursor.fetchone()[0])
    finally:
        cursor.close()


def load_crimes(conn, frame: pd.DataFrame) -> int:
    """Stage a batch and merge it into RAW.CRIMES.

    write_pandas creates CRIMES_STAGE and replaces it on every run.
    MERGE then updates a crime the city revised and inserts a crime we have
    not seen. The function returns how many staged rows were merged.
    """
    execute_sql_file(conn, SETUP_SQL)
    if frame.empty:
        logger.info("No rows to load.")
        return 0

    staged = frame.loc[:, RAW_COLUMNS].copy()
    # write_pandas uppercases unquoted column names. The setup SQL does the same.
    database = safe_ident(DATABASE)
    schema = safe_ident(RAW_SCHEMA)
    success, _chunks, nrows, _output = write_pandas(
        conn,
        staged,
        "CRIMES_STAGE",
        database=database,
        schema=schema,
        auto_create_table=True,
        overwrite=True,
        quote_identifiers=False,
    )
    if not success:
        raise RuntimeError("write_pandas failed while creating CRIMES_STAGE.")

    assignments = ",\n        ".join(
        f"target.{column} = source.{column}"
        for column in RAW_COLUMNS
        if column != "id"
    )
    insert_columns = ", ".join(RAW_COLUMNS)
    insert_values = ", ".join(f"source.{column}" for column in RAW_COLUMNS)

    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            MERGE INTO {database}.{schema}.CRIMES AS target
            USING {database}.{schema}.CRIMES_STAGE AS source
                ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET
                {assignments}
            WHEN NOT MATCHED THEN INSERT ({insert_columns})
                VALUES ({insert_values})
            """
        )
    finally:
        cursor.close()

    logger.info("Merged %s staged rows into RAW.CRIMES.", nrows)
    return int(nrows)
