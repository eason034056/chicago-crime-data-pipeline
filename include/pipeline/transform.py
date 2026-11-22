"""Run the dbt project that turns RAW.CRIMES into analytics tables.

`transform` is the T in ELT. This file does not contain the SQL. The SQL lives
in include/chicago_crime_dbt/models. This file only shells out to the dbt CLI
so Airflow and a laptop call the same commands.
"""

import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

from pipeline.config import DBT_PROJECT_DIR, private_key_path, require_env

logger = logging.getLogger(__name__)


def dbt_executable() -> str:
    """Find the dbt program that belongs to the Python running this script.

    `sys.executable` is the python binary, for example `.venv/bin/python`.
    `with_name("dbt")` asks for `.venv/bin/dbt` in that same folder. That still
    works when the virtual environment is not activated and dbt is not on PATH.
    """
    sibling = Path(sys.executable).with_name("dbt")
    if sibling.exists():
        return str(sibling)
    found = shutil.which("dbt")
    if not found:
        raise RuntimeError(
            "dbt is not installed. From the repository root, run: .venv/bin/pip install -r requirements.txt"
        )
    return found


def prepare_dbt_env() -> dict:
    """Copy the current environment and force the key path dbt should use.

    profiles.yml reads these variable names with env_var(). Setting them here
    means a laptop key and the Airflow container key can share one profiles file.
    """
    env = os.environ.copy()
    env["SNOWFLAKE_ACCOUNT"] = require_env("SNOWFLAKE_ACCOUNT")
    env["SNOWFLAKE_USER"] = require_env("SNOWFLAKE_USER")
    env["SNOWFLAKE_ROLE"] = require_env("SNOWFLAKE_ROLE")
    env["SNOWFLAKE_WAREHOUSE"] = require_env("SNOWFLAKE_WAREHOUSE")
    env["SNOWFLAKE_DATABASE"] = require_env("SNOWFLAKE_DATABASE")
    env["SNOWFLAKE_SCHEMA"] = require_env("SNOWFLAKE_SCHEMA")
    env["SNOWFLAKE_PRIVATE_KEY_PATH"] = private_key_path()
    return env


def run_dbt(*commands: str) -> None:
    """Run one or more dbt commands in the Chicago crime project.

    Example: run_dbt("seed", "run", "test").
    seed loads the district-name CSV. run builds models. test checks them.
    """
    env = prepare_dbt_env()
    executable = dbt_executable()
    for command in commands:
        logger.info("dbt %s", command)
        subprocess.run(
            [executable, command, "--profiles-dir", ".", "--project-dir", "."],
            cwd=DBT_PROJECT_DIR,
            env=env,
            check=True,
        )
