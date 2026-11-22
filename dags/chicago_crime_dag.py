"""Daily Chicago crime pipeline.

extract_and_load pulls new or revised reports and merges them into Snowflake.
dbt_run builds the star schema. dbt_test checks keys and counts.
verify_analytics stops the DAG if the fact table is empty.

The file lives in dags/ because that is the folder Airflow scans for DAGs.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# include/ holds the pipeline package. This path works on a laptop and in Astro.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "include"))

from pipeline.run import extract_and_load, verify_analytics  # noqa: E402

DBT_DIR = "${AIRFLOW_HOME}/include/chicago_crime_dbt"

# Use the key inside the container when it is there. A laptop .env path does not
# exist inside Docker, so the bash step sets the path Airflow can actually read.
DBT_SETUP = """
set -euo pipefail
if [ -f /usr/local/airflow/include/rsa_key.p8 ]; then
  export SNOWFLAKE_PRIVATE_KEY_PATH=/usr/local/airflow/include/rsa_key.p8
fi
if [ -x "${AIRFLOW_HOME}/dbt_venv/bin/dbt" ]; then
  DBT="${AIRFLOW_HOME}/dbt_venv/bin/dbt"
else
  DBT=dbt
fi
cd "%s"
""" % DBT_DIR

default_args = {
    "owner": "mlds430",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="chicago_crime_pipeline",
    description="Chicago crime: API extract, Snowflake merge, dbt star schema, tests",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["mlds430", "chicago-crime", "extra-credit"],
) as dag:
    extract_load = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load,
        execution_timeout=timedelta(minutes=60),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=DBT_SETUP + '"$DBT" seed --profiles-dir . && "$DBT" run --profiles-dir .\n',
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT_SETUP + '"$DBT" test --profiles-dir .\n',
    )

    verify = PythonOperator(
        task_id="verify_analytics",
        python_callable=verify_analytics,
    )

    extract_load >> dbt_run >> dbt_test >> verify
