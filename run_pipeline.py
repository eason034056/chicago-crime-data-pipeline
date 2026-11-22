"""Run the pipeline without setting PYTHONPATH.

From the repository root:

    .venv/bin/python run_pipeline.py --start-date 2025-01-01
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "include"))

from pipeline.run import main

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
    main()
