"""
Qiddiya project launcher.

Usage:
    uv run main.py generate     # run synthetic data generation + Databricks upload

The generator itself lives in `_qiddiya_synthetic_data_generartion/qiddiya_load_databricks.py`
and can also be run directly. dbt is run separately from `dheeraj_qiddiya_dbt_databricks/`
(see README.md).
"""

import argparse
import runpy
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
GENERATOR_SCRIPT = (
    PROJECT_ROOT / "_qiddiya_synthetic_data_generartion" / "qiddiya_load_databricks.py"
)


def run_generator() -> None:
    """Execute the synthetic data generator exactly as if it were run directly."""
    if not GENERATOR_SCRIPT.is_file():
        sys.exit(f"Generator script not found: {GENERATOR_SCRIPT}")
    runpy.run_path(str(GENERATOR_SCRIPT), run_name="__main__")


def main() -> None:
    parser = argparse.ArgumentParser(description="Qiddiya project launcher")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "generate",
        help="Generate synthetic SF/AQ data and upload it to the Databricks landing zone",
    )
    args = parser.parse_args()

    if args.command == "generate":
        run_generator()


if __name__ == "__main__":
    main()
