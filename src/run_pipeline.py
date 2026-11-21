from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path


def run_step(cmd: list[str], label: str, allow_403_with_existing_bronze: Path | None = None) -> None:
    print(f"\n=== [{label}] {' '.join(cmd)} ===")
    completed = subprocess.run(cmd, check=False)
    if completed.returncode != 0:
        if allow_403_with_existing_bronze and allow_403_with_existing_bronze.exists():
            print(
                f"Warning: {label} exited with code {completed.returncode} (likely 403 rate-limit). "
                f"Reusing existing Bronze snapshot: {allow_403_with_existing_bronze}"
            )
            return
        raise RuntimeError(f"Step failed ({label}) with exit code {completed.returncode}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run AJAX fuel-only ETL pipeline.")
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Ingestion/snapshot date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Skip fetching Bronze snapshot and run parse + quality + gold on existing Bronze file",
    )
    parser.add_argument(
        "--max-distance-km",
        type=float,
        default=15.0,
        help="Maximum distance in km from city center",
    )
    args = parser.parse_args()

    py = sys.executable
    target_date = args.date
    bronze_snapshot = Path(f"data/bronze/fuels_ajax/ingestion_date={target_date}/snapshots.json")

    if not args.skip_fetch:
        run_step(
            [py, "-m", "src.extract.fetch_fuels_ajax", "--date", target_date],
            "Fetch Fuels AJAX",
            allow_403_with_existing_bronze=bronze_snapshot,
        )

    run_step(
        [
            py,
            "-m",
            "src.transform.parse_fuels_ajax",
            "--date",
            target_date,
            "--max-distance-km",
            str(args.max_distance_km),
        ],
        "Parse Fuels AJAX",
    )

    if Path("src/quality/check_fuels.py").exists():
        run_step([py, "-m", "src.quality.check_fuels", "--date", target_date], "Quality Check Fuels")

    run_step([py, "-m", "src.transform.build_gold"], "Build Gold Fuels")
    print("\nAJAX fuel-only pipeline completed successfully.")


if __name__ == "__main__":
    main()
