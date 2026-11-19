from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import httpx


def fetch_raw_fuel_page(url: str, timeout: float = 15.0) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; BG-City-Cost-Tracker/0.1; "
            "educational data engineering project)"
        )
    }
    with httpx.Client(headers=headers, timeout=timeout, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.text


def save_bronze_snapshot(
    html: str,
    ingestion_date: date,
    base_dir: Path = Path("data/bronze/fuels"),
) -> Path:
    partition_dir = base_dir / f"ingestion_date={ingestion_date.isoformat()}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    out_path = partition_dir / "snapshot.html"
    out_path.write_text(html, encoding="utf-8")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch raw fuel price page into Bronze layer.")
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Ingestion date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--url",
        default="https://fuelo.net/cheap/where",
        help="Source URL to fetch",
    )
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date)
    html = fetch_raw_fuel_page(args.url)
    saved_path = save_bronze_snapshot(html, target_date)
    print(f"Saved Bronze snapshot for {target_date} to: {saved_path}")


if __name__ == "__main__":
    main()