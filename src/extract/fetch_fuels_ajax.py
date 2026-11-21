from __future__ import annotations

import argparse
import json
import time
from datetime import date
from pathlib import Path

import httpx

TOP_5_CITIES: dict[str, tuple[float, float]] = {
    "?????": (42.6977, 23.3219),
    "???????": (42.1354, 24.7453),
    "?????": (43.2141, 27.9147),
    "??????": (42.5048, 27.4626),
    "????": (43.8356, 25.9657),
}

FUEL_TYPES: list[str] = [
    "gasoline",
    "diesel",
    "lpg",
    "methane",
    "gasoline98",
    "gasoline98plus",
    "dieselplus",
]

AJAX_URL = "https://bg.fuelo.net/gasstations/ajax_get_near_gasstations_fuel_type"
SESSION_WARMUP_URL = "https://bg.fuelo.net/gasstations/byfuel/diesel?lang=bg"


def fetch_ajax_snapshots(
    cities: dict[str, tuple[float, float]] = TOP_5_CITIES,
    fuel_types: list[str] = FUEL_TYPES,
    delay_sec: float = 0.2,
    timeout: float = 15.0,
) -> dict[str, dict[str, str]]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
        "Referer": SESSION_WARMUP_URL,
        "X-Requested-With": "XMLHttpRequest",
    }

    snapshots: dict[str, dict[str, str]] = {}
    with httpx.Client(headers=headers, timeout=timeout, follow_redirects=True) as client:
        warm = client.get(SESSION_WARMUP_URL)
        warm.raise_for_status()

        for city_name, (lat, lon) in cities.items():
            snapshots[city_name] = {}
            for fuel_type in fuel_types:
                resp = client.post(
                    AJAX_URL,
                    data={
                        "lat": f"{lat:.4f}",
                        "lon": f"{lon:.4f}",
                        "type": fuel_type,
                        "lang": "bg",
                    },
                )
                resp.raise_for_status()
                snapshots[city_name][fuel_type] = resp.text
                if delay_sec > 0:
                    time.sleep(delay_sec)

    return snapshots


def save_bronze_ajax(
    snapshots: dict[str, dict[str, str]],
    ingestion_date: date,
    base_dir: Path = Path("data/bronze/fuels_ajax"),
) -> Path:
    out_dir = base_dir / f"ingestion_date={ingestion_date.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "snapshots.json"
    out_file.write_text(json.dumps(snapshots, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_file


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch AJAX fuel tables for Top 5 BG cities.")
    parser.add_argument("--date", default=date.today().isoformat(), help="Date in YYYY-MM-DD")
    parser.add_argument("--delay", type=float, default=0.2, help="Delay between POST requests in seconds")
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date)
    snapshots = fetch_ajax_snapshots(delay_sec=args.delay)
    out_file = save_bronze_ajax(snapshots, target_date)
    total_calls = sum(len(v) for v in snapshots.values())
    print(f"Saved {total_calls} AJAX fuel snapshots for {target_date} to {out_file}")


if __name__ == "__main__":
    main()
