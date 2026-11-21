from datetime import date
from pathlib import Path
import httpx

SOURCES = {
    "kolkostruva": ("https://kolkostruvamoiaimot.com/danni/ceni.csv", "ceni.csv"),
    "domly": ("https://getdomly.com/api/v1/stats", "stats.json"),
}


def fetch_rents(ingestion_date: str | None = None, base_dir: Path = Path("data/bronze/rents")) -> dict[str, Path]:
    run_date = ingestion_date or date.today().isoformat()
    saved_paths: dict[str, Path] = {}

    with httpx.Client(timeout=20.0, follow_redirects=True) as client:
        for source_name, (url, filename) in SOURCES.items():
            response = client.get(url)
            response.raise_for_status()

            target_dir = base_dir / source_name / f"ingestion_date={run_date}"
            target_dir.mkdir(parents=True, exist_ok=True)
            target_file = target_dir / filename
            target_file.write_bytes(response.content)
            saved_paths[source_name] = target_file
            print(f"[{source_name}] Saved {len(response.content)} bytes -> {target_file}")

    return saved_paths


if __name__ == "__main__":
    fetch_rents()
