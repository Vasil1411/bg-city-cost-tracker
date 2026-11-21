from __future__ import annotations

import argparse
import json
import re
from datetime import date
from pathlib import Path

import duckdb
import polars as pl
from bs4 import BeautifulSoup

FUEL_TYPE_MAP: dict[str, str] = {
    "gasoline": "gasoline_95",
    "diesel": "diesel",
    "lpg": "lpg",
    "methane": "cng",
    "gasoline98": "gasoline_98",
    "gasoline98plus": "gasoline_100",
    "dieselplus": "diesel_premium",
}

# ??????????? ?????? ??????? (EUR/L ??? EUR/kg ?? CNG) ?? ???????????? ??? ??????
FUEL_PRICE_BOUNDS: dict[str, tuple[float, float]] = {
    "lpg": (0.35, 1.50),
    "cng": (0.60, 3.00),
    "gasoline_95": (0.90, 2.50),
    "gasoline_98": (0.95, 2.80),
    "gasoline_100": (1.00, 2.90),
    "diesel": (0.95, 2.70),
    "diesel_premium": (1.00, 3.00),
}
DEFAULT_PRICE_BOUNDS: tuple[float, float] = (0.20, 5.00)

PRICE_RE = re.compile(r"([+-]?\d+[.,]\d{2,3})\s*?")
DIST_RE = re.compile(r"(\d+[.,]\d+)\s*km", re.IGNORECASE)
STATION_ID_RE = re.compile(r"/gasstation/id/(\d+)")
SETTLEMENT_ID_RE = re.compile(r"/settlement/(\d+)")


def parse_ajax_snapshots(
    snapshots: dict[str, dict[str, str]],
    ingestion_date: date,
    max_distance_km: float = 15.0,
) -> tuple[list[dict], int]:
    records: list[dict] = []
    dropped_outliers = 0

    for requested_city, by_fuel in snapshots.items():
        for fuel_key, html_snippet in by_fuel.items():
            if not html_snippet or "<table" not in html_snippet.lower():
                continue

            normalized_fuel = FUEL_TYPE_MAP.get(fuel_key, fuel_key)
            min_p, max_p = FUEL_PRICE_BOUNDS.get(normalized_fuel, DEFAULT_PRICE_BOUNDS)

            soup = BeautifulSoup(html_snippet, "html.parser")
            for tr in soup.select("tbody tr"):
                tds = tr.find_all("td")
                if len(tds) < 6:
                    continue

                brand_td, _, city_td, addr_td, dist_td, price_td = tds[:6]

                station_name = " ".join(brand_td.get_text(" ", strip=True).split())
                station_link = brand_td.find("a", href=True)
                station_id: int | None = None
                if station_link:
                    m_sid = STATION_ID_RE.search(station_link["href"])
                    if m_sid:
                        station_id = int(m_sid.group(1))

                reported_city = " ".join(city_td.get_text(" ", strip=True).split()) or requested_city
                city_link = city_td.find("a", href=True)
                settlement_id: int | None = None
                if city_link:
                    m_set = SETTLEMENT_ID_RE.search(city_link["href"])
                    if m_set:
                        settlement_id = int(m_set.group(1))

                address = " ".join(addr_td.get_text(" ", strip=True).split())

                dist_text = " ".join(dist_td.get_text(" ", strip=True).split())
                m_dist = DIST_RE.search(dist_text)
                distance_km = float(m_dist.group(1).replace(",", ".")) if m_dist else None
                if distance_km is not None and distance_km > max_distance_km:
                    continue

                price_text = " ".join(price_td.get_text(" ", strip=True).split())
                m_price = PRICE_RE.search(price_text)
                if not m_price:
                    continue
                price_eur = float(m_price.group(1).replace(",", "."))
                if not (min_p <= price_eur <= max_p):
                    dropped_outliers += 1
                    continue

                records.append(
                    {
                        "ingestion_date": ingestion_date.isoformat(),
                        "city": requested_city,
                        "reported_city": reported_city,
                        "settlement_id": settlement_id,
                        "station_id": station_id,
                        "station_name": station_name,
                        "address": address,
                        "distance_km": distance_km,
                        "fuel_type": normalized_fuel,
                        "source_fuel_key": fuel_key,
                        "price_eur": price_eur,
                    }
                )

    deduped: list[dict] = []
    seen: set[tuple] = set()
    for r in records:
        key = (r["ingestion_date"], r["city"], r["fuel_type"], r["station_id"] or r["station_name"])
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    return deduped, dropped_outliers


def save_silver_ajax(
    records: list[dict],
    ingestion_date: date,
    silver_parquet_path: Path = Path("data/silver/fuel_prices.parquet"),
    duckdb_path: Path = Path("warehouse/city_cost.duckdb"),
) -> pl.DataFrame:
    silver_parquet_path.parent.mkdir(parents=True, exist_ok=True)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    schema = {
        "ingestion_date": pl.Utf8,
        "city": pl.Utf8,
        "reported_city": pl.Utf8,
        "settlement_id": pl.Int64,
        "station_id": pl.Int64,
        "station_name": pl.Utf8,
        "address": pl.Utf8,
        "distance_km": pl.Float64,
        "fuel_type": pl.Utf8,
        "source_fuel_key": pl.Utf8,
        "price_eur": pl.Float64,
    }
    new_df = pl.DataFrame(records, schema=schema).with_columns(
        pl.col("ingestion_date").str.to_date()
    )

    if silver_parquet_path.exists():
        existing_df = pl.read_parquet(silver_parquet_path)
        common_cols = list(new_df.columns)
        existing_aligned = existing_df.select(
            [
                pl.col(c).cast(new_df.schema[c])
                if c in existing_df.columns
                else pl.lit(None).cast(new_df.schema[c]).alias(c)
                for c in common_cols
            ]
        ).filter(pl.col("ingestion_date") != pl.lit(ingestion_date))
        combined_df = pl.concat([existing_aligned, new_df], how="diagonal_relaxed")
    else:
        combined_df = new_df

    combined_df.write_parquet(silver_parquet_path)

    with duckdb.connect(str(duckdb_path)) as con:
        con.execute("DROP TABLE IF EXISTS silver_fuel_prices")
        con.register("silver_fuels_arrow", combined_df.to_arrow())
        con.execute("CREATE TABLE silver_fuel_prices AS SELECT * FROM silver_fuels_arrow")

    return combined_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse AJAX fuel snapshots into Silver & DuckDB.")
    parser.add_argument("--date", default=date.today().isoformat(), help="Date in YYYY-MM-DD")
    parser.add_argument("--max-distance-km", type=float, default=15.0, help="Max distance in km from city center")
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date)
    bronze_file = Path(f"data/bronze/fuels_ajax/ingestion_date={target_date.isoformat()}/snapshots.json")
    if not bronze_file.exists():
        raise FileNotFoundError(f"AJAX Bronze file not found: {bronze_file}")

    snapshots = json.loads(bronze_file.read_text(encoding="utf-8"))
    records, dropped_outliers = parse_ajax_snapshots(
        snapshots=snapshots,
        ingestion_date=target_date,
        max_distance_km=args.max_distance_km,
    )
    if not records:
        raise ValueError("Parsed 0 valid fuel records from AJAX snapshots.")

    df = save_silver_ajax(records, target_date)
    print(
        f"Parsed {len(records)} clean fuel rows for {target_date} "
        f"(filtered {dropped_outliers} price outliers). Total Silver rows: {len(df)}"
    )


if __name__ == "__main__":
    main()
