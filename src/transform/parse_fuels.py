from __future__ import annotations

import argparse
import re
from datetime import date
from pathlib import Path

import duckdb
import polars as pl
from bs4 import BeautifulSoup

PRICE_RE = re.compile(r"(\d+[.,]\d{2,3})\s*€/л")
FUEL_RE = re.compile(r"предлага\s+(.+?)\s+за\s+\d+[.,]\d{2,3}\s*€/л", re.IGNORECASE)
UPDATED_RE = re.compile(r"(Цената е актуална[^\n|]+)")


def normalize_fuel_type(raw_fuel: str | None) -> str:
    if not raw_fuel:
        return "unknown"
    cleaned = raw_fuel.strip().lower()
    if "a95" in cleaned or "а95" in cleaned:
        return "gasoline_95"
    if "a98" in cleaned or "а98" in cleaned or "100" in cleaned:
        return "gasoline_98_100"
    if "diesel" in cleaned or "дизел" in cleaned:
        return "diesel"
    if "lpg" in cleaned or "пропан" in cleaned or "газ" in cleaned:
        return "lpg"
    if "cng" in cleaned or "метан" in cleaned:
        return "cng"
    return cleaned


def parse_fuel_html(html: str, ingestion_date: date) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []

    for row_div in soup.find_all("div", class_="row"):
        # Пропускаме външни row контейнери, които съдържат други row елементи
        if row_div.find("div", class_="row"):
            continue

        col8 = row_div.find("div", class_="col-md-8")
        if not col8:
            continue

        full_text = " ".join(col8.get_text(" ", strip=True).split())
        if "€/л" not in full_text or "предлага" not in full_text.lower():
            continue

        price_match = PRICE_RE.search(full_text)
        if not price_match:
            continue

        price_eur = float(price_match.group(1).replace(",", "."))

        prefix = full_text.split("предлага", 1)[0]
        prefix = re.sub(r"^грешна ценa\s*\?\s*", "", prefix, flags=re.IGNORECASE).strip()
        parts = [p.strip(" ,") for p in prefix.split(",") if p.strip(" ,")]

        station_and_city = parts[0] if parts else prefix
        tokens = station_and_city.split()
        city = tokens[-1] if len(tokens) >= 2 else "unknown"
        station_name = " ".join(tokens[:-1]) if len(tokens) >= 2 else station_and_city

        # Филтър срещу случайни заглавия/текстове, сгрешени като град
        if city.isdigit() or len(city) > 25 or city == "unknown":
            continue

        fuel_match = FUEL_RE.search(full_text)
        raw_fuel = fuel_match.group(1).strip() if fuel_match else None
        fuel_type = normalize_fuel_type(raw_fuel)

        updated_match = UPDATED_RE.search(full_text)
        updated_text = updated_match.group(1).strip() if updated_match else None

        rows.append(
            {
                "ingestion_date": ingestion_date.isoformat(),
                "city": city,
                "station_name": station_name,
                "fuel_type": fuel_type,
                "raw_fuel_label": raw_fuel,
                "price_eur": price_eur,
                "updated_text": updated_text,
            }
        )

    # Дедупликация в рамките на един снапшот
    unique_rows: list[dict] = []
    seen: set[tuple] = set()
    for r in rows:
        key = (
            r["ingestion_date"],
            r["city"],
            r["station_name"],
            r["fuel_type"],
            r["price_eur"],
            r["updated_text"],
        )
        if key not in seen:
            seen.add(key)
            unique_rows.append(r)

    return unique_rows


def save_silver_and_duckdb(
    records: list[dict],
    ingestion_date: date,
    silver_parquet_path: Path = Path("data/silver/fuel_prices.parquet"),
    duckdb_path: Path = Path("warehouse/city_cost.duckdb"),
) -> pl.DataFrame:
    silver_parquet_path.parent.mkdir(parents=True, exist_ok=True)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    new_df = pl.DataFrame(records).with_columns(
        pl.col("ingestion_date").str.to_date()
    )

    # Idempotent merge по ingestion_date в Parquet
    if silver_parquet_path.exists():
        existing_df = pl.read_parquet(silver_parquet_path)
        existing_filtered = existing_df.filter(
            pl.col("ingestion_date") != pl.lit(ingestion_date)
        )
        combined_df = pl.concat([existing_filtered, new_df], how="diagonal_relaxed")
    else:
        combined_df = new_df

    combined_df.write_parquet(silver_parquet_path)

    # Зареждане в DuckDB
    with duckdb.connect(str(duckdb_path)) as con:
        con.execute("CREATE TABLE IF NOT EXISTS silver_fuel_prices AS SELECT * FROM combined_df LIMIT 0")
        con.execute("DELETE FROM silver_fuel_prices WHERE ingestion_date = ?", [ingestion_date.isoformat()])
        con.register("df_to_load", new_df.to_arrow())
        con.execute("INSERT INTO silver_fuel_prices SELECT * FROM df_to_load")

    return combined_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse Bronze fuel HTML into Silver & DuckDB.")
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Snapshot date in YYYY-MM-DD format",
    )
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date)
    bronze_file = Path(f"data/bronze/fuels/ingestion_date={target_date.isoformat()}/snapshot.html")
    if not bronze_file.exists():
        raise FileNotFoundError(f"Bronze file not found: {bronze_file}")

    html = bronze_file.read_text(encoding="utf-8")
    records = parse_fuel_html(html, target_date)
    if not records:
        raise ValueError("Parsed 0 fuel records — check HTML structure.")

    df = save_silver_and_duckdb(records, target_date)
    print(f"Parsed {len(records)} rows for {target_date}. Total Silver rows: {len(df)}")
    print(df.head(5))


if __name__ == "__main__":
    main()