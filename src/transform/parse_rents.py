from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

import duckdb


def _to_float(val: object) -> float | None:
    if val is None:
        return None
    s = str(val).strip().replace(" ", "").replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _to_int(val: object) -> int | None:
    num = _to_float(val)
    if num is None:
        return None
    return int(num)


def parse_kolkostruva(csv_path: Path, ingestion_date: str) -> list[dict[str, object]]:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    deduped: dict[tuple[str, str], dict[str, object]] = {}
    for row in rows:
        if (row.get("kind") or "").strip() != "град":
            continue
        rent_two_room = _to_float(row.get("rent_two_room"))
        if rent_two_room is None or rent_two_room <= 0:
            continue

        slug = (row.get("slug") or "").strip()
        key = (ingestion_date, slug)
        deduped[key] = {
            "ingestion_date": ingestion_date,
            "source": "kolkostruva",
            "slug": slug,
            "city_name": (row.get("name") or "").strip(),
            "oblast": (row.get("oblast") or "").strip(),
            "rent_two_room": rent_two_room,
            "apt_eur_m2": _to_float(row.get("apt_eur_m2")),
            "two_room_price": _to_float(row.get("two_room_price")),
            "url": (row.get("url") or "").strip(),
        }

    return list(deduped.values())


def parse_domly(json_path: Path, ingestion_date: str) -> list[dict[str, object]]:
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    items = payload["data"] if isinstance(payload, dict) and "data" in payload else payload

    deduped: dict[tuple[str, str], dict[str, object]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        rent = item.get("rent") or {}
        median_rent = _to_float(rent.get("median"))
        if median_rent is None or median_rent <= 0:
            continue

        slug = (item.get("slug") or "").strip()
        key = (ingestion_date, slug)
        deduped[key] = {
            "ingestion_date": ingestion_date,
            "source": "domly",
            "slug": slug,
            "city_name": (item.get("name") or "").strip(),
            "currency": (item.get("currency") or "EUR").strip(),
            "rent_listings": _to_int(rent.get("listings")),
            "rent_median": median_rent,
            "rent_median_per_m2": _to_float(rent.get("median_per_m2")),
            "source_updated_at": (item.get("updated_at") or "").strip(),
        }

    return list(deduped.values())


def _write_table_and_parquet(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    create_sql: str,
    insert_sql: str,
    rows: list[dict[str, object]],
    run_date: str,
    parquet_path: Path,
) -> None:
    con.execute(create_sql)
    con.execute(f"DELETE FROM {table_name} WHERE ingestion_date = ?", [run_date])
    if rows:
        con.executemany(insert_sql, rows)
    con.execute(f"COPY {table_name} TO ? (FORMAT PARQUET, OVERWRITE_OR_IGNORE)", [str(parquet_path)])


def run_transform(
    ingestion_date: str | None = None,
    bronze_dir: Path = Path("data/bronze/rents"),
    silver_dir: Path = Path("data/silver"),
    db_path: Path = Path("warehouse/city_cost.duckdb"),
) -> dict[str, int]:
    run_date = ingestion_date or date.today().isoformat()
    silver_dir.mkdir(parents=True, exist_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    csv_path = bronze_dir / "kolkostruva" / f"ingestion_date={run_date}" / "ceni.csv"
    json_path = bronze_dir / "domly" / f"ingestion_date={run_date}" / "stats.json"

    kk_rows = parse_kolkostruva(csv_path, run_date)
    dm_rows = parse_domly(json_path, run_date)

    kk_parquet = silver_dir / "rents_kolkostruva.parquet"
    dm_parquet = silver_dir / "rents_domly.parquet"

    kk_create = """
        CREATE TABLE IF NOT EXISTS silver_rents_kolkostruva (
            ingestion_date VARCHAR,
            source VARCHAR,
            slug VARCHAR,
            city_name VARCHAR,
            oblast VARCHAR,
            rent_two_room DOUBLE,
            apt_eur_m2 DOUBLE,
            two_room_price DOUBLE,
            url VARCHAR
        )
    """
    kk_insert = """
        INSERT INTO silver_rents_kolkostruva (
            ingestion_date, source, slug, city_name, oblast,
            rent_two_room, apt_eur_m2, two_room_price, url
        ) VALUES (
            $ingestion_date, $source, $slug, $city_name, $oblast,
            $rent_two_room, $apt_eur_m2, $two_room_price, $url
        )
    """

    dm_create = """
        CREATE TABLE IF NOT EXISTS silver_rents_domly (
            ingestion_date VARCHAR,
            source VARCHAR,
            slug VARCHAR,
            city_name VARCHAR,
            currency VARCHAR,
            rent_listings BIGINT,
            rent_median DOUBLE,
            rent_median_per_m2 DOUBLE,
            source_updated_at VARCHAR
        )
    """
    dm_insert = """
        INSERT INTO silver_rents_domly (
            ingestion_date, source, slug, city_name, currency,
            rent_listings, rent_median, rent_median_per_m2, source_updated_at
        ) VALUES (
            $ingestion_date, $source, $slug, $city_name, $currency,
            $rent_listings, $rent_median, $rent_median_per_m2, $source_updated_at
        )
    """

    con = duckdb.connect(str(db_path))
    try:
        _write_table_and_parquet(
            con, "silver_rents_kolkostruva", kk_create, kk_insert, kk_rows, run_date, kk_parquet
        )
        _write_table_and_parquet(
            con, "silver_rents_domly", dm_create, dm_insert, dm_rows, run_date, dm_parquet
        )
    finally:
        con.close()

    print(f"Silver kolkostruva rows: {len(kk_rows)} -> {kk_parquet} & silver_rents_kolkostruva")
    print(f"Silver domly rows: {len(dm_rows)} -> {dm_parquet} & silver_rents_domly")
    return {"kolkostruva": len(kk_rows), "domly": len(dm_rows)}


if __name__ == "__main__":
    run_transform()