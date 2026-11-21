from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb


def _scalar_int(con: duckdb.DuckDBPyConnection, sql: str, params: list[object]) -> int:
    row = con.execute(sql, params).fetchone()
    if row is None or row[0] is None:
        return 0
    return int(row[0])


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0] and int(row[0]) > 0)


def build_gold(
    ingestion_date: str | None = None,
    gold_dir: Path = Path("data/gold"),
    db_path: Path = Path("warehouse/city_cost.duckdb"),
) -> dict[str, int]:
    run_date = ingestion_date or date.today().isoformat()
    gold_dir.mkdir(parents=True, exist_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS gold_city_rents (
                ingestion_date VARCHAR,
                city_key VARCHAR,
                city_name VARCHAR,
                oblast VARCHAR,
                kolkostruva_slug VARCHAR,
                domly_slug VARCHAR,
                rent_two_room_eur DOUBLE,
                rent_median_eur DOUBLE,
                rent_median_per_m2_eur DOUBLE,
                rent_listings BIGINT,
                diff_two_room_vs_median_eur DOUBLE,
                ratio_two_room_to_median DOUBLE,
                has_kolkostruva BOOLEAN,
                has_domly BOOLEAN,
                source_updated_at VARCHAR
            )
            """
        )

        con.execute("DELETE FROM gold_city_rents WHERE ingestion_date = ?", [run_date])

        con.execute(
            """
            INSERT INTO gold_city_rents (
                ingestion_date,
                city_key,
                city_name,
                oblast,
                kolkostruva_slug,
                domly_slug,
                rent_two_room_eur,
                rent_median_eur,
                rent_median_per_m2_eur,
                rent_listings,
                diff_two_room_vs_median_eur,
                ratio_two_room_to_median,
                has_kolkostruva,
                has_domly,
                source_updated_at
            )
            WITH kk AS (
                SELECT
                    ingestion_date,
                    LOWER(REGEXP_REPLACE(TRIM(city_name), '\\s+', ' ', 'g')) AS city_key,
                    TRIM(city_name) AS clean_city_name,
                    TRIM(oblast) AS clean_oblast,
                    slug,
                    rent_two_room
                FROM silver_rents_kolkostruva
                WHERE ingestion_date = ?
            ),
            dm AS (
                SELECT
                    ingestion_date,
                    LOWER(REGEXP_REPLACE(TRIM(city_name), '\\s+', ' ', 'g')) AS city_key,
                    TRIM(city_name) AS clean_city_name,
                    slug,
                    rent_median,
                    rent_median_per_m2,
                    rent_listings,
                    source_updated_at
                FROM silver_rents_domly
                WHERE ingestion_date = ?
            )
            SELECT
                COALESCE(kk.ingestion_date, dm.ingestion_date) AS ingestion_date,
                COALESCE(kk.city_key, dm.city_key) AS city_key,
                COALESCE(kk.clean_city_name, dm.clean_city_name) AS city_name,
                kk.clean_oblast AS oblast,
                kk.slug AS kolkostruva_slug,
                dm.slug AS domly_slug,
                kk.rent_two_room AS rent_two_room_eur,
                dm.rent_median AS rent_median_eur,
                dm.rent_median_per_m2 AS rent_median_per_m2_eur,
                dm.rent_listings,
                CASE
                    WHEN kk.rent_two_room IS NOT NULL AND dm.rent_median IS NOT NULL
                    THEN ROUND(kk.rent_two_room - dm.rent_median, 2)
                    ELSE NULL
                END AS diff_two_room_vs_median_eur,
                CASE
                    WHEN kk.rent_two_room IS NOT NULL AND dm.rent_median IS NOT NULL AND dm.rent_median > 0
                    THEN ROUND(kk.rent_two_room / dm.rent_median, 4)
                    ELSE NULL
                END AS ratio_two_room_to_median,
                (kk.city_key IS NOT NULL) AS has_kolkostruva,
                (dm.city_key IS NOT NULL) AS has_domly,
                dm.source_updated_at
            FROM kk
            FULL OUTER JOIN dm
                ON kk.city_key = dm.city_key
            ORDER BY 3 NULLS LAST
            """,
            [run_date, run_date],
        )

        rents_parquet = gold_dir / "city_rents.parquet"
        con.execute(
            "COPY (SELECT * FROM gold_city_rents WHERE ingestion_date = ?) TO ? (FORMAT PARQUET)",
            [run_date, str(rents_parquet)],
        )
        rents_count = _scalar_int(
            con,
            "SELECT COUNT(*) FROM gold_city_rents WHERE ingestion_date = ?",
            [run_date],
        )

        fuels_count = 0
        if _table_exists(con, "silver_fuel_prices"):
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS gold_city_fuels (
                    ingestion_date VARCHAR,
                    city_key VARCHAR,
                    city_name VARCHAR,
                    fuel_type VARCHAR,
                    avg_price_eur DOUBLE,
                    min_price_eur DOUBLE,
                    max_price_eur DOUBLE,
                    stations_count BIGINT
                )
                """
            )
            con.execute("DELETE FROM gold_city_fuels WHERE ingestion_date = ?", [run_date])
            con.execute(
                """
                INSERT INTO gold_city_fuels (
                    ingestion_date,
                    city_key,
                    city_name,
                    fuel_type,
                    avg_price_eur,
                    min_price_eur,
                    max_price_eur,
                    stations_count
                )
                SELECT
                    ingestion_date,
                    LOWER(REGEXP_REPLACE(TRIM(city), '\\s+', ' ', 'g')) AS city_key,
                    TRIM(city) AS clean_city_name,
                    fuel_type,
                    ROUND(AVG(price_eur), 4) AS avg_price_eur,
                    ROUND(MIN(price_eur), 4) AS min_price_eur,
                    ROUND(MAX(price_eur), 4) AS max_price_eur,
                    COUNT(*) AS stations_count
                FROM silver_fuel_prices
                WHERE ingestion_date = ?
                GROUP BY 1, 2, 3, 4
                ORDER BY 3, 4
                """,
                [run_date],
            )
            fuels_parquet = gold_dir / "city_fuels.parquet"
            con.execute(
                "COPY (SELECT * FROM gold_city_fuels WHERE ingestion_date = ?) TO ? (FORMAT PARQUET)",
                [run_date, str(fuels_parquet)],
            )
            fuels_count = _scalar_int(
                con,
                "SELECT COUNT(*) FROM gold_city_fuels WHERE ingestion_date = ?",
                [run_date],
            )
    finally:
        con.close()

    print(f"Gold city_rents rows ({run_date}): {rents_count}")
    if fuels_count > 0:
        print(f"Gold city_fuels rows ({run_date}): {fuels_count}")
    return {"city_rents": rents_count, "city_fuels": fuels_count}


if __name__ == "__main__":
    build_gold("2026-11-19")