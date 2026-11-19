from __future__ import annotations

from pathlib import Path

import duckdb


def validate_silver_fuel_prices(
    duckdb_path: Path = Path("warehouse/city_cost.duckdb"),
    min_price_eur: float = 0.80,
    max_price_eur: float = 3.50,
    min_rows: int = 1,
) -> dict[str, int | float]:
    with duckdb.connect(str(duckdb_path), read_only=True) as con:
        total_rows = con.execute("SELECT COUNT(*) FROM silver_fuel_prices").fetchone()[0]
        if total_rows < min_rows:
            raise ValueError(f"DQ failed: expected at least {min_rows} rows, got {total_rows}")

        null_critical = con.execute(
            """
            SELECT COUNT(*)
            FROM silver_fuel_prices
            WHERE ingestion_date IS NULL
               OR city IS NULL
               OR fuel_type IS NULL
               OR price_eur IS NULL
            """
        ).fetchone()[0]
        if null_critical > 0:
            raise ValueError(f"DQ failed: found {null_critical} rows with NULL critical fields")

        out_of_range = con.execute(
            """
            SELECT COUNT(*)
            FROM silver_fuel_prices
            WHERE price_eur < ? OR price_eur > ?
            """,
            [min_price_eur, max_price_eur],
        ).fetchone()[0]
        if out_of_range > 0:
            raise ValueError(
                f"DQ failed: found {out_of_range} rows outside [{min_price_eur}, {max_price_eur}] EUR"
            )

        distinct_cities = con.execute(
            "SELECT COUNT(DISTINCT city) FROM silver_fuel_prices"
        ).fetchone()[0]

    return {
        "total_rows": total_rows,
        "distinct_cities": distinct_cities,
        "min_price_eur": min_price_eur,
        "max_price_eur": max_price_eur,
    }


if __name__ == "__main__":
    summary = validate_silver_fuel_prices()
    print("DQ passed:", summary)