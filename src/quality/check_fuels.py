from __future__ import annotations

from pathlib import Path

import duckdb

FUEL_PRICE_BOUNDS: dict[str, tuple[float, float]] = {
    "lpg": (0.35, 1.50),
    "cng": (0.60, 3.00),
    "gasoline_95": (0.90, 2.50),
    "gasoline_98": (0.95, 2.80),
    "gasoline_100": (1.00, 2.90),
    "diesel": (0.95, 2.70),
    "diesel_premium": (1.00, 3.00),
}
DEFAULT_BOUNDS: tuple[float, float] = (0.35, 3.50)


def validate_silver_fuel_prices(
    duckdb_path: Path = Path("warehouse/city_cost.duckdb"),
    min_rows: int = 1,
) -> dict[str, int | str]:
    with duckdb.connect(str(duckdb_path), read_only=True) as con:
        total_row = con.execute("SELECT COUNT(*) FROM silver_fuel_prices").fetchone()
        total_rows = int(total_row[0]) if total_row is not None else 0
        if total_rows < min_rows:
            raise ValueError(f"DQ failed: expected at least {min_rows} rows, got {total_rows}")

        null_row = con.execute(
            """
            SELECT COUNT(*)
            FROM silver_fuel_prices
            WHERE ingestion_date IS NULL
               OR city IS NULL
               OR fuel_type IS NULL
               OR price_eur IS NULL
            """
        ).fetchone()
        null_critical = int(null_row[0]) if null_row is not None else 0
        if null_critical > 0:
            raise ValueError(f"DQ failed: found {null_critical} rows with NULL critical fields")

        rows = con.execute(
            """
            SELECT city, fuel_type, price_eur
            FROM silver_fuel_prices
            """
        ).fetchall()

        bad_rows: list[tuple[str, str, float, float, float]] = []
        for city, fuel_type, price_eur in rows:
            lo, hi = FUEL_PRICE_BOUNDS.get(str(fuel_type), DEFAULT_BOUNDS)
            if not (lo <= float(price_eur) <= hi):
                bad_rows.append((str(city), str(fuel_type), float(price_eur), lo, hi))

        if bad_rows:
            sample = bad_rows[:5]
            raise ValueError(
                f"DQ failed: found {len(bad_rows)} rows outside per-fuel price bounds. Sample: {sample}"
            )

        distinct_row = con.execute(
            "SELECT COUNT(DISTINCT city) FROM silver_fuel_prices"
        ).fetchone()
        distinct_cities = int(distinct_row[0]) if distinct_row is not None else 0

    return {
        "total_rows": total_rows,
        "distinct_cities": distinct_cities,
        "bounds_mode": "per_fuel_type",
    }


if __name__ == "__main__":
    summary = validate_silver_fuel_prices()
    print("DQ passed:", summary)
