from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb


def _scalar_int(con: duckdb.DuckDBPyConnection, sql: str, params: list[object]) -> int:
    row = con.execute(sql, params).fetchone()
    if row is None or row[0] is None:
        return 0
    return int(row[0])


def check_rents_quality(
    ingestion_date: str | None = None,
    db_path: Path = Path("warehouse/city_cost.duckdb"),
) -> dict[str, int]:
    run_date = ingestion_date or date.today().isoformat()
    con = duckdb.connect(str(db_path), read_only=True)

    try:
        kk_count = _scalar_int(
            con,
            "SELECT COUNT(*) FROM silver_rents_kolkostruva WHERE ingestion_date = ?",
            [run_date],
        )
        if kk_count == 0:
            raise ValueError(f"DQ failed: silver_rents_kolkostruva has 0 rows for {run_date}")

        kk_bad_nulls = _scalar_int(
            con,
            """
            SELECT COUNT(*) FROM silver_rents_kolkostruva
            WHERE ingestion_date = ?
              AND (
                  slug IS NULL OR TRIM(slug) = ''
                  OR city_name IS NULL OR TRIM(city_name) = ''
                  OR rent_two_room IS NULL
              )
            """,
            [run_date],
        )
        if kk_bad_nulls > 0:
            raise ValueError(
                f"DQ failed: silver_rents_kolkostruva has {kk_bad_nulls} rows with NULL/empty keys or rent"
            )

        kk_dupes = _scalar_int(
            con,
            """
            SELECT COUNT(*) FROM (
                SELECT ingestion_date, slug, COUNT(*) AS c
                FROM silver_rents_kolkostruva
                WHERE ingestion_date = ?
                GROUP BY ingestion_date, slug
                HAVING c > 1
            )
            """,
            [run_date],
        )
        if kk_dupes > 0:
            raise ValueError(
                f"DQ failed: silver_rents_kolkostruva has {kk_dupes} duplicate (ingestion_date, slug) keys"
            )

        kk_out_of_range = _scalar_int(
            con,
            """
            SELECT COUNT(*) FROM silver_rents_kolkostruva
            WHERE ingestion_date = ?
              AND (rent_two_room < 50 OR rent_two_room > 5000)
            """,
            [run_date],
        )
        if kk_out_of_range > 0:
            raise ValueError(
                f"DQ failed: silver_rents_kolkostruva has {kk_out_of_range} out-of-range rent_two_room rows"
            )

        dm_count = _scalar_int(
            con,
            "SELECT COUNT(*) FROM silver_rents_domly WHERE ingestion_date = ?",
            [run_date],
        )
        if dm_count == 0:
            raise ValueError(f"DQ failed: silver_rents_domly has 0 rows for {run_date}")

        dm_bad_nulls = _scalar_int(
            con,
            """
            SELECT COUNT(*) FROM silver_rents_domly
            WHERE ingestion_date = ?
              AND (
                  slug IS NULL OR TRIM(slug) = ''
                  OR city_name IS NULL OR TRIM(city_name) = ''
                  OR rent_median IS NULL
              )
            """,
            [run_date],
        )
        if dm_bad_nulls > 0:
            raise ValueError(
                f"DQ failed: silver_rents_domly has {dm_bad_nulls} rows with NULL/empty keys or rent_median"
            )

        dm_dupes = _scalar_int(
            con,
            """
            SELECT COUNT(*) FROM (
                SELECT ingestion_date, slug, COUNT(*) AS c
                FROM silver_rents_domly
                WHERE ingestion_date = ?
                GROUP BY ingestion_date, slug
                HAVING c > 1
            )
            """,
            [run_date],
        )
        if dm_dupes > 0:
            raise ValueError(
                f"DQ failed: silver_rents_domly has {dm_dupes} duplicate (ingestion_date, slug) keys"
            )

        dm_out_of_range = _scalar_int(
            con,
            """
            SELECT COUNT(*) FROM silver_rents_domly
            WHERE ingestion_date = ?
              AND (
                  rent_median < 50 OR rent_median > 5000
                  OR (rent_median_per_m2 IS NOT NULL AND (rent_median_per_m2 < 1 OR rent_median_per_m2 > 100))
              )
            """,
            [run_date],
        )
        if dm_out_of_range > 0:
            raise ValueError(
                f"DQ failed: silver_rents_domly has {dm_out_of_range} out-of-range rent rows"
            )
    finally:
        con.close()

    print(f"DQ passed for rents ({run_date}): kolkostruva={kk_count}, domly={dm_count}")
    return {"kolkostruva": kk_count, "domly": dm_count}


if __name__ == "__main__":
    check_rents_quality("2026-11-19")