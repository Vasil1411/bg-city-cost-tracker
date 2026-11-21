from __future__ import annotations

from pathlib import Path

import duckdb

from src.transform.build_gold import build_gold


def test_build_gold_rents_and_fuels(tmp_path: Path) -> None:
    run_date = "2026-11-19"
    db_path = tmp_path / "city_cost.duckdb"
    gold_dir = tmp_path / "gold"

    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            CREATE TABLE silver_rents_kolkostruva (
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
        )
        con.execute(
            """
            CREATE TABLE silver_rents_domly (
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
        )
        con.execute(
            """
            CREATE TABLE silver_fuel_prices (
                ingestion_date VARCHAR,
                city VARCHAR,
                station_name VARCHAR,
                fuel_type VARCHAR,
                price_eur DOUBLE
            )
            """
        )

        con.execute(
            """
            INSERT INTO silver_rents_kolkostruva VALUES
                (?, 'kolkostruva', 'sofiya', '  София ', 'София-град', 650.0, 2500.0, 175000.0, 'https://example.com/1'),
                (?, 'kolkostruva', 'plovdiv', 'Пловдив', 'Пловдив', 500.0, 1500.0, 105000.0, 'https://example.com/2')
            """,
            [run_date, run_date],
        )
        con.execute(
            """
            INSERT INTO silver_rents_domly VALUES
                (?, 'domly', 'sofia', 'София', 'EUR', 12000, 700.0, 9.5, '2026-11-19T15:32:52+00:00'),
                (?, 'domly', 'varna', 'Варна', 'EUR', 8000, 600.0, 8.6, '2026-11-19T15:28:13+00:00')
            """,
            [run_date, run_date],
        )
        con.execute(
            """
            INSERT INTO silver_fuel_prices VALUES
                (?, 'София', 'Station A', 'A95', 1.30),
                (?, 'София', 'Station B', 'A95', 1.40),
                (?, 'Пловдив', 'Station C', 'DIESEL', 1.35)
            """,
            [run_date, run_date, run_date],
        )
    finally:
        con.close()

    counts = build_gold(ingestion_date=run_date, gold_dir=gold_dir, db_path=db_path)
    assert counts == {"city_rents": 3, "city_fuels": 2}
    assert (gold_dir / "city_rents.parquet").exists()
    assert (gold_dir / "city_fuels.parquet").exists()

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        sofia_row = con.execute(
            """
            SELECT rent_two_room_eur, rent_median_eur, diff_two_room_vs_median_eur, has_kolkostruva, has_domly
            FROM gold_city_rents
            WHERE ingestion_date = ? AND city_key = 'софия'
            """,
            [run_date],
        ).fetchone()
        assert sofia_row == (650.0, 700.0, -50.0, True, True)

        varna_row = con.execute(
            """
            SELECT rent_two_room_eur, rent_median_eur, diff_two_room_vs_median_eur, has_kolkostruva, has_domly
            FROM gold_city_rents
            WHERE ingestion_date = ? AND city_key = 'варна'
            """,
            [run_date],
        ).fetchone()
        assert varna_row == (None, 600.0, None, False, True)

        plovdiv_row = con.execute(
            """
            SELECT rent_two_room_eur, rent_median_eur, diff_two_room_vs_median_eur, has_kolkostruva, has_domly
            FROM gold_city_rents
            WHERE ingestion_date = ? AND city_key = 'пловдив'
            """,
            [run_date],
        ).fetchone()
        assert plovdiv_row == (500.0, None, None, True, False)

        sofia_fuel = con.execute(
            """
            SELECT avg_price_eur, min_price_eur, max_price_eur, stations_count
            FROM gold_city_fuels
            WHERE ingestion_date = ? AND city_key = 'софия' AND fuel_type = 'A95'
            """,
            [run_date],
        ).fetchone()
        assert sofia_fuel == (1.35, 1.3, 1.4, 2)
    finally:
        con.close()