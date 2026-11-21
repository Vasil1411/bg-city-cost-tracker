from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import polars as pl

from src.transform.build_gold import build_gold_tables


def test_build_gold_fuels_only(tmp_path: Path) -> None:
    run_date = date(2026, 11, 19)
    silver_path = tmp_path / "fuel_prices.parquet"
    gold_dir = tmp_path / "gold"
    db_path = tmp_path / "city_cost.duckdb"

    df = pl.DataFrame(
        {
            "ingestion_date": [run_date, run_date, run_date],
            "city": ["?????", "?????", "???????"],
            "station_name": ["Station A", "Station B", "Station C"],
            "fuel_type": ["gasoline_95", "gasoline_95", "diesel"],
            "price_eur": [1.30, 1.40, 1.35],
        }
    )
    df.write_parquet(silver_path)

    gold_df = build_gold_tables(
        silver_fuels_path=silver_path,
        gold_dir=gold_dir,
        duckdb_path=db_path,
    )

    assert len(gold_df) == 2
    assert (gold_dir / "city_fuels.parquet").exists()
    assert (gold_dir / "gold_city_fuels.csv").exists()

    with duckdb.connect(str(db_path), read_only=True) as con:
        sofia_row = con.execute(
            """
            SELECT avg_price_eur, min_price_eur, max_price_eur, spread_eur, stations_count
            FROM gold_city_fuels
            WHERE city = '?????' AND fuel_type = 'gasoline_95'
            """
        ).fetchone()
        assert sofia_row == (1.35, 1.3, 1.4, 0.1, 2)
