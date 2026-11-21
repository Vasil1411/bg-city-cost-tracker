from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import polars as pl


def build_gold_tables(
    silver_fuels_path: Path = Path("data/silver/fuel_prices.parquet"),
    gold_dir: Path = Path("data/gold"),
    duckdb_path: Path = Path("warehouse/city_cost.duckdb"),
) -> pl.DataFrame:
    if not silver_fuels_path.exists():
        raise FileNotFoundError(f"Silver fuels parquet not found: {silver_fuels_path}")

    gold_dir.mkdir(parents=True, exist_ok=True)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    fuels_df = pl.read_parquet(silver_fuels_path)
    if fuels_df.is_empty():
        raise ValueError("Silver fuels dataset is empty.")

    city_fuels_gold = (
        fuels_df.group_by(["ingestion_date", "city", "fuel_type"])
        .agg(
            pl.col("price_eur").mean().round(4).alias("avg_price_eur"),
            pl.col("price_eur").min().round(4).alias("min_price_eur"),
            pl.col("price_eur").max().round(4).alias("max_price_eur"),
            (pl.col("price_eur").max() - pl.col("price_eur").min()).round(4).alias("spread_eur"),
            pl.len().alias("stations_count"),
        )
        .sort(["ingestion_date", "city", "fuel_type"])
    )

    gold_parquet = gold_dir / "city_fuels.parquet"
    gold_csv = gold_dir / "gold_city_fuels.csv"

    city_fuels_gold.write_parquet(gold_parquet)
    city_fuels_gold.write_csv(gold_csv)

    with duckdb.connect(str(duckdb_path)) as con:
        con.execute("DROP TABLE IF EXISTS gold_city_fuels")
        con.register("gold_fuels_arrow", city_fuels_gold.to_arrow())
        con.execute("CREATE TABLE gold_city_fuels AS SELECT * FROM gold_fuels_arrow")

    return city_fuels_gold


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Gold analytics table for fuels only.")
    parser.add_argument("--silver-fuels", default="data/silver/fuel_prices.parquet")
    parser.add_argument("--gold-dir", default="data/gold")
    parser.add_argument("--duckdb", default="warehouse/city_cost.duckdb")
    args = parser.parse_args()

    df = build_gold_tables(
        silver_fuels_path=Path(args.silver_fuels),
        gold_dir=Path(args.gold_dir),
        duckdb_path=Path(args.duckdb),
    )
    print(f"Built gold_city_fuels with {len(df)} rows.")
    print(df.head(10))


if __name__ == "__main__":
    main()
