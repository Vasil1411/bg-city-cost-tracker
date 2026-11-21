from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from src.quality.check_rents import check_rents_quality
from src.transform.parse_rents import parse_domly, parse_kolkostruva, run_transform


def test_parse_kolkostruva_filters_and_cleans(tmp_path: Path) -> None:
    csv_file = tmp_path / "ceni.csv"
    csv_file.write_text(
        "\n".join(
            [
                "slug,kind,name,oblast,apt_eur_m2,two_room_price,rent_two_room,url",
                "sofiya,град,София,София-град,2500,170000,630,https://example.com/sofiya",
                "sofiya,град,София,София-град,2550,175000,\"650,0\",https://example.com/sofiya",
                "nesebar,гр.,Несебър,Бургас,1800,110000,500,https://example.com/nesebar",
                "broken-city,град,Тестград,Пловдив,1200,90000,0,https://example.com/broken",
                "empty-city,град,Празенград,Варна,1300,95000,,https://example.com/empty",
            ]
        ),
        encoding="utf-8",
    )

    rows = parse_kolkostruva(csv_file, "2026-11-19")
    assert len(rows) == 1
    assert rows[0]["slug"] == "sofiya"
    assert rows[0]["city_name"] == "София"
    assert rows[0]["rent_two_room"] == 650.0


def test_parse_domly_extracts_and_filters(tmp_path: Path) -> None:
    json_file = tmp_path / "stats.json"
    payload = {
        "data": [
            {
                "slug": "sofia",
                "name": "София",
                "currency": "EUR",
                "rent": {"listings": 12000, "median": 700, "median_per_m2": 9.5},
                "updated_at": "2026-11-19T15:32:52+00:00",
            },
            {
                "slug": "invalid-city",
                "name": "Грешен",
                "currency": "EUR",
                "rent": {"listings": 10, "median": None, "median_per_m2": None},
                "updated_at": "2026-11-19T15:32:52+00:00",
            },
        ]
    }
    json_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    rows = parse_domly(json_file, "2026-11-19")
    assert len(rows) == 1
    assert rows[0]["slug"] == "sofia"
    assert rows[0]["rent_median"] == 700.0
    assert rows[0]["rent_listings"] == 12000
    assert rows[0]["source_updated_at"] == "2026-11-19T15:32:52+00:00"


def test_run_transform_and_dq_pass_and_fail(tmp_path: Path) -> None:
    run_date = "2026-11-19"
    bronze_dir = tmp_path / "bronze" / "rents"
    kk_dir = bronze_dir / "kolkostruva" / f"ingestion_date={run_date}"
    dm_dir = bronze_dir / "domly" / f"ingestion_date={run_date}"
    kk_dir.mkdir(parents=True)
    dm_dir.mkdir(parents=True)

    (kk_dir / "ceni.csv").write_text(
        "slug,kind,name,oblast,apt_eur_m2,two_room_price,rent_two_room,url\n"
        "plovdiv,град,Пловдив,Пловдив,1500,105000,490,https://example.com/plovdiv\n",
        encoding="utf-8",
    )
    (dm_dir / "stats.json").write_text(
        json.dumps(
            {
                "data": [
                    {
                        "slug": "plovdiv",
                        "name": "Пловдив",
                        "currency": "EUR",
                        "rent": {"listings": 5000, "median": 500, "median_per_m2": 7.1},
                        "updated_at": "2026-11-19T15:41:04+00:00",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    db_path = tmp_path / "city_cost.duckdb"
    silver_dir = tmp_path / "silver"

    counts = run_transform(
        ingestion_date=run_date,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        db_path=db_path,
    )
    assert counts == {"kolkostruva": 1, "domly": 1}
    assert (silver_dir / "rents_kolkostruva.parquet").exists()
    assert (silver_dir / "rents_domly.parquet").exists()

    dq_counts = check_rents_quality(ingestion_date=run_date, db_path=db_path)
    assert dq_counts == {"kolkostruva": 1, "domly": 1}

    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            "UPDATE silver_rents_kolkostruva SET rent_two_room = 999999 WHERE ingestion_date = ?",
            [run_date],
        )
    finally:
        con.close()

    with pytest.raises(ValueError, match="out-of-range"):
        check_rents_quality(ingestion_date=run_date, db_path=db_path)