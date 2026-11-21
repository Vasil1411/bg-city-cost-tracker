from __future__ import annotations

import argparse
import importlib
import inspect
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _call_matching(fn: Any, candidates: dict[str, Any]) -> Any:
    sig = inspect.signature(fn)
    kwargs: dict[str, Any] = {}
    for param_name, param in sig.parameters.items():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        if param_name in candidates:
            kwargs[param_name] = candidates[param_name]
        elif param.default is inspect.Parameter.empty:
            raise TypeError(f"Липсва задължителен аргумент '{param_name}' за '{fn.__name__}'.")
    return fn(**kwargs)


def _iter_dates(start_str: str, end_str: str) -> list[str]:
    start_dt = date.fromisoformat(start_str)
    end_dt = date.fromisoformat(end_str)
    if end_dt < start_dt:
        raise ValueError(f"Крайната дата ({end_str}) е преди началната ({start_str}).")

    dates: list[str] = []
    cur = start_dt
    while cur <= end_dt:
        dates.append(cur.isoformat())
        cur += timedelta(days=1)
    return dates


def _step_fetch_fuels(run_date: str, bronze_dir: Path) -> bool:
    mod = importlib.import_module("src.extract.fetch_fuels")
    fetch_fn = getattr(mod, "fetch_raw_fuel_page", None)
    save_fn = getattr(mod, "save_bronze_snapshot", None)
    if callable(fetch_fn) and callable(save_fn):
        parsed_date = date.fromisoformat(run_date)
        html = _call_matching(
            fetch_fn,
            {
                "url": "https://fuelo.net/cheap/where",
                "timeout": 20.0,
                "ingestion_date": parsed_date,
            },
        )
        _call_matching(
            save_fn,
            {
                "html": html,
                "ingestion_date": parsed_date,
                "base_dir": bronze_dir / "fuels",
            },
        )
        return True
    main_fn = getattr(mod, "main", None)
    if callable(main_fn):
        main_fn()
        return True
    return False


def _step_fetch_rents(run_date: str, bronze_dir: Path) -> bool:
    mod = importlib.import_module("src.extract.fetch_rents")
    fn = getattr(mod, "fetch_rents", None)
    if not callable(fn):
        return False
    _call_matching(
        fn,
        {
            "ingestion_date": run_date,
            "base_dir": bronze_dir / "rents",
        },
    )
    return True


def _step_parse_fuels(run_date: str, bronze_dir: Path, silver_dir: Path, db_path: Path) -> Any:
    mod = importlib.import_module("src.transform.parse_fuels")
    parse_fn = getattr(mod, "parse_fuel_html")
    save_fn = getattr(mod, "save_silver_and_duckdb")

    snapshot_path = bronze_dir / "fuels" / f"ingestion_date={run_date}" / "snapshot.html"
    if not snapshot_path.exists():
        raise FileNotFoundError(f"Липсва суров файл за горива: {snapshot_path}")

    html = snapshot_path.read_text(encoding="utf-8", errors="replace")
    parsed_date = date.fromisoformat(run_date)
    records = _call_matching(
        parse_fn,
        {
            "html": html,
            "ingestion_date": parsed_date,
        },
    )
    return _call_matching(
        save_fn,
        {
            "records": records,
            "ingestion_date": parsed_date,
            "silver_parquet_path": silver_dir / "fuel_prices.parquet",
            "duckdb_path": db_path,
        },
    )


def _step_check_fuels(db_path: Path) -> Any:
    mod = importlib.import_module("src.quality.check_fuels")
    fn = getattr(mod, "validate_silver_fuel_prices")
    return _call_matching(fn, {"duckdb_path": db_path})


def _step_parse_rents(run_date: str, db_path: Path) -> Any:
    mod = importlib.import_module("src.transform.parse_rents")
    fn = getattr(mod, "run_transform")
    return _call_matching(fn, {"ingestion_date": run_date, "db_path": db_path})


def _step_check_rents(run_date: str, db_path: Path) -> Any:
    mod = importlib.import_module("src.quality.check_rents")
    fn = getattr(mod, "check_rents_quality")
    return _call_matching(fn, {"ingestion_date": run_date, "db_path": db_path})


def _step_build_gold(run_date: str, gold_dir: Path, db_path: Path) -> Any:
    mod = importlib.import_module("src.transform.build_gold")
    fn = getattr(mod, "build_gold")
    return _call_matching(
        fn,
        {
            "ingestion_date": run_date,
            "gold_dir": gold_dir,
            "db_path": db_path,
        },
    )


def run_for_date(
    run_date: str,
    skip_fetch: bool = False,
    skip_fuels: bool = False,
    skip_rents: bool = False,
    bronze_dir: Path = Path("data/bronze"),
    silver_dir: Path = Path("data/silver"),
    gold_dir: Path = Path("data/gold"),
    db_path: Path = Path("warehouse/city_cost.duckdb"),
) -> dict[str, Any]:
    print(f"\n=== Старт на пайплайна за дата: {run_date} ===")
    summary: dict[str, Any] = {"ingestion_date": run_date}

    if not skip_fetch:
        if not skip_fuels:
            summary["fetch_fuels"] = _step_fetch_fuels(run_date, bronze_dir)
        if not skip_rents:
            summary["fetch_rents"] = _step_fetch_rents(run_date, bronze_dir)

    if not skip_fuels:
        summary["parse_fuels"] = _step_parse_fuels(run_date, bronze_dir, silver_dir, db_path)
        summary["check_fuels"] = _step_check_fuels(db_path)

    if not skip_rents:
        summary["parse_rents"] = _step_parse_rents(run_date, db_path)
        summary["check_rents"] = _step_check_rents(run_date, db_path)

    summary["build_gold"] = _step_build_gold(run_date, gold_dir, db_path)
    print(f"=== Край за дата: {run_date} ===")
    return summary


def run_pipeline(
    ingestion_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    skip_fetch: bool = False,
    skip_fuels: bool = False,
    skip_rents: bool = False,
    bronze_dir: Path = Path("data/bronze"),
    silver_dir: Path = Path("data/silver"),
    gold_dir: Path = Path("data/gold"),
    db_path: Path = Path("warehouse/city_cost.duckdb"),
) -> list[dict[str, Any]]:
    if start_date or end_date:
        s = start_date or ingestion_date or date.today().isoformat()
        e = end_date or date.today().isoformat()
        target_dates = _iter_dates(s, e)
    else:
        target_dates = [ingestion_date or date.today().isoformat()]

    results: list[dict[str, Any]] = []
    for d in target_dates:
        results.append(
            run_for_date(
                run_date=d,
                skip_fetch=skip_fetch,
                skip_fuels=skip_fuels,
                skip_rents=skip_rents,
                bronze_dir=bronze_dir,
                silver_dir=silver_dir,
                gold_dir=gold_dir,
                db_path=db_path,
            )
        )
    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="City Cost & Lifestyle Medallion Runner")
    parser.add_argument("--date", dest="ingestion_date", default=None, help="Единична дата YYYY-MM-DD")
    parser.add_argument("--start-date", default=None, help="Начална дата YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="Крайна дата YYYY-MM-DD")
    parser.add_argument("--skip-fetch", action="store_true", help="Пропуска свалянето от интернет")
    parser.add_argument("--skip-fuels", action="store_true", help="Пропуска потока за горива")
    parser.add_argument("--skip-rents", action="store_true", help="Пропуска потока за наеми")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_pipeline(
        ingestion_date=args.ingestion_date,
        start_date=args.start_date,
        end_date=args.end_date,
        skip_fetch=args.skip_fetch,
        skip_fuels=args.skip_fuels,
        skip_rents=args.skip_rents,
    )