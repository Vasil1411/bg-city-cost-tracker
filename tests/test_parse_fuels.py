from datetime import date

from src.transform.parse_fuels import normalize_fuel_type, parse_fuel_html


def test_normalize_fuel_type() -> None:
    assert normalize_fuel_type("A95") == "gasoline_95"
    assert normalize_fuel_type("Бензин А95-H") == "gasoline_95"
    assert normalize_fuel_type("Дизел") == "diesel"
    assert normalize_fuel_type("Пропан-бутан") == "lpg"


def test_parse_fuel_html_extracts_clean_rows_and_deduplicates() -> None:
    sample_html = """
    <div class="row">
      <div class="col-md-8">
        Бензиностанция Антим Русе, ул. Антим I-ви №3 предлага A95 за 1,34 €/л.
        Цената е актуална към 13:51 на 19.11.2026г.
      </div>
    </div>
    <div class="row">
      <div class="col-md-8">
        Бензиностанция Антим Русе, ул. Антим I-ви №3 предлага A95 за 1,34 €/л.
        Цената е актуална към 13:51 на 19.11.2026г.
      </div>
    </div>
    """
    rows = parse_fuel_html(sample_html, date(2026, 11, 19))
    assert len(rows) == 1
    assert rows[0]["city"] == "Русе"
    assert rows[0]["station_name"] == "Бензиностанция Антим"
    assert rows[0]["fuel_type"] == "gasoline_95"
    assert rows[0]["price_eur"] == 1.34