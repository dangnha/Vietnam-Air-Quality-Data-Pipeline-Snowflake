"""Unit tests for the offline configuration and seed-data generation."""
from __future__ import annotations

from python.config import kaggle_csv, sample_dir, waqi


def test_waqi_cities():
    cities = waqi()["cities"]
    assert len(cities) == 5
    assert "ha-noi" in cities
    assert "cao-bang" in cities


def test_sample_data_present():
    # Seed data is git-ignored and generated via `make seed`, so this check
    # only verifies the configured paths resolve — not that files ship with the repo.
    assert sample_dir().name == "sample"
    assert kaggle_csv().name == "historical_air_quality_2021_en.csv"


def test_quality_thresholds_sane():
    from python.config import quality

    q = quality()
    assert q["min_aqi"] == 0
    assert q["max_aqi"] == 500
    assert q["max_null_percent"] > 0
    assert q["min_row_count"] > 0


def test_snowflake_disabled_without_creds():
    from python.config import snowflake_enabled

    # config.example.yaml uses placeholder "<account_locator>", so this must be False.
    assert snowflake_enabled() is False


def test_city_conformance_map_in_sql():
    """The SQL city map covers all five WAQI slugs."""
    sql = (sample_dir().parent.parent / "sql" / "01_databases_schemas.sql").read_text()
    for city in waqi()["cities"]:
        assert city in sql
