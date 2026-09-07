"""Tests for the deterministic seed-data generator (no Snowflake required)."""
from __future__ import annotations

import json

from python.config import sample_dir

CITIES = {"ha-noi", "ho-chi-minh-city", "da-nang", "gia-lai", "cao-bang"}


def test_seed_api_files_cover_five_cities():
    api_dir = sample_dir() / "api"
    if not api_dir.exists():
        return
    present = {d.name for d in api_dir.iterdir() if d.is_dir()}
    assert CITIES <= present


def test_seed_api_payload_shape():
    api_dir = sample_dir() / "api" / "ha-noi"
    files = list(api_dir.glob("*.json")) if api_dir.exists() else []
    if not files:
        return
    payload = json.loads(files[0].read_text())
    assert payload["status"] == "ok"
    assert payload["data"]["_queried_city"] == "ha-noi"
    assert "aqi" in payload["data"]
    assert "iaqi" in payload["data"]


def test_seed_csv_has_expected_columns():
    csv_path = sample_dir() / "historical_air_quality_2021_en.csv"
    if not csv_path.exists():
        return
    header = csv_path.read_text().splitlines()[0]
    for col in ["station_id", "aqi_index", "location", "pm25", "data_time_s"]:
        assert col in header
