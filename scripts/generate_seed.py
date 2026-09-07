"""Generate deterministic seed data for the Snowflake air-quality pipeline.

Produces:
  * data/sample/api/        -> WAQI-style JSON files (5 cities x 30 days x 3/day)
  * data/sample/historical_air_quality_2021_en.csv -> 2021 historical rows

Everything is reproducible (fixed RNG seed) so the full pipeline can run offline
and produce a meaningful Silver/Gold result.

Usage:
    python scripts/generate_seed.py
"""
from __future__ import annotations

import csv
import json
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Make the repo root importable when run as `python scripts/generate_seed.py`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from python.config import PROJECT_ROOT, sample_dir

# station metadata: idx -> (name, lat, lon)
CITIES = {
    "ha-noi": {
        "display": "Ha Noi",
        "stations": [
            (10221, "US Embassy Ha Noi", 21.02, 105.83),
            (10222, "Hanoi French Embassy", 21.03, 105.84),
            (10223, "Hanoi West Lake", 21.05, 105.82),
        ],
    },
    "ho-chi-minh-city": {
        "display": "Ho Chi Minh City",
        "stations": [
            (10231, "US Consulate HCMC", 10.78, 106.70),
            (10232, "HCMC Nguyen Van Cu", 10.75, 106.66),
            (10233, "HCMC District 7", 10.73, 106.72),
        ],
    },
    "da-nang": {
        "display": "Da Nang",
        "stations": [
            (10241, "Da Nang City Center", 16.07, 108.22),
            (10242, "Da Nang Airport", 16.05, 108.20),
        ],
    },
    "gia-lai": {
        "display": "Gia Lai",
        "stations": [
            (10251, "Pleiku Station", 13.97, 108.01),
            (10252, "Gia Lai Rural", 13.90, 108.10),
        ],
    },
    "cao-bang": {
        "display": "Cao Bang",
        "stations": [
            (10261, "Cao Bang Station", 22.67, 106.26),
            (10262, "Cao Bang Mountain", 22.70, 106.20),
        ],
    },
}

POLLUTANTS = ["pm25", "pm10", "no2", "o3", "so2", "co"]
TIMES_OF_DAY = [(8, 0), (14, 0), (20, 0)]


def _aqi_for(rng: random.Random, base: float, day: int, hour: int) -> float:
    # Deterministic diurnal + daily wobble around a city-specific baseline.
    diurnal = 10 * ((hour - 12) / 12.0) ** 2
    weekly = 8 * ((day % 7) - 3)
    noise = rng.uniform(-6, 6)
    return max(15.0, min(320.0, base + diurnal + weekly + noise))


def _derive_pollutants(aqi: float, rng: random.Random) -> dict:
    pm25 = round(aqi * rng.uniform(0.9, 1.1), 1)
    pm10 = round(pm25 * rng.uniform(1.4, 1.8), 1)
    return {
        "pm25": pm25,
        "pm10": pm10,
        "co": round(rng.uniform(300, 900), 1),
        "no2": round(rng.uniform(8, 45), 1),
        "o3": round(rng.uniform(20, 70), 1),
        "so2": round(rng.uniform(3, 12), 1),
        "h": round(rng.uniform(60, 90), 1),
        "t": round(rng.uniform(18, 32), 1),
        "p": round(rng.uniform(1000, 1020), 1),
        "w": round(rng.uniform(0.5, 4.0), 1),
    }


def _pollution_level(aqi: float) -> str:
    if aqi <= 50:
        return "good"
    if aqi <= 100:
        return "moderate"
    if aqi <= 150:
        return "unhealthy_sensitive"
    if aqi <= 200:
        return "unhealthy"
    if aqi <= 300:
        return "very_unhealthy"
    return "hazardous"


def _write_api_files(rng: random.Random, out_dir: Path) -> None:
    today = datetime(2026, 1, 1, tzinfo=timezone.utc)  # fixed window
    for city, meta in CITIES.items():
        base_aqi = 40 + list(CITIES).index(city) * 25
        station_idx, station_name, lat, lon = meta["stations"][0]

        for day_offset in range(30):
            for hour, minute in TIMES_OF_DAY:
                ts = today + timedelta(days=day_offset, hours=hour, minutes=minute)
                aqi = _aqi_for(rng, base_aqi, day_offset, hour)
                pollutants = _derive_pollutants(aqi, rng)

                payload = {
                    "status": "ok",
                    "data": {
                        "aqi": round(aqi, 1),
                        "idx": station_idx,
                        "dominentpol": "pm25",
                        "time": {"iso": ts.strftime("%Y-%m-%dT%H:%M:%SZ")},
                        "city": {
                            "name": station_name,
                            "url": f"https://aqicn.org/station/{station_idx}",
                            "geo": [lat, lon],
                        },
                        "iaqi": {
                            "pm25": {"v": pollutants["pm25"]},
                            "pm10": {"v": pollutants["pm10"]},
                            "co": {"v": pollutants["co"]},
                            "no2": {"v": pollutants["no2"]},
                            "o3": {"v": pollutants["o3"]},
                            "so2": {"v": pollutants["so2"]},
                            "h": {"v": pollutants["h"]},
                            "t": {"v": pollutants["t"]},
                            "p": {"v": pollutants["p"]},
                            "w": {"v": pollutants["w"]},
                        },
                        "_queried_city": city,
                    },
                }

                city_dir = out_dir / "api" / city
                city_dir.mkdir(parents=True, exist_ok=True)
                fname = f"{city}_{ts.strftime('%Y-%m-%dT%H-%M-%SZ')}.json"
                (city_dir / fname).write_text(
                    json.dumps(payload, indent=2), encoding="utf-8"
                )


def _write_csv(rng: random.Random, out_dir: Path) -> None:
    header = [
        "station_id", "aqi_index", "location", "station_name", "url",
        "dominant_pollutant", "co", "dew", "humidity", "no2", "o3", "pressure",
        "pm10", "pm25", "so2", "temperature", "wind", "data_time_s",
        "data_time_tz", "status", "alert_level",
    ]
    start = datetime(2021, 1, 1, 8, 0, 0)

    rows = []
    for city, meta in CITIES.items():
        base_aqi = 35 + list(CITIES).index(city) * 22
        for station_idx, station_name, lat, lon in meta["stations"]:
            for day_offset in range(30):
                for hour, minute in TIMES_OF_DAY:
                    ts = start + timedelta(days=day_offset, hours=hour - 8, minutes=minute)
                    aqi = _aqi_for(rng, base_aqi, day_offset, hour)
                    p = _derive_pollutants(aqi, rng)
                    level = _pollution_level(aqi)
                    rows.append({
                        "station_id": str(station_idx),
                        "aqi_index": str(round(aqi, 1)),
                        "location": meta["display"],
                        "station_name": station_name,
                        "url": f"https://aqicn.org/station/{station_idx}",
                        "dominant_pollutant": "pm25",
                        "co": str(p["co"]),
                        "dew": str(round(p["t"] - 5, 1)),
                        "humidity": str(p["h"]),
                        "no2": str(p["no2"]),
                        "o3": str(p["o3"]),
                        "pressure": str(p["p"]),
                        "pm10": str(p["pm10"]),
                        "pm25": str(p["pm25"]),
                        "so2": str(p["so2"]),
                        "temperature": str(p["t"]),
                        "wind": str(p["w"]),
                        "data_time_s": ts.strftime("%Y-%m-%d %H:%M:%S"),
                        "data_time_tz": "Asia/Ho_Chi_Minh",
                        "status": "ok",
                        "alert_level": level,
                    })

    csv_path = out_dir / "historical_air_quality_2021_en.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def run() -> None:
    rng = random.Random(42)
    out = sample_dir()
    out.mkdir(parents=True, exist_ok=True)

    # Remove stale api files but keep the historical CSV replaced atomically.
    for child in (out / "api").glob("*"):
        for f in child.glob("*.json"):
            f.unlink()

    _write_api_files(rng, out)
    _write_csv(rng, out)
    print(f"[seed] wrote deterministic data to {out}")


if __name__ == "__main__":
    run()
