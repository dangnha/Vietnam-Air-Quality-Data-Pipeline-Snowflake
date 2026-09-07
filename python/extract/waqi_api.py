"""Extract real-time air-quality data from the WAQI API.

Mirrors the AWS `lambda_waqi_ingestion` function, but writes JSON files to a
local raw directory instead of S3. The Snowpark loader (python/load/bronze.py)
then stages and COPYs them into Snowflake Bronze.

Usage:
    python -m python.extract.waqi_api
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from python.config import raw_dir, waqi

API_TEMPLATE = "https://api.waqi.info/feed/{city}/?token={token}"


def _fetch_city(city: str, token: str) -> dict | None:
    url = API_TEMPLATE.format(city=city, token=token)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    if payload.get("status") != "ok":
        print(f"[waqi] {city}: status={payload.get('status')} data={payload.get('data')}")
        return None

    # Embed the queried city so Bronze preserves the request context alongside
    # the API payload — exactly what the AWS partition key `queried_city` did.
    data = payload["data"]
    data["_queried_city"] = city
    return data


def _write_json(city: str, data: dict, out_dir: Path) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    fname = f"{city}_{ts}.json"
    dest = out_dir / city
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / fname
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def run() -> None:
    token = waqi().get("token", "").strip()
    cities = waqi().get("cities", [])
    out_dir = raw_dir() / "api"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not token:
        print("[waqi] No WAQI token configured. "
              "Set config/config.yaml -> waqi.token, or run the offline sample path.")
        return

    for city in cities:
        try:
            data = _fetch_city(city, token)
            if data is not None:
                path = _write_json(city, data, out_dir)
                print(f"[waqi] wrote {path}")
        except Exception as exc:  # keep going for the remaining cities
            print(f"[waqi] {city} failed: {exc}")
        time.sleep(1)  # be polite to the free API


if __name__ == "__main__":
    run()
