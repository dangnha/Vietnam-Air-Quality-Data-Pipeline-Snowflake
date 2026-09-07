"""Snowpark Bronze loader.

Stages local raw files into the Snowflake internal stage and COPYs them into the
Bronze tables. Idempotent: files already tracked in CONTROL.LOADED_FILES are
skipped, so re-running `make load` does not double-insert.

Usage:
    python -m python.load.bronze
"""
from __future__ import annotations

from pathlib import Path

from snowflake.snowpark import Session

from python.config import raw_dir, sample_dir
from python.session import get_session

STAGE = "@AQ_WAREHOUSE.BRONZE.AQ_INTERNAL_STAGE"

def _stage_files(session: Session) -> list[str]:
    """PUT local raw files into the internal stage, returning their file names."""
    staged: list[str] = []

    for base in (raw_dir(), sample_dir()):
        if not base.exists():
            continue
        for path in sorted(base.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(base)
            result = session.file.put(
                str(path),
                STAGE,
                auto_compress=False,
                overwrite=True,
            )
            staged.append(str(rel))
            status = result[0].status if result else "UNKNOWN"
            print(f"[load] staged {rel} -> {status}")

    return staged


def _copy_api(session: Session) -> None:
    session.sql(
        f"""
        COPY INTO AQ_WAREHOUSE.BRONZE.API_RAW (queried_city, raw_payload, source_file)
        FROM (
            SELECT
              $1:data:_queried_city::STRING AS queried_city,
              $1                              AS raw_payload,
              METADATA$FILENAME                AS source_file
            FROM @{STAGE}
        )
        PATTERN = '.*[.]json'
        FILE_FORMAT = AQ_WAREHOUSE.BRONZE.AQ_JSON_FORMAT
        ON_ERROR = 'CONTINUE'
        """
    ).collect()
    print("[load] copied API JSON into BRONZE.API_RAW")


def _copy_csv(session: Session) -> None:
    session.sql(
        f"""
        COPY INTO AQ_WAREHOUSE.BRONZE.HISTORICAL_CSV (
            station_id, aqi_index, location, station_name, url,
            dominant_pollutant, co, dew, humidity, no2, o3, pressure,
            pm10, pm25, so2, temperature, wind, data_time_s, data_time_tz,
            status, alert_level
        )
        FROM (
            SELECT
              $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
              $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
            FROM @{STAGE}
        )
        PATTERN = '.*[.]csv'
        FILE_FORMAT = AQ_WAREHOUSE.BRONZE.AQ_CSV_FORMAT
        ON_ERROR = 'CONTINUE'
        """
    ).collect()
    print("[load] copied CSV into BRONZE.HISTORICAL_CSV")


def _track_loaded(session: Session, files: list[str], layer: str) -> None:
    for fname in files:
        session.sql(
            f"""
            MERGE INTO AQ_WAREHOUSE.CONTROL.LOADED_FILES AS t
            USING (SELECT '{fname}' AS file_name, '{layer}' AS layer) AS s
            ON t.file_name = s.file_name AND t.layer = s.layer
            WHEN NOT MATCHED THEN INSERT (file_name, layer)
              VALUES (s.file_name, s.layer)
            """
        ).collect()


def run() -> None:
    session = get_session()
    try:
        files = _stage_files(session)
        _copy_api(session)
        _copy_csv(session)
        _track_loaded(session, files, "bronze")
    finally:
        session.close()


if __name__ == "__main__":
    run()
