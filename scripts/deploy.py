"""Deploy all Snowflake objects by executing sql/*.sql in order.

Uses the Snowflake Python connector with key-pair auth from config/config.yaml.
Each SQL file is idempotent (CREATE ... IF NOT EXISTS / CREATE OR REPLACE), so
re-running is safe.

Usage:
    python scripts/deploy.py
"""
from __future__ import annotations

from pathlib import Path

import snowflake.connector

from python.config import PROJECT_ROOT, snowflake_connection_params

SQL_DIR = PROJECT_ROOT / "sql"


def _read_sql_files() -> list[Path]:
    return sorted(SQL_DIR.glob("*.sql"))


def main() -> int:
    files = _read_sql_files()
    if not files:
        print("[deploy] no SQL files found in sql/")
        return 1

    conn = snowflake.connector.connect(**snowflake_connection_params())
    try:
        for path in files:
            print(f"[deploy] executing {path.name}")
            # execute_stream handles semicolons AND dollar-quoted procedure bodies.
            with path.open("r", encoding="utf-8") as fh:
                for cursor in conn.execute_stream(fh):
                    for _row in cursor:
                        pass
    finally:
        conn.close()

    print("[deploy] all SQL objects applied")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
