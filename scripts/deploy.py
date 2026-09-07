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

# 07 is an interactive demo (Time Travel / Clone / clustering). It must be run
# manually AFTER data exists, not during deployment — otherwise its Time Travel
# query fails because the tables were just created.
SKIP_FILES = {"07_snowflake_features_demo.sql"}


def _read_sql_files() -> list[Path]:
    return [p for p in sorted(SQL_DIR.glob("*.sql")) if p.name not in SKIP_FILES]


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
            # remove_comments=True avoids "Empty SQL statement" errors when a file
            # ends with comment lines (e.g. sql/02_file_formats_stages.sql).
            with path.open("r", encoding="utf-8") as fh:
                for cursor in conn.execute_stream(fh, remove_comments=True):
                    for _row in cursor:
                        pass
    finally:
        conn.close()

    print("[deploy] all SQL objects applied")
    if SKIP_FILES:
        print("[deploy] skipped interactive demo(s): " + ", ".join(sorted(SKIP_FILES)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
