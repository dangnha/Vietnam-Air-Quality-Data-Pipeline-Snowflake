"""End-to-end Snowflake-native pipeline runner.

Sequence: load Bronze -> CALL SILVER.LOAD_FROM_BRONZE() -> CALL CONTROL.RUN_DQ_GATE()
-> CALL GOLD.LOAD_ALL(). If the DQ gate raises, Gold is skipped.

This mirrors the SQL task DAG in sql/06_streams_tasks.sql, but lets you drive the
same stored procedures from a local Snowpark client — no external scheduler needed.

Usage:
    python -m python.orchestrate.native_pipeline
"""
from __future__ import annotations

from snowflake.snowpark import Session

from python.session import get_session


def _call(session: Session, label: str, proc: str) -> str:
    print(f"\n=== [{label}] ===")
    rows = session.sql(f"CALL {proc}()").collect()
    value = str(rows[0][0]) if rows and rows[0] else ""
    print(f"[orchestrate] {proc} -> {value}")
    return value


def run() -> int:
    session = get_session()
    try:
        # Load raw files into Bronze.
        from python.load.bronze import run as load_bronze

        load_bronze()

        # Silver.
        _call(session, "Transform Silver", "AQ_WAREHOUSE.SILVER.LOAD_FROM_BRONZE")

        # Data-quality gate. Raises (throws) on failure, which aborts this run.
        _call(session, "Quality gate", "AQ_WAREHOUSE.CONTROL.RUN_DQ_GATE")

        # Gold.
        _call(session, "Transform Gold", "AQ_WAREHOUSE.GOLD.LOAD_ALL")

        print("\n[orchestrate] pipeline completed successfully")
        return 0
    except Exception as exc:
        print(f"\n[orchestrate] STOPPED: {exc}")
        return 1
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(run())
