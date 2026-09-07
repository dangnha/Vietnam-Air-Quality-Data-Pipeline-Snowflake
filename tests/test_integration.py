"""Optional integration tests against a real Snowflake account.

These only run when RUN_SNOWFLAKE_INTEGRATION=1 is set in the environment AND
valid key-pair credentials are configured in config/config.yaml. They are
skipped otherwise so `make test` stays fully offline.
"""
from __future__ import annotations

import os

import pytest

from python.config import snowflake_enabled

RUN_INTEGRATION = os.getenv("RUN_SNOWFLAKE_INTEGRATION") == "1"


pytestmark = pytest.mark.skipif(
    not (RUN_INTEGRATION and snowflake_enabled()),
    reason="Set RUN_SNOWFLAKE_INTEGRATION=1 and configure key-pair auth to run integration tests",
)


def test_warehouse_exists():
    from python.session import get_session

    session = get_session()
    try:
        rows = session.sql("SHOW WAREHOUSES LIKE 'AQ_WH'").collect()
        assert len(rows) == 1
    finally:
        session.close()


def test_resource_monitor_exists():
    from python.session import get_session

    session = get_session()
    try:
        rows = session.sql("SHOW RESOURCE MONITORS LIKE 'AQ_BUDGET_MONITOR'").collect()
        assert len(rows) == 1
    finally:
        session.close()
