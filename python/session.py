"""Snowpark session factory.

Builds a Snowflake session using key-pair auth from config/config.yaml. The
Snowpark Python API (DataFrame) is used for the load path; the heavy transforms
are deliberately kept in SQL stored procedures so the project teaches both
idiomatic Snowflake approaches.
"""
from __future__ import annotations

from snowflake.snowpark import Session

from python.config import snowflake_connection_params


def get_session() -> Session:
    """Return a Snowpark session configured for the AQ_WAREHOUSE database."""
    return Session.builder.configs(snowflake_connection_params()).create()


def snowflake_enabled() -> bool:
    from python.config import snowflake_enabled as _enabled
    return _enabled()
