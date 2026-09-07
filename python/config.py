"""Central configuration for the Snowflake air-quality pipeline.

Loads config/config.yaml and exposes typed accessors used by the extract, load,
orchestrate and deploy modules. If the YAML file is missing, the example config
is used so pure-Python unit tests run offline without any account credentials.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
EXAMPLE_PATH = PROJECT_ROOT / "config" / "config.example.yaml"


def _load() -> dict[str, Any]:
    path = CONFIG_PATH if CONFIG_PATH.exists() else EXAMPLE_PATH
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


CONFIG = _load()


def snowflake() -> dict[str, Any]:
    return CONFIG.get("snowflake", {})


def waqi() -> dict[str, Any]:
    return CONFIG.get("waqi", {})


def paths() -> dict[str, Any]:
    return CONFIG.get("paths", {})


def quality() -> dict[str, Any]:
    return CONFIG.get("quality", {})


def raw_dir() -> Path:
    p = paths().get("raw_dir", "data/raw")
    return (PROJECT_ROOT / p).resolve()


def sample_dir() -> Path:
    p = paths().get("sample_dir", "data/sample")
    return (PROJECT_ROOT / p).resolve()


def kaggle_csv() -> Path:
    p = paths().get("kaggle_csv", "data/sample/historical_air_quality_2021_en.csv")
    return (PROJECT_ROOT / p).resolve()


def private_key_path() -> Path:
    p = snowflake().get("private_key_path", "~/.snowflake/rsa_key.p8")
    return Path(p).expanduser().resolve()


def snowflake_enabled() -> bool:
    """True when key-pair auth is fully configured (used to gate integration).

    Rejects placeholder account values (e.g. "<account_locator>") and requires
    the private key file to actually exist on disk.
    """
    cfg = snowflake()
    account = (cfg.get("account") or "").strip()
    user = (cfg.get("user") or "").strip()
    key_configured = bool(cfg.get("private_key_path"))
    return bool(
        account
        and not account.startswith("<")
        and user
        and not user.startswith("<")
        and key_configured
        and private_key_path().exists()
    )


def snowflake_connection_params() -> dict[str, Any]:
    """Parameters for snowflake.connector.connect(**params).

    Uses RSA key-pair authentication via the cryptography library.
    """
    from cryptography.hazmat.primitives import serialization

    cfg = snowflake()
    key_path = private_key_path()
    key_data = key_path.read_bytes()

    passphrase = cfg.get("private_key_passphrase") or None

    p_key = serialization.load_pem_private_key(
        key_data,
        password=passphrase.encode() if passphrase else None,
    )

    private_key_der = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return {
        "account": cfg.get("account"),
        "user": cfg.get("user"),
        "private_key": private_key_der,
        "role": cfg.get("role", "SYSADMIN"),
        "warehouse": cfg.get("warehouse", "AQ_WH"),
        "database": cfg.get("database", "AQ_WAREHOUSE"),
        "schema": cfg.get("schema", "CONTROL"),
    }