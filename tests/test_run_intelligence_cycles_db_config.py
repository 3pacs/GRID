"""Regression: scripts/run_intelligence_cycles.py must source its DB
credential from config.py, and must fail closed (not fall back to an
embedded password) when that configuration is missing.

Prior to this fix, the module hardcoded a plaintext
``postgresql://user:password@host:port/db`` connection string as a
module-level ``ENGINE_URL`` constant -- exposed in git history and
bypassing config.py's own DB_PASSWORD validation entirely. The fix
replaces it with ``from config import settings`` /
``create_engine(settings.DB_URL)``, the same pattern every other
``scripts/*.py`` script already uses (see
tests/test_score_oracle_trades_db_config.py for the sibling fix this
mirrors).

Follows the same sys.modules-pop / re-import pattern as
tests/test_security.py's DB_PASSWORD validator tests, since config.py
constructs its ``settings`` singleton at *import* time -- the failure mode
under test only reproduces on a fresh import, not by mutating an
already-constructed settings object.
"""

from __future__ import annotations

import importlib
import os
import re
import sys
from pathlib import Path
from unittest.mock import patch

import pytest


def _fresh_import_run_intelligence_cycles():
    """Pop config + run_intelligence_cycles from sys.modules and re-import both.

    config.py's module-level ``settings = Settings()`` only re-validates on
    a fresh import; monkeypatching os.environ after config is already
    cached would not exercise the validator at all.
    """
    for name in ("scripts.run_intelligence_cycles", "config"):
        sys.modules.pop(name, None)
    return importlib.import_module("scripts.run_intelligence_cycles")


@pytest.fixture(autouse=True)
def _restore_modules():
    saved = {
        name: sys.modules.get(name)
        for name in ("scripts.run_intelligence_cycles", "config")
    }
    yield
    for name, mod in saved.items():
        sys.modules.pop(name, None)
        if mod is not None:
            sys.modules[name] = mod


def test_configured_credentials_produce_the_configured_db_url():
    """With real configuration present, settings.DB_URL reflects it exactly.

    Proves the module is actually wired to config.py -- not just that it
    imports without error.
    """
    env = {
        "ENVIRONMENT": "development",
        "DB_HOST": "test-db-host",
        "DB_PORT": "5433",
        "DB_NAME": "test_griddb",
        "DB_USER": "test_grid_user",
        "DB_PASSWORD": "test-configured-password",
        "FRED_API_KEY": "fake",
    }
    with patch.dict(os.environ, env, clear=False):
        mod = _fresh_import_run_intelligence_cycles()

        assert mod.settings.DB_URL == (
            "postgresql://test_grid_user:test-configured-password"
            "@test-db-host:5433/test_griddb"
        )
        assert mod.engine.url.render_as_string(hide_password=False) == (
            "postgresql://test_grid_user:test-configured-password"
            "@test-db-host:5433/test_griddb"
        )


def test_missing_password_fails_closed_no_embedded_fallback():
    """Empty DB_PASSWORD must raise at import time -- not silently succeed
    with an embedded/default password.

    This is the behavior the plaintext ENGINE_URL literal bypassed entirely:
    before the fix, an unconfigured environment would still produce a
    working (if wrong-target) connection string, masking the missing
    configuration instead of failing on it.
    """
    from pydantic import ValidationError

    env = {
        "ENVIRONMENT": "development",
        "DB_PASSWORD": "",
        "FRED_API_KEY": "fake",
    }
    with patch.dict(os.environ, env, clear=False):
        with pytest.raises(ValidationError):
            _fresh_import_run_intelligence_cycles()


def test_no_hardcoded_connection_string_in_source():
    """Static guard: no literal postgresql://user:pass@host connection string
    anywhere in the file. Catches a regression reintroducing an embedded
    credential even if the config.py wiring above still passes."""
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "run_intelligence_cycles.py"
    )
    source = path.read_text(encoding="utf-8")

    literal_conn_string = re.compile(r"postgresql(\+\w+)?://[^\s\"']+:[^\s\"']+@")
    matches = literal_conn_string.findall(source)

    assert not matches, (
        f"Found {len(matches)} hardcoded-looking connection string(s) in "
        "run_intelligence_cycles.py -- DB credentials must come from "
        "config.settings.DB_URL, not a literal in source."
    )
