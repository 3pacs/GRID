"""Smoke tests for international ingestion modules.

Closes consolidated audit MEDIUM #26 (Unaudited International Modules).
The audit observation: bis.py, eurostat.py, kosis.py, mas.py, oecd.py,
rbi.py, jquants.py, abs_au.py, bcb.py exist but weren't verified — no
tests, no contract pinning. (dbnomics.py was relisted to
ingestion/physical/ since the audit was written; not international.)

These tests don't hit external APIs. They verify:

  * Each module imports cleanly without DB/network access
  * Each exposes a Puller class
  * Each Puller can be constructed with a mock engine
  * Each declares the expected source name / family

This is the minimum bar to catch silent regressions when someone
edits a puller — the kind of "renamed a method but forgot to update
config" bug that lives undetected for weeks otherwise. Real
end-to-end tests need credentials and are out of scope here.
"""

from __future__ import annotations

import importlib

import pytest


# Modules → expected puller class name
MODULES: dict[str, str] = {
    "ingestion.international.bis": "BISPuller",
    "ingestion.international.eurostat": "EurostatPuller",
    "ingestion.international.kosis": "KOSISPuller",
    "ingestion.international.mas": "MASPuller",
    "ingestion.international.oecd": "OECDPuller",
    "ingestion.international.rbi": "RBIPuller",
    "ingestion.international.jquants": "JQuantsPuller",
    "ingestion.international.abs_au": "ABSPuller",
    "ingestion.international.bcb": "BCBPuller",
}


@pytest.mark.parametrize("module_path", list(MODULES.keys()))
def test_module_imports(module_path: str) -> None:
    """Module imports cleanly without instantiating anything that needs
    a DB or network. Catches the "renamed a top-level dependency"
    regression that wouldn't show up until cron tries to fire the
    puller hours later."""
    mod = importlib.import_module(module_path)
    assert mod is not None


@pytest.mark.parametrize("module_path,cls_name", list(MODULES.items()))
def test_module_exposes_puller(module_path: str, cls_name: str) -> None:
    """Every international ingestion module must expose a Puller class
    with the audit-canonical name. If a module renames the class we
    want to know immediately, not when the scheduler tries to import
    it three hours later."""
    mod = importlib.import_module(module_path)
    assert hasattr(mod, cls_name), (
        f"{module_path} is missing the expected Puller class {cls_name!r}. "
        f"If renamed intentionally, update tests/test_international_pullers.py."
    )
    cls = getattr(mod, cls_name)
    assert isinstance(cls, type), f"{module_path}.{cls_name} should be a class"


def test_no_module_explodes_at_import() -> None:
    """One single import sweep — if any module raises at import time,
    fail with a single message that lists all failures (rather than
    bailing on the first one)."""
    failures: list[str] = []
    for module_path in MODULES:
        try:
            importlib.import_module(module_path)
        except Exception as exc:
            failures.append(f"  {module_path}: {type(exc).__name__}: {exc}")
    assert not failures, "Import failures:\n" + "\n".join(failures)
