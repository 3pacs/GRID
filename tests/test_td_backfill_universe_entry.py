"""``scripts/td_backfill_universe.py`` is the unit's entry point and now only
delegates to ``scripts/close_backfill_universe.py universe``.

The systemd unit names this file, so the file stays; the Twelve Data code in it
is gone (owner decision 2026-11-23: "drop the twelve stuff, get it from our
other sources instead"). The gem cohort is not cut over and these tests pin
that. Local only: no network, no database.
"""

from __future__ import annotations

import ast
import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENTRY = ROOT / "scripts" / "td_backfill_universe.py"
GEM = ROOT / "scripts" / "td_backfill_gem_tickers.py"


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
    return names


def _load_entry(monkeypatch):
    """Load the file the way the unit runs it: by path, repo root not on sys.path."""
    monkeypatch.setattr(sys, "path", [p for p in sys.path if Path(p or ".").resolve() != ROOT])
    monkeypatch.delitem(sys.modules, "scripts", raising=False)
    monkeypatch.delitem(sys.modules, "scripts.close_backfill_universe", raising=False)
    spec = importlib.util.spec_from_file_location("td_backfill_universe_entry", ENTRY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


def test_entry_delegates_to_the_stored_close_backfill_for_the_universe(monkeypatch):
    mod = _load_entry(monkeypatch)
    calls: list[list[str]] = []

    def fake_main(argv=None):
        calls.append(list(argv))
        return 7

    monkeypatch.setattr(mod.close_backfill_universe, "main", fake_main)
    assert mod.main() == 7
    assert calls == [["universe"]]


def test_entry_has_no_twelve_data_or_database_client_code():
    imported = _imported_modules(ENTRY)
    assert not imported & {"requests", "psycopg2", "urllib", "httpx", "time"}
    source = ENTRY.read_text(encoding="utf-8")
    assert "os.environ" not in source and "api.twelvedata" not in source
    assert "source_catalog" not in source.replace("``source_catalog``", "")


def test_gem_cohort_is_not_cut_over():
    assert "requests" in _imported_modules(GEM)
    assert "twelvedata" in GEM.read_text(encoding="utf-8").lower()
