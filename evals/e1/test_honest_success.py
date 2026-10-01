"""E1 gate 2: honest success, over every puller SmartScheduler actually runs.

#742 (``tests/test_smart_scheduler_honest_success.py``,
``tests/test_ingestion_outcome_regressions.py``) proves ``_classify_outcome``
on hand-picked return values and a few synthetic registry entries. This gate
does not repeat that table; it runs the **real** ``PULLER_REGISTRY`` entries
-- each with its own constructor shape (``api_key_mode``), registry kwargs
(including call-time callables), timeout and catalog ownership -- through
``SmartScheduler._run_puller`` with the provider class swapped for a stub
that returns each generic shape. For every entry and every shape the run is
SUCCESS, and ``source_catalog.last_pull_at`` advances, only when committed
rows > 0.

Registry completeness: every entry imports, names a callable pull method that
accepts its registry kwargs, can be constructed the way the scheduler
constructs it, and writes under a real ``source_catalog`` name (the class's
own ``SOURCE_NAME``, or a literal its module writes) -- the check that would
have caught the BLS and wiki_history "no pull_all" registrations.
"""

from __future__ import annotations

import importlib
import inspect
import types
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine

import ingestion.smart_scheduler as ss
from evals.e1.known_violations import known_violation
from ingestion.smart_scheduler import OUTCOME_SUCCESS, PULLER_REGISTRY, SmartScheduler, catalog_name_for

REPO = Path(__file__).resolve().parents[2]


class _WithSummary:
    """An aggregate object exposing ``.summary`` (the SEC/options result shape)."""

    def __init__(self, summary: dict) -> None:
        self.summary = summary


# (id, returned value or exception, committed rows > 0 and complete?)
SHAPES: list[tuple[str, Any, bool]] = [
    ("none", None, False),
    ("true", True, False),
    ("string", "done", False),
    ("int_positive", 5, True),
    ("int_zero", 0, False),
    ("int_negative", -3, False),
    ("float_count", 4.0, False),
    ("list_empty", [], False),
    ("list_rows", [{"rows_inserted": 3}, {"rows_inserted": 2}], True),
    ("list_zero_rows", [{"rows_inserted": 0}, {"rows_inserted": 0}], False),
    ("list_one_failed", [{"rows_inserted": 3}, {"status": "FAILED", "error": "x"}], False),
    ("list_all_skipped", [{"status": "SKIPPED"}, {"status": "SKIPPED"}], False),
    ("list_unknown_counts", [{"status": "SUCCESS"}], False),
    ("dict_rows", {"rows_inserted": 7}, True),
    ("dict_zero_rows", {"rows_inserted": 0}, False),
    ("dict_status_only", {"status": "SUCCESS"}, False),
    ("dict_none_count", {"status": "SUCCESS", "rows_inserted": None}, False),
    ("dict_string_count", {"rows_inserted": "7"}, False),
    ("dict_skipped_with_rows", {"status": "SKIPPED", "rows_inserted": 4}, False),
    ("dict_partial_with_rows", {"status": "PARTIAL", "rows_inserted": 5}, False),
    ("dict_errors_with_rows", {"inserted": 2, "errors": ["timeout"]}, False),
    ("dict_unchanged", {"status": "UNCHANGED"}, False),
    ("dict_incomplete_coverage", {"rows_inserted": 5, "succeeded": 1, "total": 2}, False),
    ("nested_counts", {"results": [{"rows_inserted": 2}, {"rows_inserted": 1}]}, True),
    ("nested_zero", {"results": [{"rows_inserted": 0}]}, False),
    ("nested_one_failed", {"results": [{"rows_inserted": 2}, {"status": "FAILED"}]}, False),
    ("summary_object_rows", _WithSummary({"status": "SUCCESS", "rows_inserted": 6}), True),
    ("summary_object_zero", _WithSummary({"status": "SUCCESS", "rows_inserted": 0}), False),
    ("raises", RuntimeError("upstream exploded"), False),
]


def _stub_module(entry: dict, out: Any) -> types.SimpleNamespace:
    def method(self, *args, **kwargs):
        if isinstance(out, BaseException):
            raise out
        return out

    cls = type("E1StubPuller", (), {"__init__": lambda self, *a, **k: None, entry["method"]: method})
    return types.SimpleNamespace(**{entry["cls"]: cls})


@pytest.fixture()
def scheduler(monkeypatch):
    monkeypatch.setattr(SmartScheduler, "_warn_registry_divergence", lambda self: None)
    monkeypatch.setattr(SmartScheduler, "_load_state_from_db", lambda self: None)
    sched = SmartScheduler(create_engine("sqlite://"))
    bumps: list[str] = []
    monkeypatch.setattr(sched, "_update_last_pull", lambda name: bumps.append(name))
    sched.bumps = bumps  # type: ignore[attr-defined]
    return sched


@pytest.mark.parametrize("entry", PULLER_REGISTRY, ids=lambda e: e["name"])
def test_registry_entry_reports_success_only_when_rows_were_committed(entry, scheduler, monkeypatch):
    if entry.get("api_key"):
        monkeypatch.setenv(entry["api_key"], "e1-test-key-not-real")
    mismatches = []
    for shape_id, out, succeeds in SHAPES:
        scheduler.bumps.clear()
        stub = _stub_module(entry, out)
        monkeypatch.setattr(importlib, "import_module", lambda _mod, _stub=stub: _stub)
        result = scheduler._run_puller(dict(entry))
        expect = succeeds and not entry.get("hold_reason")
        got_success = result.get("status") == OUTCOME_SUCCESS
        fresh = bool(scheduler.bumps)
        expect_fresh = expect and not ss._puller_owns_catalog(entry["name"])
        if got_success != expect or fresh != expect_fresh:
            mismatches.append((shape_id, result.get("status"), fresh))
    assert not mismatches, f"{entry['name']}: (shape, status, freshness bumped) {mismatches}"


# ── registry completeness ──────────────────────────────────────────────

# Registered entries that are not real pullable sources, as registry name ->
# known_violations ID. Empty since e1-v1.2: bls and wiki_history gained real
# pull_all entry points, pumpfun (dead upstream) left the registry, and
# coingecko writes its own source_catalog row via raw_series.
_KNOWN_INCOMPLETE: dict[str, str] = {}


def _params():
    for entry in PULLER_REGISTRY:
        marks = [known_violation(_KNOWN_INCOMPLETE[entry["name"]])] if entry["name"] in _KNOWN_INCOMPLETE else []
        yield pytest.param(entry, id=entry["name"], marks=marks)


def _construct_args(entry: dict) -> tuple[tuple, dict]:
    mode = entry.get("api_key_mode", "first") if entry.get("api_key") else None
    if mode is None or mode == "env":
        return (), {"db_engine": object()}
    if mode == "first":
        return ("key", object()), {}
    if mode == "keyword":
        return (), {"db_engine": object(), "api_key": "key"}
    raise AssertionError(f"unknown api_key_mode {mode!r}")


@pytest.mark.parametrize("entry", list(_params()))
def test_registry_entry_is_a_real_pullable_source(entry):
    name = entry["name"]
    module = importlib.import_module(entry["mod"])
    cls = getattr(module, entry["cls"], None)
    assert inspect.isclass(cls), f"{name}: {entry['mod']}.{entry['cls']} is not a class"

    args, kwargs = _construct_args(entry)
    inspect.signature(cls).bind(*args, **kwargs)  # the scheduler's constructor call must bind

    method = getattr(cls, entry["method"], None)
    assert callable(method), f"{name}: {entry['cls']} has no callable {entry['method']!r}"
    params = inspect.signature(method).parameters
    var_kw = any(p.kind is p.VAR_KEYWORD for p in params.values())
    unknown = [k for k in (entry.get("kwargs") or {}) if k not in params and not var_kw]
    assert not unknown, f"{name}: {entry['method']} does not accept registry kwargs {unknown}"

    catalog = catalog_name_for(name)
    assert catalog, f"{name}: no source_catalog name"
    declared = getattr(cls, "SOURCE_NAME", None)
    if isinstance(declared, str) and declared:
        assert declared.lower() == catalog.lower(), f"{name}: class writes {declared!r}, registry maps {catalog!r}"
    else:
        source = (REPO / (entry["mod"].replace(".", "/") + ".py")).read_text(encoding="utf-8")
        assert f"'{catalog}'" in source or f'"{catalog}"' in source, (
            f"{name}: {entry['cls']} has no SOURCE_NAME and its module never names {catalog!r}"
        )
