"""ingestion.altdata.finra_dix: S&P 500 membership by date and the dollar-weighted DIX ratio."""

from __future__ import annotations

import importlib
import re
from datetime import date
from pathlib import Path

import pytest

from ingestion.altdata import finra_dix as X

REPO = Path(__file__).resolve().parents[1]
DOC = {"base_date": "2020-01-02", "covers_to": "2020-06-01", "base_members": ["AAA", "BRK.B", "CCC"],
       "changes": [{"date": "2020-03-01", "add": ["DDD"], "remove": ["CCC"]},
                   {"date": "2020-06-01", "add": ["EEE"], "remove": []}]}


def test_membership_applies_changes_in_date_order():
    m = X.Membership(DOC)
    assert m.on(date(2020, 1, 2)) == {"AAA", "BRK.B", "CCC"}
    assert m.on(date(2020, 3, 1)) == {"AAA", "BRK.B", "DDD"}
    assert m.on(date(2020, 7, 1)) == {"AAA", "BRK.B", "DDD", "EEE"}
    assert m.on(date(2019, 12, 31)) == frozenset()
    assert m.union(date(2020, 2, 1), date(2020, 4, 1)) == {"AAA", "BRK.B", "CCC", "DDD"}


def test_shipped_membership_is_complete_and_current_enough():
    m = X.Membership.load()
    assert 495 <= len(m.on(date(2019, 6, 3))) <= 510
    assert 495 <= len(m.on(date(2026, 8, 18))) <= 510
    assert "BRK.B" in m.on(date(2024, 1, 2))
    assert m.covers_to >= "2026-08-01"


def _rows(sym_sv_tv):
    return [{"date": date(2024, 1, 2), "symbol": s, "short_volume": sv, "total_volume": tv} for s, sv, tv in sym_sv_tv]


def test_dix_is_dollar_weighted_with_class_share_symbols(monkeypatch):
    monkeypatch.setattr(X, "MIN_MATCHED", 2)
    rows = _rows([("AAA", 40, 100), ("BRK/B", 60, 100), ("AAA", 10, 0), ("ZZZ", 99, 100)])
    res = X.compute_dix(rows, ["AAA", "BRK.B", "CCC"], {"AAA": 10.0, "BRK.B": 400.0})
    assert res["matched"] == 2 and res["members"] == 3
    assert res["value"] == pytest.approx((50 * 10 + 60 * 400) / (100 * 10 + 100 * 400))   # AAA rows summed


def test_dix_needs_enough_priced_members(monkeypatch):
    monkeypatch.setattr(X, "MIN_MATCHED", 3)
    assert X.compute_dix(_rows([("AAA", 1, 2), ("BRK/B", 1, 2)]), ["AAA", "BRK.B"], {"AAA": 1.0, "BRK.B": 1.0}) is None


def test_pull_range_writes_only_missing_days_and_reports_gaps(monkeypatch):
    p = object.__new__(X.FINRADixPuller)
    p.membership = X.Membership(DOC)
    written = []
    monkeypatch.setattr(X, "MIN_MATCHED", 2)
    monkeypatch.setattr(p, "_key", lambda: "k", raising=False)
    monkeypatch.setattr(p, "_existing", lambda a, b: {date(2020, 3, 2)}, raising=False)
    monkeypatch.setattr(p, "_write", lambda d, res: written.append((d, round(res["value"], 4))), raising=False)
    files = {date(2020, 3, 3): _rows([("AAA", 50, 100), ("BRK/B", 50, 100)]), date(2020, 3, 4): None}
    monkeypatch.setattr(X, "fetch_file_rows", lambda d: files.get(d))
    monkeypatch.setattr(X, "fetch_closes", lambda tickers, a, b, key: {date(2020, 3, 3): {"AAA": 1.0, "BRK.B": 1.0}})
    out = p.pull_range(date(2020, 3, 2), date(2020, 3, 4))
    assert written == [(date(2020, 3, 3), 0.5)]
    assert out["status"] == "PARTIAL" and out["missing_file"] == ["2020-03-04"]


def test_missing_tiingo_key_skips(monkeypatch):
    p = object.__new__(X.FINRADixPuller)
    p.membership = X.Membership(DOC)
    monkeypatch.setattr(p, "_key", lambda: None, raising=False)
    assert p.pull_range(date(2020, 3, 2), date(2020, 3, 4))["status"] == "SKIPPED"


def test_feature_migration_and_entity_map():
    rev = importlib.import_module("migrations.versions.finra_dix_feature_20261113")
    assert re.findall(r"\(\s*'(finra_dix_sp500)'\s*,\s*'alternative'", rev.DIX_FEATURE) == ["finra_dix_sp500"]
    assert " ".join(rev.DIX_FEATURE.split()).endswith("ON CONFLICT (name) DO NOTHING")
    with pytest.raises(RuntimeError, match="refused"):
        rev.downgrade()
    from normalization.entity_map import SEED_MAPPINGS
    assert SEED_MAPPINGS.get("finra:dix_sp500") == "finra_dix_sp500"


def test_scheduler_runs_dix_with_its_own_timeout():
    src = (REPO / "ingestion" / "smart_scheduler.py").read_text()
    line = next(x for x in src.splitlines() if '"name": "finra_dix"' in x)
    assert '"cls": "FINRADixPuller"' in line and '"timeout_s": 600' in line and '"TIINGO_API_KEY"' in line
