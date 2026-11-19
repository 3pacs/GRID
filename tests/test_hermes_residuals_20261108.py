"""Hermes residual failures, 2026-11-08 grid-svr journal.

* Wikipedia_Attention: a rate-limited article search was cached as "no
  article" for the process lifetime (84 of 100 tickers skipped).
* Hermes repairs never cleared a source from the failed list (pull_log
  records scheduler runs only), and held sources stayed on it.
* Unusual_Whales: 30 tickers at ~4 s each against a 60 s timeout.
* UX auditor read a Settings field that does not exist.
No network, no database.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from ingestion.altdata import social_attention as sa
from ingestion.smart_scheduler import (
    OUTCOME_NO_NEW_DATA,
    OUTCOME_PARTIAL,
    OUTCOME_SUCCESS,
    PULLER_REGISTRY,
    _classify_outcome,
)
from scripts import hermes_fixers as hf


@pytest.fixture
def puller(monkeypatch):
    monkeypatch.setattr(sa.WikipediaAttentionPuller, "_article_cache", {})
    monkeypatch.setattr(sa.WikipediaAttentionPuller, "_wikidata_cache", {})
    p = sa.WikipediaAttentionPuller.__new__(sa.WikipediaAttentionPuller)
    p.engine = None
    monkeypatch.setattr(p, "_load_sec_name_map", lambda: {"ABBV": "AbbVie Inc."})
    monkeypatch.setattr(sa.time, "sleep", lambda s: None)
    return p


def _resp(status, payload=None):
    def raise_for_status():
        if status >= 400:
            raise sa.requests.HTTPError(f"{status}")
    return SimpleNamespace(status_code=status, json=lambda: payload or {},
                           raise_for_status=raise_for_status)


def test_rate_limited_search_is_not_cached_as_no_article(puller, monkeypatch):
    monkeypatch.setattr(sa.requests, "get", lambda *a, **k: _resp(429))
    out = puller.pull_ticker("ABBV")
    assert out["status"] == "FAILED" and out["reason"] == "wiki_lookup_unavailable"
    assert "ABBV" not in puller._article_cache

    hit = {"query": {"search": [{"title": "AbbVie"}]}}
    monkeypatch.setattr(sa.requests, "get", lambda *a, **k: _resp(200, hit))
    monkeypatch.setattr(sa.WikipediaAttentionPuller, "_is_company_via_wikidata",
                        classmethod(lambda cls, t: True))
    assert puller._resolve_wiki_article("ABBV") == "AbbVie"


def test_network_error_is_unavailable_not_absent(puller, monkeypatch):
    def boom(*a, **k):
        raise sa.requests.ConnectionError("reset")
    monkeypatch.setattr(sa.requests, "get", boom)
    with pytest.raises(sa._LookupUnavailable):
        puller._resolve_wiki_article("ABBV")
    assert "ABBV" not in puller._article_cache


def test_wikidata_failure_fails_open_without_caching(monkeypatch):
    monkeypatch.setattr(sa.WikipediaAttentionPuller, "_wikidata_cache", {})
    monkeypatch.setattr(sa.requests, "get", lambda *a, **k: _resp(429))
    assert sa.WikipediaAttentionPuller._is_company_via_wikidata("Patrick Dovigi") is True
    assert "Patrick Dovigi" not in sa.WikipediaAttentionPuller._wikidata_cache


def test_ticker_without_a_name_is_a_clean_empty_result(puller):
    out = puller.pull_ticker("ZZZZ")
    assert out["status"] == "NO_NEW_DATA" and out["reason"] == "no_wiki_article"


def test_run_with_only_absent_articles_is_not_partial(puller, monkeypatch):
    stored = {"status": "SUCCESS", "ticker": "AAPL", "rows": 30}
    monkeypatch.setattr(puller, "pull_ticker", lambda t, days=90: (
        stored if t == "AAPL" else {"status": "NO_NEW_DATA", "ticker": t, "rows": 0,
                                    "reason": "no_wiki_article"}))
    out = puller.pull_all(tickers=["AAPL", "ZZZZ", "YYYY"])
    assert out["tickers_skipped"] == 2
    assert _classify_outcome(out)[:2] == (OUTCOME_SUCCESS, 30)


def test_unavailable_lookups_keep_the_run_partial(puller, monkeypatch):
    monkeypatch.setattr(puller, "pull_ticker", lambda t, days=90: (
        {"status": "SUCCESS", "ticker": t, "rows": 30} if t == "AAPL" else
        {"status": "FAILED", "ticker": t, "rows": 0, "error": "Wikipedia search HTTP 429",
         "reason": "wiki_lookup_unavailable"}))
    out = puller.pull_all(tickers=["AAPL", "ABBV"])
    assert _classify_outcome(out)[0] == OUTCOME_PARTIAL


def test_all_absent_is_no_new_data(puller, monkeypatch):
    monkeypatch.setattr(puller, "pull_ticker", lambda t, days=90: {
        "status": "NO_NEW_DATA", "ticker": t, "rows": 0, "reason": "no_wiki_article"})
    assert _classify_outcome(puller.pull_all(tickers=["ZZZZ"]))[0] == OUTCOME_NO_NEW_DATA


def test_wikimedia_requests_carry_contact_details():
    assert "@" in sa._WIKIMEDIA_UA


@pytest.fixture
def recovered(monkeypatch):
    table: dict = {}
    monkeypatch.setattr(hf, "_REPAIR_RECOVERED_AT", table)
    monkeypatch.setattr(hf, "_REPEATED_FAILURES", {})
    return table


def test_repair_recovery_after_the_failure_clears_it(recovered):
    failed_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    assert not hf._recovered_after("OppInsights", failed_at)
    hf.note_repair_recovered("OppInsights")
    assert hf._recovered_after("OppInsights", failed_at)
    assert hf._recovered_after("oppinsights", failed_at.replace(tzinfo=None))
    assert hf._recovered_after("OppInsights", failed_at.isoformat())
    # A failure after the recovery is a new failure.
    assert not hf._recovered_after("OppInsights", datetime.now(timezone.utc) + timedelta(seconds=1))
    assert not hf._recovered_after("OppInsights", None)


def test_held_sources_cover_every_alias():
    held = hf._held_source_names()
    for alias in ("tiingo_fundamentals", "TIINGO_FUNDAMENTALS", "smart:tiingo_fundamentals"):
        assert hf._normalize_source_key(alias) in held
    assert "unusual_whales" not in held


class _Conn:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, query, params=None):
        if params is not None:  # the pull_log relation probe
            return SimpleNamespace(scalar=lambda: True)
        return SimpleNamespace(fetchall=lambda: self.rows)


def test_diagnosis_skips_held_and_repaired_sources(recovered, monkeypatch):
    now = datetime.now(timezone.utc)
    rows = [
        ("TIINGO_FUNDAMENTALS", 2, now - timedelta(hours=6), "status 400"),
        ("OppInsights", 2, now - timedelta(hours=6), "4 of 5 items completed"),
    ]
    hf.note_repair_recovered("OppInsights")
    monkeypatch.setattr(hf, "_relation_exists", lambda conn, name: True)
    engine = SimpleNamespace(connect=lambda: _Conn(rows))
    warnings: list[str] = []
    monkeypatch.setattr(hf.log, "warning", lambda msg, **kw: warnings.append(msg.format(**kw)))
    result = hf.diagnose_and_fix_pulls(engine, hermes_available=False, state=object(), dry_run=True)
    assert result["retried"] == 0
    assert not any("Failed sources" in w for w in warnings)


def test_unusual_whales_timeout_covers_a_full_scan():
    entry = next(e for e in PULLER_REGISTRY if e["name"] == "unusual_whales")
    # 30 tickers measured at ~4.2 s each on grid-svr.
    assert entry["timeout_s"] >= 30 * 4.2 * 1.5


def test_ux_auditor_without_password_skips_login(monkeypatch):
    from scripts import ux_auditor

    monkeypatch.delenv("GRID_MASTER_PASSWORD", raising=False)

    def no_post(*a, **k):
        raise AssertionError("must not log in without a password")

    monkeypatch.setattr(ux_auditor.requests, "post", no_post)
    assert ux_auditor._get_auth_token("http://127.0.0.1:1") is None
