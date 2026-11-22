"""Regression tests closing Codex review HOLD on PR #1373 (head 6b6ecbf5).

Covers:
1. Snapshot binding: regime labels cite only features from the exact producer
   analytical snapshot (bound by snapshot id/hash and availability clock <=
   decision clock); unrelated snapshots and missing labels are withheld.
2. Distinct eligible feature IDs: duplicates, nulls, blanks, non-typed, and
   ineligible entries do not count toward MIN_REGIME_BOUND_INPUTS.
3. Positive instrument binding: named instruments (e.g. 10-year Treasury yield
   -> DGS10 from raw yc_10y) must bind to their own source series and never
   fall back to unrelated unit-compatible numbers (DGS2, DGS5, T10Y2Y, features).
4. Clock sanity: future timestamps and negative ages are rejected rather than
   trusted as fresh (including when a claimed age/freshness says fresh).

All tests are offline: synthetic dicts, fake connections, mocked LLM — no DB,
no network, no live or protected outputs read.
"""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

os.environ.setdefault("DB_PASSWORD", "test-password")

from ollama.market_briefing import MarketBriefingEngine
from tests.test_market_briefing_number_grounding import (
    _FakeConnection,
    _FakeEngine,
    _FakeOllamaClient,
    _FakeResult,
    _empty_snapshot,
    _make_engine,
    _no_network,
)

ROOT = Path(__file__).resolve().parents[1]


def _load_packet_module():
    spec = importlib.util.spec_from_file_location(
        "daily_publication_packet_binding_under_test",
        ROOT / "scripts" / "daily_publication_packet.py",
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _canonical_sha256(payload: dict) -> str:
    raw = json.dumps(
        payload, sort_keys=True, ensure_ascii=False, allow_nan=False,
        separators=(",", ":"), default=str,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _packet_bundle(records: list[dict], producer_snapshots: dict | None = None) -> dict:
    day = "2026-11-18"
    bundle = {
        "version": "publication-capture-v1",
        "producer": "scripts/daily_publication_packet.py",
        "publication_day": day,
        "session_start": f"{day}T00:00:00Z",
        "session_end": "2026-11-19T00:00:00Z",
        "captured_at": "2026-11-19T06:00:00Z",
        "database_receipt": {"transaction_read_only": "on"},
        "channels": {
            "market_briefings": {
                "count": len(records),
                "truncated": False,
                "complete": True,
                "state": "ROWS_RETURNED",
            },
        },
        "failures": [],
        "existing_diary_context": [],
        "records": records,
    }
    if producer_snapshots is not None:
        bundle["producer_snapshots"] = producer_snapshots
    return bundle


def _captured_producer(job, snapshot_id: int, payload: dict, created_at: str = "2026-11-18T09:59:00Z") -> dict:
    """The packet's capture of one analytical_snapshots row, keyed by id."""
    row = {"id": snapshot_id, "category": "regime_detection", "payload": payload, "created_at": created_at}
    return {str(snapshot_id): job.describe_producer_snapshot(row)}


def _briefing_record(row_id: int, snapshot_data: dict, *, created_at: str = "2026-11-18T14:00:00Z") -> dict:
    return {
        "channel": "market_briefings",
        "record": {
            "id": row_id,
            "briefing_type": "hourly",
            "briefing_date": "2026-11-18",
            "content": f"Synthetic briefing {row_id}.",
            "created_at": created_at,
            "snapshot_bytes": 600,
            "snapshot_data": snapshot_data,
        },
    }


class _BoundRegimeConn(_FakeConnection):
    """Fake DB connection serving decision_journal and analytical_snapshots rows."""

    def __init__(self, regime_row, snap_rows=None, *, row_by_sid=None):
        super().__init__(row_by_sid=row_by_sid)
        self._regime_row = regime_row
        self._snap_rows = list(snap_rows) if snap_rows is not None else []
        self.snap_queries: list[tuple[str, dict]] = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        params = dict(params or {})
        if "FROM decision_journal" in sql:
            return _FakeResult(one=self._regime_row)
        if "FROM analytical_snapshots" in sql:
            self.snap_queries.append((sql, params))
            rows = list(self._snap_rows)
            if "snapshot_id" in params:
                rows = [r for r in rows if len(r) >= 2 and r[0] == params["snapshot_id"]]
            return _FakeResult(one=(rows[0] if rows else None), many=rows)
        return super().execute(stmt, params)


# ── HOLD 1: Snapshot binding & missing regime label ──────────────────────────


def test_briefing_unrelated_analytical_snapshot_cannot_lend_feature_count_and_withholds_regime(tmp_path):
    """Unbound decision_journal row (no snapshot_id/hash) must NOT borrow
    features_used from an unrelated latest analytical_snapshots row."""
    unrelated_payload = {
        "regime": "FRAGILE",
        "n_features": 12,
        "features_used": {f"c{i}": f"feat_{i}" for i in range(10)},
    }
    regime_row = ("FRAGILE", 0.72, 0.15, {}, "DEFENSIVE", "2026-11-18T10:00:00Z")
    conn = _BoundRegimeConn(
        regime_row,
        snap_rows=[(99, unrelated_payload, "2026-11-18T09:55:00Z", date(2026, 11, 18))],
    )

    snap = _make_engine(_FakeOllamaClient(None), _FakeEngine(conn), tmp_path)._gather_market_snapshot()

    assert "latest_regime" not in snap
    assert snap.get("latest_regime_withheld", {}).get("reason")
    assert conn.snap_queries == []


@pytest.mark.parametrize("case", [
    "wrong_snapshot_id",
    "wrong_snapshot_hash",
    "regime_label_mismatch",
    "missing_snapshot_label",
    "blank_snapshot_label",
    "snapshot_not_found",
])
def test_briefing_snapshot_binding_mismatch_or_missing_label_withholds_regime(tmp_path, case):
    """Any producer snapshot mismatch (id, hash, regime label, missing label, or absent row)
    must withhold latest_regime with a logged reason."""
    valid_payload = {
        "regime": "FRAGILE",
        "features_used": {f"c{i}": f"feat_{i}" for i in range(8)},
    }
    expected_hash = _canonical_sha256(valid_payload)
    flags = {"snapshot_id": 42, "snapshot_hash": expected_hash}
    regime_row = ("FRAGILE", 0.72, 0.15, flags, "DEFENSIVE", "2026-11-18T10:00:00Z")

    if case == "wrong_snapshot_id":
        snap_rows = [(99, valid_payload, "2026-11-18T09:59:00Z", date(2026, 11, 18))]
    elif case == "wrong_snapshot_hash":
        tampered = {**valid_payload, "features_used": {f"c{i}": f"other_{i}" for i in range(9)}}
        snap_rows = [(42, tampered, "2026-11-18T09:59:00Z", date(2026, 11, 18))]
    elif case == "regime_label_mismatch":
        other_regime = {"regime": "GROWTH", "features_used": valid_payload["features_used"]}
        flags = {"snapshot_id": 42, "snapshot_hash": _canonical_sha256(other_regime)}
        regime_row = ("FRAGILE", 0.72, 0.15, flags, "DEFENSIVE", "2026-11-18T10:00:00Z")
        snap_rows = [(42, other_regime, "2026-11-18T09:59:00Z", date(2026, 11, 18))]
    elif case == "missing_snapshot_label":
        no_label = {"features_used": valid_payload["features_used"]}
        flags = {"snapshot_id": 42, "snapshot_hash": _canonical_sha256(no_label)}
        regime_row = ("FRAGILE", 0.72, 0.15, flags, "DEFENSIVE", "2026-11-18T10:00:00Z")
        snap_rows = [(42, no_label, "2026-11-18T09:59:00Z", date(2026, 11, 18))]
    elif case == "blank_snapshot_label":
        blank_label = {"regime": "   ", "features_used": valid_payload["features_used"]}
        flags = {"snapshot_id": 42, "snapshot_hash": _canonical_sha256(blank_label)}
        regime_row = ("FRAGILE", 0.72, 0.15, flags, "DEFENSIVE", "2026-11-18T10:00:00Z")
        snap_rows = [(42, blank_label, "2026-11-18T09:59:00Z", date(2026, 11, 18))]
    else:
        snap_rows = []

    conn = _BoundRegimeConn(regime_row, snap_rows=snap_rows)
    snap = _make_engine(_FakeOllamaClient(None), _FakeEngine(conn), tmp_path)._gather_market_snapshot()

    assert "latest_regime" not in snap, f"{case}: latest_regime should be withheld"
    assert snap.get("latest_regime_withheld", {}).get("reason")


@pytest.mark.parametrize("bad_state", [None, "", "   ", "UNKNOWN", "NONE", "N/A"])
def test_briefing_missing_or_unasserted_journal_regime_label_is_withheld(tmp_path, bad_state):
    payload = {"regime": "FRAGILE", "features_used": {f"c{i}": f"feat_{i}" for i in range(8)}}
    flags = {"snapshot_id": 42, "snapshot_hash": _canonical_sha256(payload)}
    regime_row = (bad_state, 0.72, 0.15, flags, "DEFENSIVE", "2026-11-18T10:00:00Z")
    conn = _BoundRegimeConn(
        regime_row,
        snap_rows=[(42, payload, "2026-11-18T09:59:00Z", date(2026, 11, 18))],
    )

    snap = _make_engine(_FakeOllamaClient(None), _FakeEngine(conn), tmp_path)._gather_market_snapshot()

    assert "latest_regime" not in snap
    assert snap.get("latest_regime_withheld", {}).get("reason") == "missing_regime_label"


def test_briefing_bound_producer_snapshot_passes_and_carries_binding_metadata(tmp_path):
    payload = {
        "regime": "FRAGILE",
        "features_used": {f"c{i}": f"feat_{i}" for i in range(8)},
    }
    expected_hash = _canonical_sha256(payload)
    flags = {"snapshot_id": 42, "snapshot_hash": expected_hash}
    regime_row = ("FRAGILE", 0.72, 0.15, flags, "DEFENSIVE", "2026-11-18T10:00:00Z")
    conn = _BoundRegimeConn(
        regime_row,
        snap_rows=[(42, payload, "2026-11-18T09:59:00Z", date(2026, 11, 18))],
    )

    engine = _make_engine(_FakeOllamaClient(None), _FakeEngine(conn), tmp_path)
    snap = engine._gather_market_snapshot()

    lr = snap["latest_regime"]
    assert lr["state"] == "FRAGILE"
    assert lr["bound_inputs"] == 8
    assert lr["snapshot_id"] == 42
    assert lr["snapshot_hash"] == expected_hash
    assert len(lr["eligible_feature_ids"]) == 8

    # End-to-end: verified bound regime renders into data_context and fallback
    ctx = engine._build_data_context(snap)
    assert "### LATEST REGIME INFERENCE" in ctx
    assert "- State: FRAGILE" in ctx
    fallback = engine._generate_fallback_briefing(snap)
    assert "## Latest Regime" in fallback
    assert "FRAGILE" in fallback

    # And passes daily_publication_packet cleanly once the packet has read the
    # producer snapshot it cites
    job = _load_packet_module()
    packet, exclusions = job.build_packet(_packet_bundle(
        [_briefing_record(8002, snap)], producer_snapshots=_captured_producer(job, 42, payload),
    ))
    assert exclusions == []
    assert [p["id"] for p in packet["publications"]] == ["market_briefings:8002"]
    # Without that read, the same row is refused as unverified
    _, unread = job.build_packet(_packet_bundle([_briefing_record(8002, snap)]))
    assert [(e["disposition"], e["reasons"]) for e in unread] == [
        ("REFUSED_UNVERIFIED_REGIME_BINDING", ["producer_snapshot_not_captured"]),
    ]


@pytest.mark.parametrize("snapshot_data,case_id", [
    (
        {
            "latest_regime": {"state": "FRAGILE", "snapshot_id": 10, "timestamp": "2026-11-18T10:00:00Z"},
            "features_used": {f"c{i}": f"feat_{i}" for i in range(10)},
            "snapshot_id": 99,
        },
        "parent_unrelated_snapshot",
    ),
    (
        {
            "latest_regime": {
                "state": "FRAGILE",
                "snapshot_id": 10,
                "snapshot_hash": "a" * 64,
                "timestamp": "2026-11-18T10:00:00Z",
                "bound_inputs": 9,
                "producer_snapshot": {
                    "snapshot_id": 99,
                    "regime": "FRAGILE",
                    "available_at": "2026-11-18T09:55:00Z",
                    "features_used": {f"c{i}": f"feat_{i}" for i in range(9)},
                },
            },
        },
        "producer_snapshot_id_mismatch",
    ),
    (
        {
            "latest_regime": {
                "state": "FRAGILE",
                "snapshot_id": 10,
                "timestamp": "2026-11-18T10:00:00Z",
                "bound_inputs": 9,
                "producer_snapshot": {
                    "snapshot_id": 10,
                    "regime": "GROWTH",
                    "available_at": "2026-11-18T09:55:00Z",
                    "features_used": {f"c{i}": f"feat_{i}" for i in range(9)},
                },
            },
        },
        "producer_snapshot_regime_mismatch",
    ),
    (
        {
            "latest_regime": {
                "state": "FRAGILE",
                "snapshot_id": 10,
                "timestamp": "2026-11-18T10:00:00Z",
                "bound_inputs": 9,
                "producer_snapshot": {
                    "snapshot_id": 10,
                    "available_at": "2026-11-18T09:55:00Z",
                    "features_used": {f"c{i}": f"feat_{i}" for i in range(9)},
                },
            },
        },
        "producer_snapshot_missing_label",
    ),
    (
        {
            "latest_regime": {
                "state": "",
                "bound_inputs": 10,
                "timestamp": "2026-11-18T10:00:00Z",
            },
        },
        "missing_regime_label_blank",
    ),
    (
        {
            "latest_regime": {
                "bound_inputs": 10,
                "timestamp": "2026-11-18T10:00:00Z",
            },
        },
        "missing_regime_label_absent",
    ),
])
def test_packet_refuses_unrelated_snapshot_and_missing_regime_label(snapshot_data, case_id):
    job = _load_packet_module()
    bundle = _packet_bundle([_briefing_record(8001, snapshot_data)])

    packet, exclusions = job.build_packet(bundle)

    assert packet["publications"] == [], f"{case_id}: should be withheld/refused"
    assert len(exclusions) == 1
    assert "market_briefings:8001" in packet["capture_receipt"]["quarantined_publication_ids"]
    assert any("market_briefings:8001" in b for b in packet["blockers"])


# ── HOLD 2: Distinct non-null typed eligible feature IDs ─────────────────────


def test_briefing_duplicate_null_blank_and_ineligible_features_counted_distinctly(tmp_path):
    """Duplicates, nulls, blanks, booleans, and ineligible dicts must not pad bound_inputs."""
    payload = {
        "regime": "FRAGILE",
        "features_used": [
            "vix_close",
            "vix_close",
            " VIX_CLOSE ",
            None,
            "",
            "   ",
            "null",
            True,
            False,
            {"feature_id": "hy_oas", "eligible": True},
            {"feature_id": "hy_oas", "eligible": True},
            {"feature_id": "bad_feat", "eligible": False},
            {"feature_id": "stale_feat", "stale": True},
            {"feature_id": None, "eligible": True},
            "dgs10",
            42,
            42,
        ],
    }
    flags = {"snapshot_id": 55, "snapshot_hash": _canonical_sha256(payload)}
    regime_row = ("FRAGILE", 0.72, 0.15, flags, "DEFENSIVE", "2026-11-18T10:00:00Z")
    conn = _BoundRegimeConn(
        regime_row,
        snap_rows=[(55, payload, "2026-11-18T09:59:00Z", date(2026, 11, 18))],
    )

    snap = _make_engine(_FakeOllamaClient(None), _FakeEngine(conn), tmp_path)._gather_market_snapshot()

    # Only 4 distinct valid eligible IDs: "vix_close", "hy_oas", "dgs10", 42
    assert snap["latest_regime"]["bound_inputs"] == 4

    job = _load_packet_module()
    packet, exclusions = job.build_packet(_packet_bundle([_briefing_record(8010, snap)]))
    assert packet["publications"] == []
    assert exclusions[0]["disposition"] == "REFUSED_INSUFFICIENT_REGIME_INPUTS"


@pytest.mark.parametrize("regime_meta", [
    {
        "state": "FRAGILE",
        "timestamp": "2026-11-18T10:00:00Z",
        "features_used": ["f1", "f1", "f2", "f2", None, "", "   ", "null", "f3", "f3"],
    },
    {
        "state": "GROWTH",
        "timestamp": "2026-11-18T10:00:00Z",
        # 10 dict keys mapping to only 3 distinct non-null target feature IDs
        "features_used": {
            "c0": "vix", "c1": "vix", "c2": "vix", "c3": None, "c4": "",
            "c5": "   ", "c6": "hy_oas", "c7": "hy_oas", "c8": "dgs10", "c9": False,
        },
    },
    {
        "state": "CRISIS",
        "timestamp": "2026-11-18T10:00:00Z",
        # Claimed bound_inputs=10 cannot override features_used having only 4 eligible IDs
        "bound_inputs": 10,
        "features_used": [
            {"feature_id": 1, "eligible": True},
            {"feature_id": 1, "eligible": True},
            {"feature_id": 2, "eligible": False},
            {"feature_id": 3, "model_eligible": False},
            {"feature_id": 4, "eligible": True},
            {"feature_id": 5, "eligible": True},
            {"feature_id": 6, "eligible": True},
            {"feature_id": None, "eligible": True},
        ],
    },
])
def test_packet_duplicate_null_and_ineligible_feature_ids_do_not_reach_threshold(regime_meta):
    job = _load_packet_module()
    bundle = _packet_bundle([_briefing_record(8011, {"latest_regime": regime_meta})])

    packet, exclusions = job.build_packet(bundle)

    assert packet["publications"] == []
    assert len(exclusions) == 1
    assert exclusions[0]["disposition"] == "REFUSED_INSUFFICIENT_REGIME_INPUTS"


# ── HOLD 3: Positive instrument binding (Treasury yield wrong-source) ────────


@pytest.mark.parametrize("claim_text,expected_unverified", [
    ("The 10-year Treasury yield stands at 4.80% today.", "4.80%"),
    ("The 10-year Treasury yield stands at 4.50% today.", "4.50%"),
    ("The 10-year Treasury yield stands at -0.45% today.", "-0.45%"),
    ("With the curve inverted, the 10-year Treasury yield is 0.45% today.", "0.45%"),
    ("The 10-year Treasury yield stands at 4.33% today.", "4.33%"),
    ("The 10-year Treasury yield stands at 4.15% today.", "4.15%"),
    ("The ten-year Treasury yield stands at 4.50% today.", "4.50%"),
])
def test_grounding_named_10y_treasury_yield_rejects_wrong_source_values(
    tmp_path, monkeypatch, claim_text, expected_unverified,
):
    """When yc_10y / DGS10 is absent, a named 10-year Treasury yield claim must
    FAIL grounding even when DGS2, DGS5, T10Y2Y, DFF, macro, or features contain
    the exact same numeric value (no fallback to global context pool)."""
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.compute_sentiment", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.log_prediction", _no_network)
    monkeypatch.setattr("intelligence.context_provider.build_full_context", lambda *_a, **_k: "")

    snapshot = _empty_snapshot()
    snapshot["timestamp"] = "2026-11-18T14:00:00+00:00"
    snapshot["date"] = "2026-11-18"
    snapshot["rates"] = {
        "DGS2": {"value": 4.80, "date": "2026-11-18"},
        "DGS5": {"value": 4.50, "date": "2026-11-18"},
        "T10Y2Y": {"value": -0.45, "date": "2026-11-18"},
        "DFF": {"value": 4.33, "date": "2026-11-18"},
    }
    # Unit-compatible feature & macro entries that used to slip through pool fallback
    snapshot["features"] = {
        "custom_rate_proxy": {"value": 4.15, "date": "2026-11-18"},
        "alt_5y_proxy": {"value": 4.50, "date": "2026-11-18"},
    }
    snapshot["macro"] = {
        "UNRATE": {"value": 4.50, "date": "2026-11-01"},
    }

    engine = _make_engine(_FakeOllamaClient(f"## What's Happening Now\n{claim_text}\n"), None, tmp_path)
    monkeypatch.setattr(engine, "_gather_market_snapshot", lambda: copy.deepcopy(snapshot))

    result = engine.generate_briefing(briefing_type="hourly", save=False)

    assert result["grounding"]["ungrounded_count"] == 1, (
        f"Expected ungrounded on {claim_text!r}, got {result['grounding']}"
    )
    assert result["grounding"]["ungrounded_values"] == [expected_unverified]
    assert f"{expected_unverified} [unverified]" in result["content"]


def test_grounding_10y_treasury_binds_positively_to_yc_10y_dgs10_only(tmp_path, monkeypatch):
    """When yc_10y populates rates.DGS10=4.22, 4.22% passes for 10Y while
    4.50% (present in DGS5 and features) still fails for 10Y."""
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.compute_sentiment", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.log_prediction", _no_network)
    monkeypatch.setattr("intelligence.context_provider.build_full_context", lambda *_a, **_k: "")

    rows = {
        "yc_10y": (4.22, date(2026, 11, 18)),
        "DGS2": (4.80, date(2026, 11, 18)),
        "DGS5": (4.50, date(2026, 11, 18)),
        "T10Y2Y": (-0.58, date(2026, 11, 18)),
    }

    good_engine = _make_engine(
        _FakeOllamaClient("## What's Happening Now\nThe 10-year Treasury yield is 4.22%.\n"),
        _FakeEngine(_FakeConnection(row_by_sid=rows)),
        tmp_path,
    )
    good = good_engine.generate_briefing(briefing_type="hourly", save=False)
    assert good["grounding"]["ungrounded_count"] == 0

    bad_engine = _make_engine(
        _FakeOllamaClient("## What's Happening Now\nThe 10-year Treasury yield is 4.50%.\n"),
        _FakeEngine(_FakeConnection(row_by_sid=rows)),
        tmp_path,
    )
    bad = bad_engine.generate_briefing(briefing_type="hourly", save=False)
    assert bad["grounding"]["ungrounded_count"] == 1
    assert bad["grounding"]["ungrounded_values"] == ["4.50%"]


# ── HOLD 4: Clock sanity — reject future timestamps & negative ages ──────────


@pytest.mark.parametrize("snapshot_data,case_id", [
    (
        {"rates": {"DGS10": {"value": 4.35, "date": "2026-11-25"}}},
        "future_obs_date",
    ),
    (
        {"rates": {"DGS10": {"value": 4.35, "date": "2026-11-25", "age_days": 0}}},
        "future_obs_date_with_claimed_fresh_age",
    ),
    (
        {
            "rates": {
                "DGS10": {
                    "value": 4.35,
                    "date": "2026-11-25",
                    "freshness": {"status": "fresh", "age": 0, "limit": 5, "unit": "days"},
                },
            },
        },
        "future_obs_date_with_claimed_fresh_block",
    ),
    (
        {"models": {"regime_hmm": {"trained_at": "2026-11-18T18:00:00Z", "max_age_days": 30}}},
        "future_same_day_timestamp",
    ),
    (
        {"rates": {"DGS2": {"value": 4.80, "date": "2026-11-18", "age_days": -1}}},
        "negative_age_days",
    ),
    (
        {"rates": {"DGS2": {"value": 4.80, "date": "2026-11-18", "data_staleness_days": -3}}},
        "negative_data_staleness_days",
    ),
    (
        {"rates": {"DGS2": {"value": 4.80, "date": "2026-11-18", "age_hours": -0.5}}},
        "negative_age_hours",
    ),
    (
        {
            "rates": {
                "DGS2": {
                    "value": 4.80,
                    "date": "2026-11-18",
                    "freshness": {"status": "fresh", "age": -2, "limit": 5, "unit": "days"},
                },
            },
        },
        "negative_freshness_age",
    ),
    (
        {
            "latest_regime": {
                "state": "FRAGILE",
                "bound_inputs": 8,
                "timestamp": "2026-11-18T18:00:00Z",
            },
        },
        "future_regime_timestamp",
    ),
])
def test_packet_rejects_future_timestamps_and_negative_ages(snapshot_data, case_id):
    """Future timestamps and negative ages must be rejected, never trusted as fresh."""
    job = _load_packet_module()
    bundle = _packet_bundle([_briefing_record(8020, snapshot_data, created_at="2026-11-18T14:00:00Z")])

    packet, exclusions = job.build_packet(bundle)

    assert packet["publications"] == [], f"{case_id}: should be rejected"
    assert len(exclusions) == 1
    assert "market_briefings:8020" in packet["capture_receipt"]["quarantined_publication_ids"]
    assert any("market_briefings:8020" in b for b in packet["blockers"])


def test_packet_does_not_trust_claimed_fresh_age_over_stale_timestamp():
    """A stale observation date must still be flagged stale even if age_days=0 or
    freshness={'status': 'fresh', 'age': 0} claims otherwise."""
    job = _load_packet_module()
    bundle = _packet_bundle([_briefing_record(
        8021,
        {
            "rates": {
                "DGS2": {"value": 4.80, "date": "2026-10-01", "age_days": 0},
                "DGS10": {
                    "value": 4.35,
                    "date": "2026-10-01",
                    "freshness": {"status": "fresh", "age": 1, "limit": 5, "unit": "days"},
                },
            },
        },
        created_at="2026-11-18T14:00:00Z",
    )])

    packet, exclusions = job.build_packet(bundle)

    assert exclusions == []
    assert len(packet["publications"]) == 1
    pub = packet["publications"][0]
    assert pub["stale"] is True
    stale_paths = {item["path"] for item in pub["stale_items"]}
    assert "snapshot_data.rates.DGS2" in stale_paths
    assert "snapshot_data.rates.DGS10" in stale_paths


def test_briefing_rejects_future_db_rows_and_future_or_negative_age_snapshot_items(tmp_path, monkeypatch):
    """MarketBriefingEngine rejects future timestamps and negative ages both at
    DB gather time and during context rendering / grounding."""
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.compute_sentiment", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.log_prediction", _no_network)
    monkeypatch.setattr("intelligence.context_provider.build_full_context", lambda *_a, **_k: "")

    future_day = date.today() + timedelta(days=5)
    future_ts = (datetime.now(timezone.utc) + timedelta(hours=4)).isoformat()
    payload = {"regime": "FRAGILE", "features_used": {f"c{i}": f"feat_{i}" for i in range(8)}}
    flags = {"snapshot_id": 77, "snapshot_hash": _canonical_sha256(payload)}

    # (a) Future raw_series observation and future analytical_snapshots clock rejected at gather
    conn = _BoundRegimeConn(
        regime_row=("FRAGILE", 0.72, 0.15, flags, "DEFENSIVE", "2026-11-18T10:00:00Z"),
        snap_rows=[(77, payload, future_ts, future_day)],
        row_by_sid={"yc_10y": (4.35, future_day), "DGS2": (4.80, date(2026, 11, 18))},
    )
    gathered = _make_engine(_FakeOllamaClient(None), _FakeEngine(conn), tmp_path)._gather_market_snapshot()
    assert "DGS10" not in gathered["rates"]
    assert gathered["rates"]["DGS2"]["value"] == 4.80
    assert "latest_regime" not in gathered
    assert gathered.get("latest_regime_withheld", {}).get("reason")

    # (b) Future-dated or negative-age snapshot entries are withheld from data_context
    #     and fail grounding if claimed in LLM text.
    snapshot = _empty_snapshot()
    snapshot["timestamp"] = "2026-11-18T14:00:00+00:00"
    snapshot["date"] = "2026-11-18"
    snapshot["rates"] = {
        "DGS10": {"value": 4.35, "date": "2026-11-25", "age_days": 0},
        "DGS2": {"value": 4.80, "date": "2026-11-18", "age_days": -2},
    }
    engine = _make_engine(
        _FakeOllamaClient(
            "## What's Happening Now\n"
            "The 10-year Treasury yield is 4.35% and the 2-year Treasury yield is 4.80%.\n"
        ),
        None,
        tmp_path,
    )
    monkeypatch.setattr(engine, "_gather_market_snapshot", lambda: copy.deepcopy(snapshot))
    result = engine.generate_briefing(briefing_type="hourly", save=False)

    assert result["grounding"]["ungrounded_count"] == 2
    assert result["grounding"]["ungrounded_values"] == ["4.35%", "4.80%"]
    assert "4.35% [unverified]" in result["content"]
    assert "4.80% [unverified]" in result["content"]

    # (c) Direct grounding call also rejects a future asof date against snapshot header
    direct_ctx = (
        "## Market Data Snapshot — 2026-11-18T14:00:00+00:00 (assembly time, not an evidence clock)\n"
        "### RATES\n"
        "- rates:DGS10: raw value=4.35 | asof=2026-11-25 | source=unknown\n"
    )
    direct = MarketBriefingEngine._check_numbers_grounded_by_instrument(
        "The 10-year Treasury yield is 4.35%.", direct_ctx,
    )
    assert len(direct.ungrounded) == 1
    assert direct.ungrounded[0].raw == "4.35%"


# ── Producer half: scripts/auto_regime.py binds its journal row to its snapshot ──


def test_auto_regime_saves_snapshot_before_journal_and_shares_one_run_tag():
    """The journal entry can name the snapshot only if the snapshot is saved
    first; regime_history keeps its place after the journal and reuses the
    same run tag, so the three rows join on one key."""
    import inspect

    import scripts.auto_regime as auto

    src = inspect.getsource(auto.run)
    i_tag = src.index('run_tag = f"auto_regime.run:{uuid4().hex}"')
    i_check = src.index("_journal_will_accept(confidence, trans_prob)")
    i_snap = src.index("snap_store.save_snapshot(")
    i_bind = src.index("_stored_snapshot_binding(engine, snapshot_id)")
    i_journal = src.index("journal.log_decision(")
    i_history = src.index("persist_regime_history(")
    assert i_tag < i_check < i_snap < i_bind < i_journal < i_history
    assert "contradiction_flags={**contradictions, **binding}" in src
    assert "RegimeReplacement(SAME_DAY_REFRESH_REASON, run_tag)" in src
    assert src.count("uuid4()") == 1
    assert src.count("save_snapshot(") == 1


@pytest.mark.parametrize("confidence,trans_prob,ok", [
    (0.71, 0.15, True),
    (0.0, 1.0, True),
    (float("nan"), 0.1, False),
    (0.7, float("inf"), False),
    (1.2, 0.1, False),
    (0.7, -0.01, False),
    (True, 0.1, False),
])
def test_snapshot_is_not_saved_when_the_journal_would_reject_the_values(confidence, trans_prob, ok):
    import numpy as np

    import scripts.auto_regime as auto

    assert auto._journal_will_accept(confidence, trans_prob) is ok
    if ok:
        assert auto._journal_will_accept(np.float64(confidence), np.float32(trans_prob)) is True


def _pg_jsonb_roundtrip(obj):
    """What Postgres 15 JSONB hands back: -0.0 -> 0.0, and integral floats at
    or above 1e16 come back as integers (measured by the regime/state owner:
    '{"a": -0.0, "b": 1e20, "c": 1e-05}'::jsonb -> {"a": 0.0, "b":
    100000000000000000000, "c": 0.00001})."""
    from store.snapshots import _safe_json

    def norm(v):
        if isinstance(v, float):
            if v == 0.0:
                return 0.0
            if v.is_integer() and abs(v) >= 1e16:
                return int(v)
            return v
        if isinstance(v, dict):
            return {k: norm(x) for k, x in v.items()}
        if isinstance(v, list):
            return [norm(x) for x in v]
        return v

    return norm(json.loads(_safe_json(obj)))


def test_producer_binding_round_trips_through_the_briefing_guard(tmp_path):
    """auto_regime hashes the payload it reads back after saving, so its
    binding matches what the briefing guard hashes, even when JSONB
    normalized numbers (round(ds, 4) gives -0.0 for tiny negative slopes)."""
    from unittest.mock import MagicMock

    import numpy as np

    import scripts.auto_regime as auto
    from store.snapshots import canonical_payload_sha256

    payload = {
        "regime": "FRAGILE",
        "confidence": np.float64(0.7134),
        "stress_index": 1.2345,
        "stress_derivative": round(-0.00001, 4),  # -0.0
        "contributions": {"big": {"z": 1e20, "contribution": 0.1}},
        "n_features": np.int64(9),
        "features_used": {f"concept_{i}": f"feature_{i}" for i in range(9)},
        "data_as_of": date(2026, 11, 18),
        "regime_run_tag": "auto_regime.run:abc123",
    }
    stored = _pg_jsonb_roundtrip(payload)
    # Hashing the in-memory form would NOT match what the reader sees.
    assert canonical_payload_sha256(json.loads(json.dumps(payload, default=str))) != canonical_payload_sha256(stored)

    engine = MagicMock()
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    conn.execute.return_value.fetchone.return_value = (stored,)
    engine.connect.return_value = conn
    binding = auto._stored_snapshot_binding(engine, 42)
    params = conn.execute.call_args[0][1]
    assert params == {"sid": 42, "category": "regime_detection"}
    assert binding == {"snapshot_id": 42, "snapshot_hash": canonical_payload_sha256(stored)}

    flags = {
        "calm_in_stress": "vix z=-1.2 is calm despite FRAGILE regime",
        **binding,
        "regime_run_tag": "auto_regime.run:abc123",
    }
    regime_row = ("FRAGILE", 0.7134, 0.15, flags, "DEFENSIVE", "2026-11-18T10:00:00Z")
    bconn = _BoundRegimeConn(
        regime_row, snap_rows=[(42, stored, "2026-11-18T09:59:00Z", date(2026, 11, 18))],
    )
    snap = _make_engine(_FakeOllamaClient(None), _FakeEngine(bconn), tmp_path)._gather_market_snapshot()

    lr = snap["latest_regime"]
    assert lr["state"] == "FRAGILE"
    assert lr["snapshot_id"] == 42
    assert lr["bound_inputs"] == 9
    assert lr["contradictions"] == {"calm_in_stress": flags["calm_in_stress"]}


@pytest.mark.parametrize("readback", [None, ("not json",), ([1, 2],), RuntimeError("db down")])
def test_failed_snapshot_read_back_gives_no_binding(readback):
    from unittest.mock import MagicMock

    import scripts.auto_regime as auto

    engine = MagicMock()
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    if isinstance(readback, Exception):
        conn.execute.side_effect = readback
    else:
        conn.execute.return_value.fetchone.return_value = readback
    engine.connect.return_value = conn
    assert auto._stored_snapshot_binding(engine, 42) == {}
    assert auto._stored_snapshot_binding(engine, None) == {}


def test_regime_current_lists_contradictions_without_binding_keys():
    from unittest.mock import MagicMock, patch

    from api.routers import regime as regime_router

    engine, conn = MagicMock(), MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    prod = MagicMock()
    prod.fetchone.return_value = (7, "stress_index", "1.0")
    latest = MagicMock()
    latest.fetchone.return_value = (
        "FRAGILE", 0.72, 0.1,
        {"calm_in_stress": "x", "snapshot_id": 42, "snapshot_hash": "a" * 64,
         "regime_run_tag": "auto_regime.run:abc"},
        "DEFENSIVE", "NEUTRAL", datetime(2026, 11, 18, 10, tzinfo=timezone.utc),
    )
    history = MagicMock()
    history.fetchone.return_value = (None,)
    conn.execute.side_effect = [prod, latest, history]
    engine.connect.return_value = conn
    with patch.object(regime_router, "get_db_engine", return_value=engine):
        resp = regime_router.get_current(_token="t")
    assert resp.contradiction_flags == ["calm_in_stress: x"]


# ── Codex HOLD on #1386 head 032cd9ae ────────────────────────────────────────
# (1) a real SQLAlchemy Row is not a tuple; (2) the packet must take the count
# from the producer snapshot it reads, never from inline ids or counts.


def _real_rows(*rows: tuple) -> list:
    """Real ``sqlalchemy.engine.Row`` objects (from in-memory SQLite)."""
    from sqlalchemy import create_engine, text

    out = []
    eng = create_engine("sqlite://")
    with eng.connect() as conn:
        for sid, payload, created, as_of in rows:
            out.append(conn.execute(
                text("SELECT :id AS id, :payload AS payload, :created AS created_at, :as_of AS as_of_date"),
                {"id": sid, "payload": json.dumps(payload), "created": created, "as_of": as_of},
            ).fetchone())
    eng.dispose()
    return out


@pytest.mark.parametrize("flag_key", ["snapshot_id", None])
def test_briefing_accepts_real_sqlalchemy_rows_for_the_producer_snapshot(tmp_path, flag_key):
    from sqlalchemy.engine import Row

    payload = {"regime": "FRAGILE", "features_used": {f"c{i}": f"feat_{i}" for i in range(8)}}
    digest = _canonical_sha256(payload)
    flags = {"snapshot_hash": digest} if flag_key is None else {"snapshot_id": 42, "snapshot_hash": digest}
    regime_row = ("FRAGILE", 0.72, 0.15, flags, "DEFENSIVE", "2026-11-18T10:00:00Z")
    rows = _real_rows((42, payload, "2026-11-18T09:59:00Z", "2026-11-18"))
    assert isinstance(rows[0], Row) and not isinstance(rows[0], (tuple, list))

    conn = _BoundRegimeConn(regime_row, snap_rows=rows)
    snap = _make_engine(_FakeOllamaClient(None), _FakeEngine(conn), tmp_path)._gather_market_snapshot()

    assert "latest_regime_withheld" not in snap, snap.get("latest_regime_withheld")
    lr = snap["latest_regime"]
    assert (lr["snapshot_id"], lr["snapshot_hash"], lr["bound_inputs"]) == (42, digest, 8)


def test_briefing_row_helper_normalizes_rows_and_rejects_non_rows():
    row = _real_rows((1, {"a": 1}, "2026-11-18T09:59:00Z", "2026-11-18"))[0]
    assert MarketBriefingEngine._row_values(row) == (1, json.dumps({"a": 1}), "2026-11-18T09:59:00Z", "2026-11-18")
    assert MarketBriefingEngine._row_values((1, 2, 3)) == (1, 2, 3)
    for not_a_row in (None, "abc", b"abc", {"id": 1}, 7):
        assert MarketBriefingEngine._row_values(not_a_row) is None


def test_briefing_withholds_a_regime_bound_only_by_inline_features(tmp_path):
    flags = {"regime": "FRAGILE", "features_used": {f"c{i}": f"feat_{i}" for i in range(9)}}
    regime_row = ("FRAGILE", 0.72, 0.15, flags, "DEFENSIVE", "2026-11-18T10:00:00Z")
    conn = _BoundRegimeConn(regime_row)
    snap = _make_engine(_FakeOllamaClient(None), _FakeEngine(conn), tmp_path)._gather_market_snapshot()
    assert "latest_regime" not in snap
    assert snap["latest_regime_withheld"]["reason"] == "unverified_inline_binding"
    assert conn.snap_queries == []


_PAYLOAD_8 = {"regime": "FRAGILE", "features_used": {f"c{i}": f"feat_{i}" for i in range(8)}}
_PAYLOAD_1 = {"regime": "FRAGILE", "features_used": {"c0": "feat_0"}}
_INLINE_8 = {"bound_inputs": 8, "eligible_feature_ids": [f"feat_{i}" for i in range(8)]}


def _bound_regime(payload: dict, *, sid: int = 42, **extra) -> dict:
    return {"state": "FRAGILE", "snapshot_id": sid, "snapshot_hash": _canonical_sha256(payload),
            "timestamp": "2026-11-18T10:00:00Z", **extra}


@pytest.mark.parametrize("latest_regime,producer,expected", [
    # inline ids only, no producer binding at all
    ({"state": "FRAGILE", "timestamp": "2026-11-18T10:00:00Z", **_INLINE_8}, {},
     ("REFUSED_UNVERIFIED_REGIME_BINDING", "missing_producer_snapshot_binding")),
    # bound, but the packet holds no read of that snapshot
    (_bound_regime(_PAYLOAD_8, **_INLINE_8), {},
     ("REFUSED_UNVERIFIED_REGIME_BINDING", "producer_snapshot_not_captured")),
    # eight inline ids against a verified producer count of one
    (_bound_regime(_PAYLOAD_1, **_INLINE_8), {"payload": _PAYLOAD_1},
     ("REFUSED_REGIME_BINDING_MISMATCH", "inline_feature_ids_disagree_with_producer")),
    # an inline scalar count of eight against a verified producer count of one
    (_bound_regime(_PAYLOAD_1, bound_inputs=8), {"payload": _PAYLOAD_1},
     ("REFUSED_REGIME_BINDING_MISMATCH", "inline_bound_inputs_disagree_with_producer:8!=1")),
    # verified producer with one feature and no inline count
    (_bound_regime(_PAYLOAD_1), {"payload": _PAYLOAD_1},
     ("REFUSED_INSUFFICIENT_REGIME_INPUTS", "regime_bound_inputs_below_minimum:1<8")),
    # cited id read but absent
    (_bound_regime(_PAYLOAD_8), {"row": None},
     ("REFUSED_REGIME_BINDING_MISMATCH", "producer_snapshot_not_found")),
    # stored payload differs from the cited hash
    (_bound_regime(_PAYLOAD_8), {"payload": {**_PAYLOAD_8, "n_features": 99}},
     ("REFUSED_REGIME_BINDING_MISMATCH", "producer_snapshot_hash_mismatch")),
    # producer saved after the briefing was published
    (_bound_regime(_PAYLOAD_8), {"payload": _PAYLOAD_8, "created_at": "2026-11-18T15:00:00Z"},
     ("REFUSED_REGIME_BINDING_MISMATCH", "producer_snapshot_future_clock")),
    # producer asserts a different regime
    (_bound_regime({**_PAYLOAD_8, "regime": "GROWTH"}), {"payload": {**_PAYLOAD_8, "regime": "GROWTH"}},
     ("REFUSED_REGIME_BINDING_MISMATCH", "producer_snapshot_regime_mismatch")),
    # wrong category under the cited id
    (_bound_regime(_PAYLOAD_8), {"payload": _PAYLOAD_8, "category": "other"},
     ("REFUSED_REGIME_BINDING_MISMATCH", "producer_snapshot_malformed_payload")),
    # hash that is not a sha256 digest
    ({**_bound_regime(_PAYLOAD_8), "snapshot_hash": "abc"}, {"payload": _PAYLOAD_8},
     ("REFUSED_UNVERIFIED_REGIME_BINDING", "missing_producer_snapshot_binding")),
], ids=["inline_only", "not_captured", "inline8_vs_producer1", "inline_scalar8_vs_producer1", "producer1", "not_found",
        "hash_mismatch", "future_producer", "regime_mismatch", "wrong_category", "bad_hash"])
def test_packet_takes_the_regime_count_only_from_the_captured_producer(latest_regime, producer, expected):
    job = _load_packet_module()
    if not producer:
        captured = {}
    else:
        row = None if "row" in producer else {
            "id": 42, "category": producer.get("category", "regime_detection"),
            "payload": producer["payload"], "created_at": producer.get("created_at", "2026-11-18T09:59:00Z"),
        }
        captured = {"42": job.describe_producer_snapshot(row)}
    record = _briefing_record(8100, {"latest_regime": latest_regime})
    packet, exclusions = job.build_packet(_packet_bundle([record], producer_snapshots=captured))
    assert packet["publications"] == []
    assert [(e["disposition"], e["reasons"]) for e in exclusions] == [(expected[0], [expected[1]])]


def test_packet_carries_a_producer_verified_regime_with_matching_inline_count():
    job = _load_packet_module()
    record = _briefing_record(8101, {"latest_regime": _bound_regime(_PAYLOAD_8, **_INLINE_8)})
    packet, exclusions = job.build_packet(_packet_bundle(
        [record], producer_snapshots=_captured_producer(job, 42, _PAYLOAD_8),
    ))
    assert exclusions == []
    assert [p["id"] for p in packet["publications"]] == ["market_briefings:8101"]


def test_packet_refuses_every_carried_regime_when_the_producer_read_failed():
    job = _load_packet_module()
    bundle = _packet_bundle([_briefing_record(8102, {"latest_regime": _bound_regime(_PAYLOAD_8)})])
    bundle["producer_snapshots"] = None
    _, exclusions = job.build_packet(bundle)
    assert [(e["disposition"], e["reasons"]) for e in exclusions] == [
        ("REFUSED_UNVERIFIED_REGIME_BINDING", ["producer_snapshot_not_captured"]),
    ]


def test_packet_hash_is_the_producer_hash():
    from store.snapshots import canonical_payload_sha256

    job = _load_packet_module()
    payload = {"regime": "FRAGILE", "x": -0.0, "big": 10**20, "d": "é"}
    entry = job.describe_producer_snapshot(
        {"id": 1, "category": "regime_detection", "payload": json.dumps(payload), "created_at": "2026-11-18T09:59:00Z"}
    )
    assert entry["payload_sha256"] == canonical_payload_sha256(payload)
    assert entry["regime"] == "FRAGILE" and entry["bound_inputs"] is None


def test_read_producer_snapshots_queries_each_cited_id_once_with_bound_parameters():
    job = _load_packet_module()
    asof = datetime(2026, 11, 19, 6, tzinfo=timezone.utc)

    class _Result:
        def __init__(self, row):
            self._row = row

        def mappings(self):
            return self

        def first(self):
            return self._row

    class _Conn:
        def __init__(self):
            self.calls = []

        def execute(self, stmt, params):
            self.calls.append((str(stmt), dict(params)))
            if params["sid"] == 42:
                return _Result({"id": 42, "category": "regime_detection", "payload": _PAYLOAD_8,
                                "created_at": datetime(2026, 11, 18, 9, 59, tzinfo=timezone.utc)})
            return _Result(None)

    records = [
        _briefing_record(1, {"latest_regime": _bound_regime(_PAYLOAD_8, sid=42)}),
        _briefing_record(2, {"latest_regime": _bound_regime(_PAYLOAD_8, sid=42)}),
        _briefing_record(3, {"regime_detection": {"state": "FRAGILE", "producer_snapshot_id": "7"}}),
        {"channel": "thesis_snapshots", "record": {"id": 9, "snapshot_id": 5}},
    ]
    conn = _Conn()
    got = job.read_producer_snapshots(conn, records, asof)
    assert [c[1]["sid"] for c in conn.calls] == [7, 42]
    assert all(c[0] == job.PRODUCER_SNAPSHOT_QUERY for c in conn.calls)
    assert all(c[1]["category"] == "regime_detection" and c[1]["asof"] == asof for c in conn.calls)
    assert got["7"] == {"found": False}
    assert got["42"]["found"] is True and got["42"]["bound_inputs"] == 8
    assert got["42"]["payload_sha256"] == _canonical_sha256(_PAYLOAD_8)


_PG_URL = os.environ.get("GRID_TEST_DB_URL")


@pytest.mark.skipif(not _PG_URL, reason="requires disposable PostgreSQL via GRID_TEST_DB_URL")
def test_real_postgres_result_binds_briefing_and_packet(tmp_path):
    """The briefing guard and the packet read the producer row from a real
    PostgreSQL result (JSONB payload, timestamptz clock), not a tuple double."""
    from sqlalchemy import create_engine, text

    from store.snapshots import canonical_payload_sha256

    # A connection option, not a SET in a connect hook: that SET runs in an
    # implicit transaction, and the first rollback quietly reverts it.
    eng = create_engine(_PG_URL, connect_args={"options": "-csearch_path=rc_publication_binding_test"})

    with eng.begin() as c:
        c.execute(text("DROP SCHEMA IF EXISTS rc_publication_binding_test CASCADE"))
        c.execute(text("CREATE SCHEMA rc_publication_binding_test"))
        c.execute(text(
            "CREATE TABLE analytical_snapshots (id BIGSERIAL PRIMARY KEY, category TEXT NOT NULL, "
            "payload JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL, as_of_date DATE)"
        ))
        payload = {**_PAYLOAD_8, "stress_derivative": -0.0, "big": 1e20}
        sid = c.execute(
            text("INSERT INTO analytical_snapshots (category, payload, created_at, as_of_date) "
                 "VALUES ('regime_detection', CAST(:p AS JSONB), :ts, :d) RETURNING id"),
            {"p": json.dumps(payload), "ts": datetime(2026, 11, 18, 9, 59, tzinfo=timezone.utc),
             "d": date(2026, 11, 18)},
        ).scalar_one()
        stored = c.execute(text("SELECT payload FROM analytical_snapshots WHERE id = :i"), {"i": sid}).scalar_one()
    try:
        digest = canonical_payload_sha256(stored)
        regime_row = ("FRAGILE", 0.72, 0.15, {"snapshot_id": sid, "snapshot_hash": digest},
                      "DEFENSIVE", "2026-11-18T10:00:00Z")

        pg_conn = eng.connect()

        class _PgSnapshotConn(_BoundRegimeConn):
            """Every other query is faked; the producer lookup runs on PostgreSQL."""

            def execute(self, stmt, params=None):
                if "FROM analytical_snapshots" in str(stmt):
                    return pg_conn.execute(stmt, params)
                return super().execute(stmt, params)

        try:
            snap = _make_engine(
                _FakeOllamaClient(None), _FakeEngine(_PgSnapshotConn(regime_row)), tmp_path,
            )._gather_market_snapshot()
        finally:
            pg_conn.close()
        assert "latest_regime_withheld" not in snap, snap.get("latest_regime_withheld")
        assert snap["latest_regime"]["snapshot_hash"] == digest

        job = _load_packet_module()
        records = [_briefing_record(8200, snap, created_at="2026-11-18T14:00:00Z")]
        with eng.connect() as pg:
            captured = job.read_producer_snapshots(pg, records, datetime(2026, 11, 19, 6, tzinfo=timezone.utc))
        assert captured[str(sid)]["payload_sha256"] == digest
        packet, exclusions = job.build_packet(_packet_bundle(records, producer_snapshots=captured))
        assert exclusions == []
        assert [p["id"] for p in packet["publications"]] == ["market_briefings:8200"]
    finally:
        with eng.begin() as c:
            c.execute(text("DROP SCHEMA rc_publication_binding_test CASCADE"))
        eng.dispose()


@pytest.mark.parametrize("sid", ["\u00b2", "\u0663", " 4\u00b2 ", True, -1, 1.5])
def test_packet_refuses_odd_snapshot_ids_without_aborting_the_run(sid):
    """A non-ASCII digit id ("²", Arabic-Indic "٣") made int() raise and abort
    the whole packet; it now refuses that row as unverified."""
    job = _load_packet_module()
    regime = {**_bound_regime(_PAYLOAD_8), "snapshot_id": sid}
    records = [_briefing_record(8300, {"latest_regime": regime}), _briefing_record(8301, {"note": "no regime"})]
    assert job.regime_snapshot_refs(records[0]["record"]) == []
    packet, exclusions = job.build_packet(_packet_bundle(records, producer_snapshots={}))
    assert [(e["publication_id"], e["disposition"]) for e in exclusions] == [
        ("market_briefings:8300", "REFUSED_UNVERIFIED_REGIME_BINDING"),
    ]
    assert [p["id"] for p in packet["publications"]] == ["market_briefings:8301"]


# ── Follow-up to Codex's accept of #1386 (review 6014483299 on 59bd9bee) ─────
# "describe_producer_snapshot's generic count helper can still accept scalar
# bound_inputs when a producer payload lacks feature entries ... Require actual
# eligible ID entries on the producer side."


@pytest.mark.parametrize("payload", [
    {"regime": "FRAGILE", "bound_inputs": 12},
    {"regime": "FRAGILE", "n_features": 12, "feature_count": 12, "n_features_used": 12},
    {"regime": "FRAGILE", "features_used": "feat_0,feat_1,feat_2,feat_3,feat_4,feat_5,feat_6,feat_7"},
    {"regime": "FRAGILE", "features_used": None, "bound_inputs": 9},
], ids=["scalar_bound_inputs", "registry_sizes", "features_used_string", "features_used_null"])
def test_producer_without_feature_entries_proves_no_count(payload):
    """A producer payload that only states a number is not evidence of eight
    distinct eligible ids: the regime is refused as unverified."""
    job = _load_packet_module()
    entry = job.describe_producer_snapshot(
        {"id": 42, "category": "regime_detection", "payload": payload, "created_at": "2026-11-18T09:59:00Z"}
    )
    assert entry["eligible_feature_ids"] is None and entry["bound_inputs"] is None
    record = _briefing_record(8400, {"latest_regime": _bound_regime(payload)})
    packet, exclusions = job.build_packet(_packet_bundle([record], producer_snapshots={"42": entry}))
    assert packet["publications"] == []
    assert [(e["disposition"], e["reasons"]) for e in exclusions] == [
        ("REFUSED_UNVERIFIED_REGIME_BINDING", ["producer_snapshot_missing_bound_inputs"]),
    ]


def test_producer_scalar_count_never_overrides_its_feature_entries():
    job = _load_packet_module()
    payload = {**_PAYLOAD_1, "bound_inputs": 12, "n_features": 12}
    entry = job.describe_producer_snapshot(
        {"id": 42, "category": "regime_detection", "payload": payload, "created_at": "2026-11-18T09:59:00Z"}
    )
    assert entry["eligible_feature_ids"] == ["feat_0"] and entry["bound_inputs"] == 1
    record = _briefing_record(8401, {"latest_regime": _bound_regime(payload)})
    _, exclusions = job.build_packet(_packet_bundle([record], producer_snapshots={"42": entry}))
    assert [(e["disposition"], e["reasons"]) for e in exclusions] == [
        ("REFUSED_INSUFFICIENT_REGIME_INPUTS", ["regime_bound_inputs_below_minimum:1<8"]),
    ]


def test_carried_ids_must_be_the_producers_ids_not_just_as_many():
    job = _load_packet_module()
    others = {"bound_inputs": 8, "eligible_feature_ids": [f"other_{i}" for i in range(8)]}
    record = _briefing_record(8402, {"latest_regime": _bound_regime(_PAYLOAD_8, **others)})
    _, exclusions = job.build_packet(_packet_bundle(
        [record], producer_snapshots=_captured_producer(job, 42, _PAYLOAD_8),
    ))
    assert [(e["disposition"], e["reasons"]) for e in exclusions] == [
        ("REFUSED_REGIME_BINDING_MISMATCH", ["inline_feature_ids_disagree_with_producer"]),
    ]


@pytest.mark.parametrize("filter_key", ["eligible_feature_ids", "eligible_features"])
def test_packet_and_briefing_count_the_same_producer_payload_alike(tmp_path, filter_key):
    """The producer lists ten features but marks eight eligible. The briefing
    counts eight; the packet must count the same eight, or it would refuse a
    correctly bound briefing (on main it counted ten)."""
    payload = {
        "regime": "FRAGILE",
        "features_used": {f"c{i}": f"feat_{i}" for i in range(10)},
        filter_key: [f"feat_{i}" for i in range(8)],
        "ineligible_features": ["feat_9"],
    }
    digest = _canonical_sha256(payload)
    regime_row = ("FRAGILE", 0.72, 0.15, {"snapshot_id": 42, "snapshot_hash": digest},
                  "DEFENSIVE", "2026-11-18T10:00:00Z")
    conn = _BoundRegimeConn(regime_row, snap_rows=[(42, payload, "2026-11-18T09:59:00Z", date(2026, 11, 18))])
    snap = _make_engine(_FakeOllamaClient(None), _FakeEngine(conn), tmp_path)._gather_market_snapshot()
    assert snap["latest_regime"]["bound_inputs"] == 8

    job = _load_packet_module()
    captured = _captured_producer(job, 42, payload)
    assert captured["42"]["bound_inputs"] == 8
    packet, exclusions = job.build_packet(_packet_bundle([_briefing_record(8403, snap)], producer_snapshots=captured))
    assert exclusions == []
    assert [p["id"] for p in packet["publications"]] == ["market_briefings:8403"]


def test_auto_regime_payload_shape_counts_its_concept_feature_names():
    """auto_regime writes features_used = {concept: feature_name}; the
    feature names are the ids."""
    job = _load_packet_module()
    payload = {"regime": "FRAGILE", "n_features": 40,
               "features_used": {f"concept_{i}": f"feature_{i % 9}" for i in range(12)}}
    entry = job.describe_producer_snapshot(
        {"id": 7, "category": "regime_detection", "payload": payload, "created_at": "2026-11-18T09:59:00Z"}
    )
    assert entry["bound_inputs"] == 9
    assert sorted(entry["eligible_feature_ids"]) == sorted(f"feature_{i}" for i in range(9))


# ── Codex HOLD on #1387 head 328f7979 ────────────────────────────────────────
# (1) `eligible_feature_ids or eligible_features` read a present empty list as
# absent; malformed present filters must be rejected, in the briefing too.
# (2) matching inline ids could mask a contradictory scalar bound_inputs.

_FEATURES_8 = {f"c{i}": f"feat_{i}" for i in range(8)}


def _briefing_on(tmp_path, payload: dict) -> dict:
    regime_row = ("FRAGILE", 0.72, 0.15, {"snapshot_id": 42, "snapshot_hash": _canonical_sha256(payload)},
                  "DEFENSIVE", "2026-11-18T10:00:00Z")
    conn = _BoundRegimeConn(regime_row, snap_rows=[(42, payload, "2026-11-18T09:59:00Z", date(2026, 11, 18))])
    return _make_engine(_FakeOllamaClient(None), _FakeEngine(conn), tmp_path)._gather_market_snapshot()


@pytest.mark.parametrize("filter_key", ["eligible_feature_ids", "eligible_features"])
def test_a_present_empty_eligibility_filter_means_nothing_is_eligible(tmp_path, filter_key):
    payload = {"regime": "FRAGILE", "features_used": _FEATURES_8, filter_key: []}
    snap = _briefing_on(tmp_path, payload)
    assert snap["latest_regime"]["bound_inputs"] == 0
    assert snap["latest_regime"]["eligible_feature_ids"] == []

    job = _load_packet_module()
    captured = _captured_producer(job, 42, payload)
    assert captured["42"]["eligible_feature_ids"] == [] and captured["42"]["bound_inputs"] == 0
    packet, exclusions = job.build_packet(_packet_bundle([_briefing_record(8500, snap)], producer_snapshots=captured))
    assert packet["publications"] == []
    assert [(e["disposition"], e["reasons"]) for e in exclusions] == [
        ("REFUSED_INSUFFICIENT_REGIME_INPUTS", ["regime_bound_inputs_below_minimum:0<8"]),
    ]


@pytest.mark.parametrize("filters", [
    {"eligible_feature_ids": [], "eligible_features": [f"feat_{i}" for i in range(8)]},
    {"eligible_feature_ids": ["feat_0"], "eligible_features": ["feat_1"]},
    {"eligible_feature_ids": None},
    {"eligible_feature_ids": "feat_0"},
    {"eligible_features": {"feat_0": True}},
    {"ineligible_features": "feat_0"},
    {"excluded_features": None},
], ids=["empty_vs_alias", "conflicting_aliases", "null", "string", "dict", "excluded_string", "excluded_null"])
def test_malformed_or_conflicting_filters_fail_closed_on_both_sides(tmp_path, filters):
    payload = {"regime": "FRAGILE", "features_used": _FEATURES_8, **filters}
    snap = _briefing_on(tmp_path, payload)
    assert "latest_regime" not in snap
    assert snap["latest_regime_withheld"]["reason"] == "producer_snapshot_invalid_feature_filter"

    job = _load_packet_module()
    entry = _captured_producer(job, 42, payload)["42"]
    assert entry["feature_filter_invalid"] is True and entry["eligible_feature_ids"] is None
    record = _briefing_record(8501, {"latest_regime": _bound_regime(payload)})
    _, exclusions = job.build_packet(_packet_bundle([record], producer_snapshots={"42": entry}))
    assert [(e["disposition"], e["reasons"]) for e in exclusions] == [
        ("REFUSED_REGIME_BINDING_MISMATCH", ["producer_snapshot_invalid_feature_filter"]),
    ]


def test_agreeing_aliases_and_empty_exclusions_are_accepted(tmp_path):
    ids = [f"feat_{i}" for i in range(8)]
    payload = {"regime": "FRAGILE", "features_used": _FEATURES_8, "eligible_feature_ids": ids,
               "eligible_features": list(reversed(ids)), "ineligible_features": [], "excluded_features": {}}
    snap = _briefing_on(tmp_path, payload)
    assert snap["latest_regime"]["bound_inputs"] == 8
    job = _load_packet_module()
    packet, exclusions = job.build_packet(_packet_bundle(
        [_briefing_record(8502, snap)], producer_snapshots=_captured_producer(job, 42, payload),
    ))
    assert exclusions == [] and [p["id"] for p in packet["publications"]] == ["market_briefings:8502"]


@pytest.mark.parametrize("extra,reason", [
    ({"eligible_feature_ids": [f"feat_{i}" for i in range(8)], "bound_inputs": 12},
     "inline_bound_inputs_disagree_with_producer:12!=8"),
    ({"features_used": _FEATURES_8, "bound_inputs": 7},
     "inline_bound_inputs_disagree_with_producer:7!=8"),
    ({"features_used": {"c0": "feat_0"}, "eligible_feature_ids": [f"feat_{i}" for i in range(8)]},
     "inline_feature_ids_disagree_with_producer"),
    ({"eligible_feature_ids": "feat_0"}, "invalid_inline_eligible_feature_ids"),
], ids=["ids_match_count_wrong", "features_match_count_wrong", "one_claim_of_two_wrong", "ids_not_a_list"])
def test_every_inline_claim_is_checked_on_its_own(extra, reason):
    job = _load_packet_module()
    record = _briefing_record(8503, {"latest_regime": _bound_regime(_PAYLOAD_8, **extra)})
    _, exclusions = job.build_packet(_packet_bundle(
        [record], producer_snapshots=_captured_producer(job, 42, _PAYLOAD_8),
    ))
    assert [(e["disposition"], e["reasons"]) for e in exclusions] == [
        ("REFUSED_REGIME_BINDING_MISMATCH", [reason]),
    ]


def test_producer_feature_filters_rule():
    from store.snapshots import InvalidFeatureFilter, producer_feature_filters

    assert producer_feature_filters({}) == (None, [])
    assert producer_feature_filters({"eligible_feature_ids": []}) == ([], [])
    assert producer_feature_filters({"eligible_features": ["a"], "ineligible_features": ["b"],
                                     "excluded_features": {"c": 1}}) == (["a"], ["b", "c"])
    assert producer_feature_filters({"eligible_feature_ids": ["a", "b"], "eligible_features": ["b", "a", "a"]})[0] == ["b", "a", "a"]
    for bad in ({"eligible_feature_ids": None}, {"eligible_features": 3}, {"excluded_features": "x"},
                {"eligible_feature_ids": [], "eligible_features": ["a"]}):
        with pytest.raises(InvalidFeatureFilter):
            producer_feature_filters(bad)
