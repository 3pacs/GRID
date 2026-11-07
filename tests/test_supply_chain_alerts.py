"""Tests for alerts/supply_chain_alerts.py.

Covers the five mandated scenarios plus a few extras:
  - concentration shift detection via mocked snapshot iterator
  - chokepoint degradation detection via mocked snapshot iterator
  - new-seen tracking via mocked alert_state lookups
  - large acquisition filter
  - digest HTML formatting
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from alerts import supply_chain_alerts as sca


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fake_engine_with_conn(conn):
    """Return a mock engine whose .connect() returns a context manager
    yielding ``conn`` and whose .begin() does the same.
    """
    engine = MagicMock()

    class _CM:
        def __enter__(self_inner):
            return conn

        def __exit__(self_inner, *exc):
            return False

    engine.connect.side_effect = lambda: _CM()
    engine.begin.side_effect = lambda: _CM()
    return engine


def _edge(
    *,
    upstream_id: str = "u",
    downstream_id: str = "d",
    relationship: str = "raw_material",
    input_type: str | None = "widget",
    pct_downstream_cogs: float | None = None,
    chokepoint_score: float | None = None,
    annual_usd: float | None = None,
    prev_score: float | None = None,
    prev_pct: float | None = None,
):
    return {
        "upstream_id": upstream_id,
        "downstream_id": downstream_id,
        "relationship": relationship,
        "input_type": input_type,
        "pct_downstream_cogs": pct_downstream_cogs,
        "chokepoint_score": chokepoint_score,
        "annual_usd": annual_usd,
        "prev_score": prev_score,
        "prev_pct": prev_pct,
        "snapshotted_at": None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Pure helpers
# ─────────────────────────────────────────────────────────────────────────────

def test_edge_key_builds_pipe_separated():
    assert sca._edge_key("A", "B", "raw_material") == "A|B|raw_material"
    assert sca._edge_key("A", "B", None) == "A|B|"


def test_canvas_link_points_at_supply_view():
    link = sca._canvas_link("HSY")
    assert link.endswith("/HSY/supply")
    assert sca._canvas_link(None) == sca.CANVAS_LINK_BASE


def test_fmt_usd_scales_suffixes():
    assert sca._fmt_usd(1_500_000_000) == "$1.50B"
    assert sca._fmt_usd(2_500_000) == "$2.5M"
    assert sca._fmt_usd(None) == "n/a"


# ─────────────────────────────────────────────────────────────────────────────
# Detectors
# ─────────────────────────────────────────────────────────────────────────────

def test_concentration_shift_detects_delta_above_threshold():
    edges = [
        _edge(pct_downstream_cogs=0.12, prev_pct=0.05),   # +7pp -> fire
        _edge(upstream_id="u2", pct_downstream_cogs=0.06, prev_pct=0.05),  # 1pp skip
        _edge(upstream_id="u3", pct_downstream_cogs=None, prev_pct=0.05),  # none skip
    ]
    conn = MagicMock()
    engine = _fake_engine_with_conn(conn)

    with patch.object(sca, "_iter_edges_with_snap", return_value=edges), \
         patch.object(sca, "_already_alerted", return_value=False):
        out = sca.detect_concentration_shifts(engine, threshold_pp=0.05)

    assert len(out) == 1
    assert out[0]["alert_type"] == "concentration_shift"
    assert out[0]["payload"]["delta_pp"] == pytest.approx(0.07)
    assert "+7.0pp" in out[0]["delta"]


def test_concentration_shift_respects_dedup():
    edges = [_edge(pct_downstream_cogs=0.2, prev_pct=0.1)]
    conn = MagicMock()
    engine = _fake_engine_with_conn(conn)

    with patch.object(sca, "_iter_edges_with_snap", return_value=edges), \
         patch.object(sca, "_already_alerted", return_value=True):
        out = sca.detect_concentration_shifts(engine)
    assert out == []


def test_chokepoint_degradation_detects_score_rise():
    edges = [
        _edge(chokepoint_score=0.72, prev_score=0.55),   # +0.17 -> fire
        _edge(upstream_id="u2", chokepoint_score=0.60, prev_score=0.55),  # +0.05 skip
        _edge(upstream_id="u3", chokepoint_score=0.30, prev_score=0.60),  # drop, skip
    ]
    conn = MagicMock()
    engine = _fake_engine_with_conn(conn)

    with patch.object(sca, "_iter_edges_with_snap", return_value=edges), \
         patch.object(sca, "_already_alerted", return_value=False):
        out = sca.detect_chokepoint_degradation(engine, delta_threshold=0.15)

    assert len(out) == 1
    finding = out[0]
    assert finding["alert_type"] == "chokepoint_degradation"
    assert finding["payload"]["delta"] == pytest.approx(0.17)


def test_new_high_chokepoint_crossing_from_below():
    edges = [
        _edge(chokepoint_score=0.75, prev_score=0.55),   # crossing
        _edge(upstream_id="u2", chokepoint_score=0.80, prev_score=0.78),  # already high
        _edge(upstream_id="u3", chokepoint_score=0.65, prev_score=0.50),  # still low
        _edge(upstream_id="u4", chokepoint_score=0.72, prev_score=None),   # first snap
    ]
    conn = MagicMock()
    engine = _fake_engine_with_conn(conn)

    with patch.object(sca, "_iter_edges_with_snap", return_value=edges), \
         patch.object(sca, "_already_alerted", return_value=False):
        out = sca.detect_new_high_chokepoints(engine, threshold=0.7)

    fired = {f["payload"]["edge_key"] for f in out}
    assert any(k.startswith("u|d|") for k in fired)
    assert any(k.startswith("u4|") for k in fired)
    assert len(out) == 2


def test_new_seen_tracking_via_already_alerted():
    """Dedup: once a finding is in alert_state we do not re-emit."""
    edges = [_edge(chokepoint_score=0.9, prev_score=0.5)]
    conn = MagicMock()
    engine = _fake_engine_with_conn(conn)

    with patch.object(sca, "_iter_edges_with_snap", return_value=edges), \
         patch.object(sca, "_already_alerted", return_value=True):
        out = sca.detect_chokepoint_degradation(engine)
    assert out == []

    with patch.object(sca, "_iter_edges_with_snap", return_value=edges), \
         patch.object(sca, "_already_alerted", return_value=False):
        out = sca.detect_chokepoint_degradation(engine)
    assert len(out) == 1


def test_large_acquisition_filter_threshold_and_formatting():
    acq_rows = [
        # (actor_id, counterparty_id, amount_usd, fiscal_period,
        #  source_filing, as_of)
        ("MSFT", "ATVI", 6.9e10, None, "8-K 2026-04-01", None),
    ]
    conn = MagicMock()
    # execute() -> object with .fetchall
    exec_result = MagicMock()
    exec_result.fetchall.return_value = acq_rows
    conn.execute.return_value = exec_result

    engine = _fake_engine_with_conn(conn)
    with patch.object(sca, "_already_alerted", return_value=False):
        out = sca.detect_large_acquisitions(engine, min_usd=5e9)

    assert len(out) == 1
    f = out[0]
    assert f["alert_type"] == "large_acquisition"
    assert f["entity"] == "MSFT"
    assert "$69.00B" in f["delta"]
    assert f["payload"]["amount_usd"] == 6.9e10


# ─────────────────────────────────────────────────────────────────────────────
# Digest HTML formatting
# ─────────────────────────────────────────────────────────────────────────────

def test_render_digest_html_contains_sections_and_subject():
    findings = {
        "new_suppliers": [],
        "concentration_shifts": [
            {
                "alert_type": "concentration_shift",
                "key": "concentration_shift:A|B|r:0.12",
                "entity": "A -> B",
                "headline": "Concentration shift: A -> B",
                "delta": "+7.0pp (now 12.0%)",
                "context": "B COGS exposure moved 5 -> 12 for widget.",
                "deep_link": "https://grid.stepdad.finance/#/canvas/B/supply",
                "payload": {},
            }
        ],
        "chokepoint_degradation": [],
        "new_high_chokepoints": [],
        "geographic_spikes": [],
        "large_acquisitions": [
            {
                "alert_type": "large_acquisition",
                "key": "large_acquisition:MSFT:ATVI:None:69000000000",
                "entity": "MSFT",
                "headline": "Large acquisition announced: MSFT $69.00B",
                "delta": "$69.00B",
                "context": "MSFT announced an acquisition of ATVI for $69.00B.",
                "deep_link": "https://grid.stepdad.finance/#/canvas/MSFT/supply",
                "payload": {"amount_usd": 6.9e10},
            }
        ],
        "contagion_risk": [],
    }
    html = sca.render_digest_html(findings)
    assert "Supply Chain Pulse" in html
    assert "Concentration Shifts" in html
    assert "Large Acquisitions" in html
    assert "A -&gt; B" in html  # HTML escaped
    assert "MSFT" in html
    assert "canvas/MSFT/supply" in html


def test_render_digest_html_does_not_claim_all_clear_when_empty():
    html = sca.render_digest_html({k: [] for k in sca.DETECTOR_ORDER})
    assert "No findings supplied" in html
    assert "All Clear" not in html


def test_run_all_aggregates_and_returns_counts():
    conn = MagicMock()
    engine = _fake_engine_with_conn(conn)

    with patch.object(sca, "detect_new_suppliers", return_value=[{"alert_type": "new_supplier", "key": "k", "entity": "e", "headline": "h", "delta": "d", "context": "c", "deep_link": "l", "payload": {}}]), \
         patch.object(sca, "detect_concentration_shifts", return_value=[]), \
         patch.object(sca, "detect_chokepoint_degradation", return_value=[]), \
         patch.object(sca, "detect_new_high_chokepoints", return_value=[]), \
         patch.object(sca, "detect_geographic_spikes", return_value=[]), \
         patch.object(sca, "detect_large_acquisitions", return_value=[]), \
         patch.object(sca, "detect_contagion_risk", return_value=[]), \
         patch.object(sca, "refresh_snapshots", return_value=42):
        result = sca.run_all(engine, since_hours=24, send_email=False)

    assert result["total"] == 1
    assert result["sent"] is False
    assert result["snapshots_written"] == 42
    assert len(result["findings"]["new_suppliers"]) == 1



def test_supply_summary_counts_findings_without_claiming_unique_incidents():
    finding = {"entity": "A", "headline": "A risk", "delta": "0.90",
               "context": "Full detail", "payload": {"edge_key": "A|B|r", "chokepoint_score": .9}}
    findings = {"new_high_chokepoints": [finding], "contagion_risk": [finding],
                "large_acquisitions": [{"headline": "Deal", "payload": {"amount_usd": 6e9}}]}
    html = sca.render_digest_html(findings)
    assert "3 findings shown" in html
    assert "0.90" in html and "not a probability" in html
    assert "$6.00B" in html and "largest reported acquisition" in html
    assert "may describe the same edge" in html
    assert html.index("At a glance") < html.index("New High Chokepoints")
    assert "font-size:32px" in html


def test_supply_send_and_preview_share_summary():
    finding = {"headline": "Deal", "payload": {"amount_usd": 6e9}}
    with patch("alerts.email._send") as send:
        assert sca.send_digest({"large_acquisitions": [finding]})
    assert send.call_args.args[1][0]["title"] == "At a glance"
    assert "1 findings shown" in send.call_args.args[1][0]["body"]


def test_supply_failure_coverage_is_not_all_clear():
    html = sca.render_digest_html({}, scan_context={"failed_detectors": ["new_suppliers"],
                                                  "since_hours": 48})
    assert "new_suppliers" in html and "failed" in html
    assert "48-hour" in html
    assert "All Clear" not in html


def test_supply_links_are_escaped_and_unsafe_schemes_rejected():
    row = {"headline": '<img src=x onerror="x">', "deep_link": 'javascript:alert(1)'}
    html = sca._render_finding_row(row)
    assert "<img" not in html
    assert "javascript:" not in html
    html = sca._render_finding_row({"deep_link": 'https://example.test/?x="a"&b=c'})
    assert '&quot;a&quot;&amp;b=c' in html



def test_supply_summary_ignores_nonfinite_metrics_and_keeps_unknown_groups():
    findings = {"new_high_chokepoints": [{"payload": {"chokepoint_score": float("nan")}}],
                "large_acquisitions": [{"payload": {"amount_usd": float("inf")}}],
                "custom_group": [{"headline": "Custom <finding>"}]}
    html = sca.render_digest_html(findings)
    assert "3 findings shown" in html
    assert "Highest reported" not in html
    assert "largest reported acquisition" not in html
    assert "Custom &lt;finding&gt;" in html


def test_supply_summary_concentration_uses_percentage_points():
    html = sca.render_digest_html({"concentration_shifts": [{"payload": {"delta_pp": -.075}}]})
    assert "-7.5 percentage points" in html
    assert "not a percentage return" in html


def test_supply_finding_keeps_full_detail_and_provenance():
    row = {"context": "Detailed context " * 100 + "END DETAIL", "alert_type": "large_acquisition",
           "payload": {"baseline_at": "<old>", "record_as_of": "2026-04-01"}}
    html = sca._render_finding_row(row)
    assert "END DETAIL" in html
    assert "Comparison baseline: &lt;old&gt;" in html
    assert "Record as of: 2026-04-01" in html
    assert "not confirmation the transaction closed" in html


def test_run_all_passes_failure_context_without_changing_detection(monkeypatch):
    finding = {"alert_type": "large_acquisition", "key": "k", "entity": "A", "headline": "h",
               "delta": "d", "context": "detail", "deep_link": "https://example.test", "payload": {}}
    for name in sca.DETECTOR_ORDER:
        monkeypatch.setattr(sca, f"detect_{name}", lambda *a, **kw: [])
    monkeypatch.setattr(sca, "detect_large_acquisitions", lambda *a, **kw: [finding])
    def fail(*a, **kw):
        raise RuntimeError("synthetic source failure")
    monkeypatch.setattr(sca, "detect_new_suppliers", fail)
    monkeypatch.setattr(sca, "refresh_snapshots", lambda *a: 0)
    with patch.object(sca, "send_digest", return_value=False) as send:
        result = sca.run_all(MagicMock(), since_hours=48, send_email=True)
    assert result["failed_detectors"] == ["new_suppliers"]
    assert result["total"] == 1
    assert send.call_args.kwargs["scan_context"]["since_hours"] == 48
    assert send.call_args.kwargs["scan_context"]["failed_detectors"] == ["new_suppliers"]


def test_acquisition_preserves_actual_record_timestamp():
    from datetime import datetime, timezone
    conn = MagicMock()
    timestamp = datetime(2026, 11, 1, tzinfo=timezone.utc)
    conn.execute.return_value.fetchall.return_value = [("A", "B", 6e9, None, "filing", timestamp)]
    with patch.object(sca, "_already_alerted", return_value=False):
        finding = sca.detect_large_acquisitions(_fake_engine_with_conn(conn))[0]
    assert finding["payload"]["record_as_of"] == timestamp.isoformat()


def test_supply_empty_send_stays_suppressed():
    with patch("alerts.email._send") as send:
        assert sca.send_digest({}) is False
    send.assert_not_called()



@pytest.mark.parametrize("failed", sca.DETECTOR_ORDER)
def test_each_detector_failure_is_visible_in_sent_summary(monkeypatch, failed):
    finding = {"alert_type": "new_supplier", "key": "k", "entity": "A", "headline": "h",
               "delta": "d", "context": "detail", "deep_link": "https://example.test", "payload": {}}
    for name in sca.DETECTOR_ORDER:
        monkeypatch.setattr(sca, f"detect_{name}", lambda *a, **kw: [finding])
    def fail(*a, **kw):
        raise RuntimeError("synthetic source failure")
    monkeypatch.setattr(sca, f"detect_{failed}", fail)
    monkeypatch.setattr(sca, "refresh_snapshots", lambda *a: 0)
    with patch("alerts.email._send") as send:
        result = sca.run_all(MagicMock(), send_email=True)
    assert result["failed_detectors"] == [failed]
    assert result["total"] == 6
    summary = send.call_args.args[1][0]["body"]
    assert f"detector(s) failed: {failed}" in summary
    assert "not a source-data timestamp" in summary


def test_supply_malformed_url_uses_canvas_fallback():
    html = sca._render_finding_row({"deep_link": "https://[bad"})
    assert 'href="https://grid.stepdad.finance/#/canvas"' in html
