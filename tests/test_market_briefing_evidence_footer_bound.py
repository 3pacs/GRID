"""Bounded publication evidence appendix (market_briefings row 5007 regression).

On 2026-11-06 14:27Z the hourly briefing row id 5007 was written with
1,527,952 characters of ``content``: a 1,617-character narrative followed by a
"Feature evidence declarations" JSON appendix of 1,005 records. The narrative
mentioned ``0%``; 961 of the ~2,700 model-eligible features in the snapshot had
a latest value of exactly 0, every one equal-value-matched the mention, and
``_publication_evidence_footer`` rendered a ~1.5 KB declaration record for each
of them. The same fan-out, replicated across three stage roles, made the row's
``snapshot_data`` 30 MB. Dozens of hourly rows since 2026-10-26 (when the
appendix was introduced) carry 100-550 KB of content from the same mechanism.

These tests pin the bounded behaviour with the REAL module (no AST loader, no
network, no DB, no model): a mention with more equal-value matches than the
diagnostics list cap gets ``ambiguity_overflow`` and no records, listing stays
small, legitimate low-fan-out attribution is unchanged, and the appendix byte
cap withholds the whole appendix with a declared note instead of trimming.
"""
from __future__ import annotations

import json
import os
import types
from unittest.mock import Mock, patch


os.environ.setdefault("DB_PASSWORD", "test-password")

from ollama import market_briefing as publication
from ollama.market_briefing import MarketBriefingEngine

FANOUT = 961  # zero-valued features in the 2026-11-06 snapshot


def _fanout_snapshot(extra: dict | None = None) -> dict:
    features = {f"zero_feature_{i:04d}": {"value": 0.0, "date": "2026-11-05"} for i in range(FANOUT)}
    features.update(extra or {})
    return {"timestamp": "2026-11-06T14:27:48", "features": features}


def _diagnostics(narrative: str, snapshot: dict, context: str, **kwargs) -> dict:
    return publication._publication_stage_diagnostics(
        narrative, narrative, narrative, context, snapshot, None, **kwargs
    )


def _decode(rendered: str) -> dict:
    block = rendered.split("```json\n", 1)[1].split("\n```", 1)[0]
    return json.loads(block)


# ── stage diagnostics listing is bounded, counting is not ─────────────────


def test_diagnostics_list_every_match_counted_but_listing_capped():
    narrative = "Breadth sits at 0% today."
    diagnostics = _diagnostics(narrative, _fanout_snapshot(), "breadth=0%")
    assert diagnostics["snapshot_match_list_cap"] == 8
    for role in ("candidate", "grounding_input", "emitted"):
        entries = diagnostics["roles"][role]["numeric_mentions"]
        assert len(entries) == 1
        entry = entries[0]
        assert entry["snapshot_match_count"] == FANOUT
        assert entry["snapshot_matches_complete"] is False
        assert len(entry["snapshot_matches"]) == 8
        assert entry["retained_snapshot_presence"] == "present"
    # The whole diagnostics structure stays far below the 30 MB production rows.
    assert len(json.dumps(diagnostics)) < 20_000


def test_diagnostics_small_fanout_lists_everything_and_is_marked_complete():
    snapshot = {"features": {"alpha": {"value": 28.18}, "beta": {"value": 28.18}}}
    entry = _diagnostics("VIX is 28.18.", snapshot, "vix=28.18")["roles"]["emitted"]["numeric_mentions"][0]
    assert entry["snapshot_match_count"] == 2
    assert entry["snapshot_matches_complete"] is True
    assert sorted(m["path"] for m in entry["snapshot_matches"]) == ["features.alpha.value", "features.beta.value"]


def test_diagnostics_cap_is_a_keyword_so_contract_loaders_need_no_module_constant():
    snapshot = _fanout_snapshot()
    entry = _diagnostics("0% breadth.", snapshot, "0%", match_list_cap=3)["roles"]["emitted"]["numeric_mentions"][0]
    assert len(entry["snapshot_matches"]) == 3
    assert entry["snapshot_match_count"] == FANOUT


# ── evidence footer refuses attribution for overflowing mentions ──────────


def test_footer_overflow_mention_gets_no_records_and_is_declared():
    narrative = "Breadth sits at 0% today."
    snapshot = _fanout_snapshot()
    diagnostics = _diagnostics(narrative, snapshot, "breadth=0%")
    footer = publication._publication_evidence_footer(snapshot, diagnostics, narrative_span=(0, len(narrative)))
    assert footer["records"] == []
    assert footer["rendered_text"] == ""
    assert footer["rendering"]["status"] == "empty"
    assert footer["rendering"]["overflow_mentions"] == 1
    assert footer["mentions"] == [{
        "start": narrative.index("0%"), "end": narrative.index("0%") + 2, "feature_paths": [],
        "attribution": "ambiguity_overflow", "numeric_context_match": True,
        "snapshot_match_count": FANOUT,
    }]


def test_footer_mixed_narrative_keeps_unique_attribution_and_drops_fanout():
    narrative = "VIX is 28.18 while breadth sits at 0%."
    snapshot = _fanout_snapshot({"vix_close": {"value": 28.18, "date": "2026-11-06", "source_id": "synthetic"}})
    diagnostics = _diagnostics(narrative, snapshot, "vix=28.18 breadth=0%")
    footer = publication._publication_evidence_footer(snapshot, diagnostics, narrative_span=(0, len(narrative)))
    assert [r["feature_key"] for r in footer["records"]] == ["vix_close"]
    assert [m["attribution"] for m in footer["mentions"]] == ["candidate_only", "ambiguity_overflow"]
    assert footer["rendering"]["status"] == "rendered"
    decoded = _decode(footer["rendered_text"])
    assert [r["feature_key"] for r in decoded["records"]] == ["vix_close"]
    assert decoded["mentions"][1]["snapshot_match_count"] == FANOUT
    # Row 5007 carried 1,526,335 characters of appendix for five mentions.
    assert len(footer["rendered_text"]) < 8_000


def test_footer_low_fanout_attribution_is_unchanged():
    snapshot = {"features": {"alpha": {"value": 0.125}, "beta": {"value": 0.125}, "gamma": {"value": 0.125}}}
    narrative = "Observation 0.125."
    diagnostics = _diagnostics(narrative, snapshot, "supplied 0.125.")
    footer = publication._publication_evidence_footer(snapshot, diagnostics, narrative_span=(0, len(narrative)))
    assert [r["feature_key"] for r in footer["records"]] == ["alpha", "beta", "gamma"]
    assert footer["mentions"][0]["attribution"] == "ambiguous_candidates"
    assert footer["mentions"][0]["feature_paths"] == [
        "features.alpha.value", "features.beta.value", "features.gamma.value"]
    assert "snapshot_match_count" not in footer["mentions"][0]
    assert footer["rendering"]["status"] == "rendered"


def test_footer_without_completeness_flag_behaves_as_before():
    # Hand-built diagnostics (the contract tests' shape) carry no completeness flag.
    diagnostics = {"roles": {"emitted": {"numeric_mentions": [{
        "start": 8, "end": 13, "generation_context_numeric_match": False,
        "snapshot_matches": [{"path": "features.alpha.value"}]}]}}}
    footer = publication._publication_evidence_footer(
        {"features": {"alpha": {"value": 0.125}}}, diagnostics, narrative_span=(0, 20))
    assert [r["feature_key"] for r in footer["records"]] == ["alpha"]
    assert footer["mentions"][0]["attribution"] == "candidate_only"


# ── appendix byte cap: withhold whole, declare, never trim ─────────────────


def test_footer_over_byte_cap_withholds_whole_appendix_with_declared_note():
    snapshot = {"features": {"alpha": {"value": 0.125}, "beta": {"value": 0.125}}}
    narrative = "Observation 0.125."
    diagnostics = _diagnostics(narrative, snapshot, "supplied 0.125.")
    footer = publication._publication_evidence_footer(
        snapshot, diagnostics, narrative_span=(0, len(narrative)), rendered_cap_bytes=500)
    assert footer["rendering"]["status"] == "withheld_over_cap"
    assert footer["rendering"]["encoded_bytes"] > 500 == footer["rendering"]["cap_bytes"]
    assert "```json" not in footer["rendered_text"]
    assert "Appendix withheld: 2 declaration records" in footer["rendered_text"]
    assert footer["rendered_text"].startswith("\n\n---\n### Feature evidence declarations\n")
    assert len(footer["rendered_text"]) < 400
    # Records are not trimmed: the stored payload still declares both.
    assert [r["feature_key"] for r in footer["records"]] == ["alpha", "beta"]


def test_footer_default_byte_cap_is_a_quarter_mebibyte():
    import inspect
    cap = inspect.signature(publication._publication_evidence_footer).parameters["rendered_cap_bytes"].default
    assert cap == 262_144
    # Below the packet producer's 1 MiB per-text cap with a normal narrative in front of it.
    assert cap + 64_000 < 1_048_576


# ── end to end through generate_briefing ─────────────────────────────────


def _generate(response: str, snapshot: dict, context: str) -> dict:
    engine = MarketBriefingEngine.__new__(MarketBriefingEngine)
    engine.engine = object()
    engine.ollama = types.SimpleNamespace(chat=Mock(return_value=response))
    engine._gather_market_snapshot = Mock(return_value=snapshot)
    engine._build_data_context = Mock(return_value=context)
    engine._get_system_prompt = Mock(return_value="SYNTHETIC")
    engine._get_user_prompt = Mock(return_value="SYNTHETIC")
    engine._save_briefing = Mock()
    engine._persist_to_db = Mock()
    optional = types.ModuleType("disabled_optional_context")
    with patch.dict(__import__("sys").modules, {
            "ingestion.wiki_history": optional, "ingestion.social_sentiment": optional,
            "intelligence.sentiment_scorer": optional}):
        return engine.generate_briefing(save=True)


def test_generate_briefing_row_5007_shape_stays_bounded():
    # "VIX" is avoided on purpose: the publication context guard omits VIX claims
    # without a volatility observation, which is a different stage from the one
    # under test here. The 2026-11-06 row's "0.54" (12 matches) and "0%" (961
    # matches) mentions are reproduced as written.
    narrative = ("The market is in a **FRAGILE** regime (confidence 0.54). Breadth sits at 0%. "
                 "Observation alpha reads 28.18.")
    snapshot = _fanout_snapshot({"alpha": {"value": 28.18, "date": "2026-11-06"},
                                 **{f"half_{i}": {"value": 0.54} for i in range(12)}})
    result = _generate(narrative, snapshot, "regime confidence 0.54; breadth 0%; alpha 28.18")
    content = result["content"]
    assert content.startswith(narrative)          # narrative intact, nothing cut
    assert result["grounding"]["ungrounded_count"] == 0
    footer = result["publication_evidence_footer"]
    assert [r["feature_key"] for r in footer["records"]] == ["alpha"]
    assert [m["attribution"] for m in footer["mentions"]] == [
        "ambiguity_overflow", "ambiguity_overflow", "candidate_only"]
    assert footer["mentions"][0]["snapshot_match_count"] == 12
    assert footer["mentions"][1]["snapshot_match_count"] == FANOUT
    assert content.endswith(footer["rendered_text"])
    assert len(content) < 10_000                  # row 5007: 1,527,952
    stored = json.dumps(result["snapshot"], default=str)
    assert len(stored) < 150_000                  # row 5007: 30,849,893


def test_generate_briefing_data_summary_fallback_is_bounded_too():
    # Model unavailable: the deterministic data summary lists 20 features, several
    # of which are zero-valued, and every one equal-value-matches the fan-out set.
    snapshot = _fanout_snapshot()
    result = _generate(None, snapshot, "no numbers here")
    assert result["publication_status"]["content_kind"] == "data_summary"
    assert len(result["content"]) < 30_000
    assert len(json.dumps(result["snapshot"], default=str)) < 400_000
    footer = result["publication_evidence_footer"]
    assert footer["records"] == []
    assert all(m["attribution"] == "ambiguity_overflow" for m in footer["mentions"])
