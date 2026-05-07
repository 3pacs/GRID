"""Tests for verification/ref_guard.py"""

from __future__ import annotations

import sys
sys.path.insert(0, ".")

from verification.ref_guard import verify_references
from verification.url_health import URLCheckResult, URLClassification


def _make_result(url: str, cls: URLClassification, wayback: str | None = None) -> URLCheckResult:
    return URLCheckResult(
        url=url, classification=cls, http_status=200 if cls == URLClassification.LIVE else 404,
        wayback_url=wayback, latency_ms=50,
    )


def test_all_live_refs_pass():
    results = [_make_result("https://a.com", URLClassification.LIVE)]
    verdict = verify_references("some text with ref", 0.9, results)
    assert verdict.action == "pass"
    assert verdict.adjusted_confidence == 0.9


def test_single_hallucinated_flags():
    results = [_make_result("https://fake.com", URLClassification.LIKELY_HALLUCINATED)]
    verdict = verify_references("some text", 0.9, results)
    assert verdict.action == "flag"
    assert verdict.adjusted_confidence < 0.9


def test_multiple_hallucinated_compounds_to_reject():
    results = [
        _make_result("https://fake1.com", URLClassification.LIKELY_HALLUCINATED),
        _make_result("https://fake2.com", URLClassification.LIKELY_HALLUCINATED),
        _make_result("https://fake3.com", URLClassification.LIKELY_HALLUCINATED),
    ]
    verdict = verify_references("text", 0.9, results)
    # 0.5^3 = 0.125 → 0.9 * 0.125 = 0.1125 < 0.9 * 0.4 = 0.36 → reject
    assert verdict.action == "reject"


def test_majority_dead_degrades():
    results = [
        _make_result("https://dead1.com", URLClassification.DEAD, "https://web.archive.org/1"),
        _make_result("https://dead2.com", URLClassification.DEAD, "https://web.archive.org/2"),
        _make_result("https://live.com", URLClassification.LIVE),
    ]
    verdict = verify_references("text", 0.9, results)
    assert verdict.action == "clean"


def test_empty_results_pass():
    verdict = verify_references("no refs", 0.9, [])
    assert verdict.action == "pass"
    assert verdict.adjusted_confidence == 0.9


def test_over_citation_penalty():
    results = [_make_result(f"https://r{i}.com", URLClassification.LIVE) for i in range(8)]
    # 8 refs in short text (< 500 words)
    verdict = verify_references("short text here", 0.9, results)
    # Should get density penalty
    density_check = [c for c in verdict.checks if c.check_name == "ref_density"]
    assert len(density_check) == 1
    assert not density_check[0].passed
