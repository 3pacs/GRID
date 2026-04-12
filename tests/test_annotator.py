"""Tests for verification/annotator.py"""

from __future__ import annotations

import sys
sys.path.insert(0, ".")

from verification.annotator import annotate_output
from verification.url_health import URLCheckResult, URLClassification


def _make_result(url: str, cls: URLClassification, wayback: str | None = None) -> URLCheckResult:
    return URLCheckResult(
        url=url, classification=cls,
        http_status=200 if cls == URLClassification.LIVE else 404,
        wayback_url=wayback, latency_ms=50,
    )


def test_live_urls_unchanged():
    text = "See [Reuters](https://reuters.com/article) for details."
    results = [_make_result("https://reuters.com/article", URLClassification.LIVE)]
    out = annotate_output(text, results)
    assert out.cleaned_text == text
    assert out.removed_count == 0
    assert out.replaced_count == 0


def test_dead_urls_replaced_with_wayback():
    text = "See [old article](https://dead.com/article) here."
    results = [_make_result(
        "https://dead.com/article", URLClassification.DEAD,
        wayback="https://web.archive.org/web/2025/https://dead.com/article",
    )]
    out = annotate_output(text, results)
    assert "web.archive.org" in out.cleaned_text
    # The original bare URL is replaced, but it appears inside the wayback URL
    assert out.cleaned_text.count("dead.com/article") == 1  # only inside wayback URL
    assert "(https://web.archive.org/" in out.cleaned_text
    assert out.replaced_count == 1


def test_hallucinated_markdown_link_removed():
    text = "According to [this study](https://fake.com/study123) the data shows..."
    results = [_make_result("https://fake.com/study123", URLClassification.LIKELY_HALLUCINATED)]
    out = annotate_output(text, results)
    assert "fake.com" not in out.cleaned_text
    assert "this study" in out.cleaned_text
    assert "[source not verified]" in out.cleaned_text
    assert out.removed_count == 1


def test_unknown_urls_tagged():
    text = "See [report](https://timeout.com/report) for context."
    results = [_make_result("https://timeout.com/report", URLClassification.UNKNOWN)]
    out = annotate_output(text, results)
    assert "[unverified]" in out.cleaned_text


def test_empty_results():
    text = "No URLs here."
    out = annotate_output(text, [])
    assert out.cleaned_text == text


def test_mixed_results():
    text = (
        "Live: [a](https://live.com) "
        "Dead: [b](https://dead.com) "
        "Fake: [c](https://fake.com)"
    )
    results = [
        _make_result("https://live.com", URLClassification.LIVE),
        _make_result("https://dead.com", URLClassification.DEAD, "https://web.archive.org/dead"),
        _make_result("https://fake.com", URLClassification.LIKELY_HALLUCINATED),
    ]
    out = annotate_output(text, results)
    assert "live.com" in out.cleaned_text
    assert "web.archive.org/dead" in out.cleaned_text
    assert "fake.com" not in out.cleaned_text
    assert "[source not verified]" in out.cleaned_text
