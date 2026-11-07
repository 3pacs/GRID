"""FinBERT writes one score per (series_id, obs_date).

hf_financial_news holds several articles per day under one series_id, and
every insert in a scoring transaction shares a pull_timestamp, so the old
per-article inserts collided on uq_raw_series_composite and rolled each batch
back (scorer stuck from 2026-11-06).
"""

from __future__ import annotations

import importlib.util
import sys
import types
from datetime import date

import pytest

# The aggregation helper is pure Python; stub the model libraries only while
# importing where they are not installed (CI has no torch wheel), then remove
# the stubs so other tests' importorskip("torch") still skips.
_stubbed: list[str] = []
for _name in ("torch", "transformers"):
    if _name not in sys.modules and importlib.util.find_spec(_name) is None:
        _stub = types.ModuleType(_name)
        _stub.AutoModelForSequenceClassification = object  # type: ignore[attr-defined]
        _stub.AutoTokenizer = object  # type: ignore[attr-defined]
        sys.modules[_name] = _stub
        _stubbed.append(_name)
try:
    from ingestion.ml.finbert_scorer import aggregate_daily_scores  # noqa: E402
finally:
    for _name in _stubbed:
        sys.modules.pop(_name, None)


def _sc(pos: float, neg: float, neu: float) -> dict:
    return {"label": "x", "score": 0.0, "positive": pos, "negative": neg, "neutral": neu}


def test_same_day_texts_collapse_to_one_mean_row() -> None:
    d = date(2018, 1, 2)
    scoreable = [(1, "hf_news.a", d, "first"), (2, "hf_news.a", d, "second"), (3, "hf_news.b", d, "other")]
    scores = [_sc(0.8, 0.1, 0.1), _sc(0.2, 0.5, 0.3), _sc(0.1, 0.1, 0.8)]
    out = aggregate_daily_scores(scoreable, scores)
    assert [(g["series_id"], g["n_texts"]) for g in out] == [("hf_news.a", 2), ("hf_news.b", 1)]
    a = out[0]
    assert a["positive"] == pytest.approx(0.5)
    assert a["negative"] == pytest.approx(0.3)
    assert a["compound"] == pytest.approx(0.2)
    assert a["label"] == "positive"
    assert a["text_snippet"] == "first"
    assert out[1]["label"] == "neutral"


def test_truncated_fetch_drops_the_partial_oldest_day() -> None:
    new, old = date(2018, 1, 3), date(2018, 1, 2)
    scoreable = [(1, "s", new, "n1"), (2, "s", old, "o1")]
    scores = [_sc(0.5, 0.2, 0.3), _sc(0.1, 0.6, 0.3)]
    out = aggregate_daily_scores(scoreable, scores, truncated=True)
    assert [g["obs_date"] for g in out] == [new]
    assert [g["obs_date"] for g in aggregate_daily_scores(scoreable, scores)] == [new, old]


def test_truncated_single_day_is_kept() -> None:
    d = date(2018, 1, 2)
    out = aggregate_daily_scores([(1, "s", d, "t")], [_sc(0.3, 0.3, 0.4)], truncated=True)
    assert len(out) == 1 and out[0]["label"] == "neutral"


def test_writer_is_idempotent_and_orders_newest_vintage_first() -> None:
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "ingestion" / "ml" / "finbert_scorer.py").read_text()
    assert "ON CONFLICT DO NOTHING" in src
    assert src.count("pull_timestamp DESC") >= 2
