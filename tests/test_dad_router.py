from api.routers.dad import (
    _fit_signals,
    _gold_from_summary,
    _grid_decision_stack,
    _normalize_ticker,
    _parse_finviz_value,
    _shorten,
)


def test_normalize_ticker_allows_common_symbols():
    assert _normalize_ticker(" $purr ") == "PURR"
    assert _normalize_ticker("brk-b") == "BRK.B"
    assert _normalize_ticker(" rxt; drop ") == "RXT"


def test_gold_from_summary_scores_high_workbook_footprint():
    summary = {
        "mentions": 28,
        "file_count": 4,
        "sheet_count": 9,
        "evidence_score": 18.5,
        "source_types": "cell,filename,sheet_name",
    }

    gold = _gold_from_summary(summary)

    assert gold["verdict"] == "High workbook conviction"
    assert gold["tone"] == "strong"
    assert gold["score"] >= 80


def test_gold_from_summary_handles_missing_ticker():
    gold = _gold_from_summary(None)

    assert gold["score"] == 0
    assert gold["tone"] == "neutral"
    assert "not showing up" in gold["one_liner"]


def test_fit_signals_use_source_and_context_language():
    summary = {
        "mentions": 5,
        "file_count": 2,
        "sheet_count": 2,
        "evidence_score": 5.0,
        "source_types": "cell,filename",
    }
    evidence = [
        {
            "evidence_text": "RXT buy target upside",
            "row_context": "portfolio position shares",
        }
    ]

    signals = _fit_signals(summary, evidence)
    labels = {signal["label"] for signal in signals}

    assert "Workbook depth" in labels
    assert "Named at file level" in labels
    assert "Dad-method language" in labels


def test_shorten_collapses_whitespace_and_truncates():
    assert _shorten("a   b\nc", 20) == "a b c"
    assert _shorten("x" * 25, 10) == "xxxxxxxxx..."


def test_parse_finviz_value_handles_suffixes_and_text():
    assert _parse_finviz_value("1.25B") == 1_250_000_000
    assert _parse_finviz_value("14.5%") == 14.5
    assert _parse_finviz_value("-") is None
    assert _parse_finviz_value("Technology") == "Technology"


def test_grid_decision_stack_uses_workbook_grid_and_fundamentals():
    summary = {
        "mentions": 10,
        "file_count": 3,
        "sheet_count": 4,
        "evidence_score": 8.0,
        "source_types": "cell",
    }
    gold = _gold_from_summary(summary)
    grid = {
        "metrics": {
            "return_1y_pct": 32.0,
            "pct_from_52w_high": -4.0,
        },
        "source_freshness": [
            {"source": "yfinance", "state": "fresh"},
            {"source": "finviz_fundamentals", "state": "fresh"},
        ],
    }
    finviz = {
        "status": "ready",
        "field_count": 5,
        "freshness": {"state": "fresh", "label": "fresh"},
        "fields": {
            "forward_pe": {"parsed": 22.0},
            "roe": {"parsed": 19.0},
            "debt_equity": {"parsed": 0.4},
            "profit_margin": {"parsed": 16.0},
        },
    }
    decision = _grid_decision_stack(
        summary,
        gold,
        grid,
        finviz,
        {"date": "2026-06-17", "put_call_ratio": 0.8},
        {"signal_sources": [{"trust_score": 0.7}], "tradingview_signals": [], "regime": None},
    )

    assert decision["score"] >= 45
    assert decision["stance"] in {"Deep review first", "Watchlist with checks"}
    assert any(card["source"] == "Finviz fundamentals" for card in decision["cards"])
