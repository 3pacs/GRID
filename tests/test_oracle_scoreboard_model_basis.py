"""Per-model scoreboard rows share the overall line's basis (2026-11-07).

oracle_models' running counters (every raw prediction, duplicates and
pre-rescore labels) showed options_flow at 55,964 predictions / +14,776 P&L
next to an overall 24,387 / -558. Counts and P&L now come from
oracle_predictions WHERE dedup_keep, like the overall line.
"""

from __future__ import annotations

from sqlalchemy import create_engine, text

from oracle import scoreboard as sb


def _engine():
    eng = create_engine("sqlite://")
    with eng.begin() as c:
        c.execute(text("CREATE TABLE oracle_models (name TEXT, weight REAL, predictions_made INT, hits INT, "
                       "misses INT, partials INT, cumulative_pnl REAL, sharpe REAL, description TEXT)"))
        c.execute(text("CREATE TABLE oracle_predictions (id TEXT, model_name TEXT, ticker TEXT, verdict TEXT, "
                       "pnl_pct REAL, dedup_keep BOOLEAN)"))
        # inflated legacy counters that must not reach the scoreboard
        c.execute(text("INSERT INTO oracle_models VALUES ('options_flow', 1.4, 55964, 15624, 27826, 12514, 14776.11, 0, 'flow'),"
                       " ('idle', 1.0, 9, 9, 0, 0, 99, 0, 'never predicted')"))
        rows = [
            ("a", "options_flow", "AAPL", "hit", 2.0, True),
            ("b", "options_flow", "AAPL", "miss", -1.5, True),
            ("c", "options_flow", "MSFT", "partial", 0.5, True),
            ("d", "options_flow", "MSFT", "pending", None, True),
            ("e", "options_flow", "MSFT", "hit", 9.0, False),   # duplicate, not kept
            ("f", "options_flow", "TSLA", "void", None, True),   # voided, not scored
        ]
        for r in rows:
            c.execute(text("INSERT INTO oracle_predictions VALUES (:i, :m, :t, :v, :p, :k)"),
                      dict(zip("imtvpk", r)))
    return eng


def test_model_rows_use_kept_predictions_not_legacy_counters() -> None:
    models = {m["name"]: m for m in sb._fetch_model_rows(_engine())}
    of = models["options_flow"]
    assert of["predictions_made"] == 5           # a, b, c, d, f (e is not kept)
    assert (of["hits"], of["misses"], of["partials"], of["pending"]) == (1, 1, 1, 1)
    assert of["total_scored"] == 3 and of["cumulative_pnl"] == 1.0
    assert of["weight"] == 1.4 and of["description"] == "flow"
    assert of["basis"] == "oracle_predictions dedup_keep"


def test_registered_model_without_predictions_reads_zero() -> None:
    idle = {m["name"]: m for m in sb._fetch_model_rows(_engine())}["idle"]
    assert idle["predictions_made"] == 0 and idle["total_scored"] == 0 and idle["cumulative_pnl"] == 0


def test_model_rows_add_up_to_the_overall_line() -> None:
    eng = _engine()
    overall = sb._fetch_overall_stats(eng)
    models = sb._fetch_model_rows(eng)
    assert sum(m["total_scored"] for m in models) == overall["scored"]
    assert round(sum(m["cumulative_pnl"] for m in models), 2) == overall["total_pnl"]
