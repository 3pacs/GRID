"""Per-model scoreboard rows share the overall line's basis (2026-11-07).

oracle_models' running counters (every raw prediction, duplicates and
pre-rescore labels) showed options_flow at 55,964 predictions / +14,776 P&L
next to an overall 24,387 / -558. Counts and P&L now come from
oracle_predictions WHERE dedup_keep, like the overall line.
"""

from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy import create_engine, text

from oracle import scoreboard as sb


def _engine():
    eng = create_engine("sqlite://")
    with eng.begin() as c:
        c.execute(text("CREATE TABLE oracle_models (name TEXT, weight REAL, predictions_made INT, hits INT, "
                       "misses INT, partials INT, cumulative_pnl REAL, sharpe REAL, description TEXT)"))
        c.execute(text("CREATE TABLE oracle_predictions (id TEXT, model_name TEXT, ticker TEXT, verdict TEXT, "
                       "pnl_pct REAL, dedup_keep BOOLEAN, expiry DATE, recording_policy TEXT)"))
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
            c.execute(text("INSERT INTO oracle_predictions (id, model_name, ticker, verdict, pnl_pct, dedup_keep) "
                           "VALUES (:i, :m, :t, :v, :p, :k)"),
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


def test_lifecycle_partitions_all_kept_rows_including_unknown_statuses() -> None:
    eng = _engine()
    today = date.today()
    with eng.begin() as c:
        c.execute(text("DELETE FROM oracle_predictions"))
        rows = [
            ("future", "pending", today + timedelta(days=1), None, True),
            ("due", "pending", today, None, True),
            ("overdue", "pending", today - timedelta(days=1), None, True),
            ("missing-price", "no_data", today, None, True),
            ("win", "hit", today, 10.0, True),
            ("loss", "miss", today, -2.0, True),
            ("part", "partial", today, 1.0, True),
            ("legacy-unknown", "void", today, None, True),
            ("null-verdict", None, today, None, True),
            ("missing-expiry", "pending", None, None, True),
            ("duplicate", "hit", today, 100.0, False),
        ]
        c.execute(text("INSERT INTO oracle_predictions (id, verdict, expiry, pnl_pct, dedup_keep) "
                       "VALUES (:id, :verdict, :expiry, :pnl, :keep)"),
                  [dict(zip(("id", "verdict", "expiry", "pnl", "keep"), row)) for row in rows])
    result = sb._fetch_overall_stats(eng)
    assert {k: result[k] for k in ("active", "awaiting_score", "unscorable", "scored", "other")} == {
        "active": 1, "awaiting_score": 2, "unscorable": 1, "scored": 3, "other": 3,
    }
    assert sum(result[k] for k in ("active", "awaiting_score", "unscorable", "scored", "other")) == result["total_predictions"] == 10
    assert result["pending"] == 4
    assert result["mean_signed_return_pct"] == 3.0
    assert result["accuracy"] == 0.5
    assert result["lifecycle_as_of"] == today.isoformat()


def test_mean_return_is_unavailable_without_measured_outcomes() -> None:
    eng = _engine()
    with eng.begin() as c:
        c.execute(text("DELETE FROM oracle_predictions"))
        c.execute(text("INSERT INTO oracle_predictions (id, verdict, dedup_keep) VALUES ('p', 'no_data', TRUE)"))
    result = sb._fetch_overall_stats(eng)
    assert result["mean_signed_return_pct"] is None
    assert result["unscorable"] == result["total_predictions"] == 1


def test_recording_cohorts_do_not_turn_legacy_wins_into_prospective_results():
    eng = _engine()
    with eng.begin() as conn:
        conn.execute(text("""INSERT INTO oracle_predictions
            (id, verdict, dedup_keep, recording_policy) VALUES
            ('new', 'pending', TRUE, 'oracle_forward_v2'),
            ('abstain', 'no_data', TRUE, 'oracle_forward_v2'),
            ('external', 'pending', TRUE, 'oracle_external_v2')"""))
    cohorts = sb._fetch_recording_cohorts(eng)
    assert cohorts["oracle_forward_v2"] == {
        "total": 2, "pending": 1, "unscorable": 1, "scored": 0, "other": 0,
        "mean_signed_return_pct": None,
    }
    assert cohorts["legacy"]["scored"] == 3
    assert cohorts["oracle_external_v2"]["total"] == 1
