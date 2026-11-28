"""Hit rates divide by graded verdicts only: abstentions are not misses."""
from __future__ import annotations

from sqlalchemy import create_engine, text

from oracle.trace_evolver import MIN_SCORED_PREDICTIONS, TargetedMutator


def test_top_performer_ignores_no_data_pending_and_soft_duplicate_rows() -> None:
    db = create_engine("sqlite://")
    n = MIN_SCORED_PREDICTIONS
    with db.begin() as conn:
        conn.execute(text("CREATE TABLE oracle_predictions "
                          "(id TEXT, model_name TEXT, verdict TEXT, dedup_keep BOOLEAN)"))
        rows = (
            # a: 60 % of graded verdicts, plus many abstentions
            [("a", "hit")] * (6 * n // 10) + [("a", "miss")] * (4 * n // 10) + [("a", "no_data")] * (5 * n)
            # b: 50 %, no abstentions
            + [("b", "hit")] * (n // 2) + [("b", "miss")] * (n // 2) + [("b", "pending")] * n
        )
        for i, (model, verdict) in enumerate(rows):
            conn.execute(text("INSERT INTO oracle_predictions VALUES (:i, :m, :v, TRUE)"),
                         {"i": f"p{i}", "m": model, "v": verdict})
        # soft duplicates of b's hits do not count either
        for i in range(5 * n):
            conn.execute(text("INSERT INTO oracle_predictions VALUES (:i, 'b', 'hit', FALSE)"),
                         {"i": f"dup{i}"})
    mutator = object.__new__(TargetedMutator)
    mutator.engine = db

    assert mutator._get_top_performer() == "a"
