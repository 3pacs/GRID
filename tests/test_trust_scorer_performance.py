"""Performance guards for trust scoring aggregation."""

from __future__ import annotations

from datetime import date


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self):
        self.statements: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        if "SELECT DISTINCT source_type, source_id" in sql:
            return _Rows([("news", "a"), ("news", "b")])
        if sql.lstrip().upper().startswith("SELECT") and "WHERE source_type = :st AND source_id = :si" in sql:
            raise AssertionError("trust scorer made one query per source")
        if "SELECT source_type, source_id, outcome" in sql:
            return _Rows([
                ("news", "a", "CORRECT", 0.02, date.today(), "NVDA"),
                ("news", "b", "WRONG", -0.01, date.today(), "MSFT"),
            ])
        return _Rows([])


class _Engine:
    def __init__(self):
        self.conn = _Conn()

    def connect(self):
        return self.conn

    def begin(self):
        return self.conn


def test_update_trust_scores_batches_scored_signal_rows(monkeypatch):
    from intelligence import trust_scorer

    monkeypatch.setattr(trust_scorer, "_ensure_tables", lambda engine: None)

    engine = _Engine()
    result = trust_scorer.update_trust_scores(engine)

    assert result["total"] == 2
    assert sum("UPDATE signal_sources" in sql for sql in engine.conn.statements) == 2
