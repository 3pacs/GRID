from __future__ import annotations


class _Result:
    def __init__(self, row=None):
        self._row = row

    def fetchone(self):
        return self._row


class _Conn:
    def __init__(self):
        self.source_row = None
        self.executed = []
        self.resolved_params = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def execute(self, clause, params=None):
        sql = str(clause)
        params = dict(params or {})
        self.executed.append((sql, params))

        if "SELECT id FROM source_catalog" in sql:
            return _Result(self.source_row)
        if "INSERT INTO source_catalog" in sql:
            self.source_row = (321,)
            return _Result(self.source_row)
        if "SELECT id FROM feature_registry" in sql:
            return _Result((676,))
        if "INSERT INTO resolved_series" in sql:
            self.resolved_params.append(params)
            return _Result()
        return _Result()


class _Engine:
    def __init__(self):
        self.conn = _Conn()

    def begin(self):
        return self.conn


def test_price_fallback_records_integer_source_priority():
    from ingestion.price_fallback import PriceFallbackPuller

    engine = _Engine()
    puller = PriceFallbackPuller(db_engine=engine)

    saved = puller.save_to_db([
        {"ticker": "SPY", "price": 501.25, "date": "2026-05-27", "source": "stooq"}
    ])

    assert saved == 1
    assert engine.conn.resolved_params[0]["src"] == 321
    assert isinstance(engine.conn.resolved_params[0]["src"], int)
