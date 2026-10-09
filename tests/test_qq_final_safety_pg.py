"""Actual bounded PG14 trust propagation; only explicitly supplied private fixtures."""
from sqlalchemy import text
from intelligence import trust_scorer as trust
from tests.test_qq_short_transactions_pg import pg as _pg, all_rows

pg = _pg


def test_pg_trust_101_rows_uses_50_50_1_and_preserves_payload_outcomes(pg, monkeypatch):
    engine, counts = pg
    monkeypatch.setattr(trust, "_ensure_tables", lambda eng: None)
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE signal_sources ADD outcome_return float, ADD trust_score float, ADD hit_count integer, ADD miss_count integer, ADD avg_lead_time_hours float"))
    # Setup itself is bounded: 50 + 50 + 1 total DATA modifications.
    for start in range(0, 101, 50):
        with engine.begin() as conn:
            for i in range(start, min(start + 50, 101)):
                conn.execute(text("""INSERT INTO signal_sources(source_type,source_id,ticker,signal_type,signal_date,signal_value,outcome,outcome_return)
                    VALUES ('quiverquant:house',:sid,'SYNTHETIC','house_trading',CURRENT_DATE,'{}',:outcome,.01)"""),
                    {"sid": 'qq_house_trading:' + str(i), "outcome": 'CORRECT' if i % 2 == 0 else 'WRONG'})
    assert [n for n in counts if n] == [50, 50, 1]
    before = all_rows(engine)
    counts.clear()
    result = trust.update_trust_scores(engine)
    assert [n for n in counts if n] == [50, 50, 1]
    after = all_rows(engine)
    modified = {"trust_score", "hit_count", "miss_count", "avg_lead_time_hours"}
    for old, new in zip(before, after):
        assert {k:v for k,v in old.items() if k not in modified} == {k:v for k,v in new.items() if k not in modified}
        assert (new['hit_count'],new['miss_count']) == (51,50)
    assert result['sources'][0]['propagated_rows'] == 101
    assert result['total'] == 1 and len({r['trust_score'] for r in after}) == 1
