"""One-time script: generate antitheses for all existing active hypotheses."""
import json
import sys
sys.path.insert(0, ".")

from sqlalchemy import text
from db import get_engine
from intelligence.hypothesis_engine import HypothesisGenerator, Hypothesis

engine = get_engine()
gen = HypothesisGenerator(engine)

with engine.connect() as conn:
    rows = conn.execute(text(
        "SELECT id, thesis, pattern_type, evidence, test_criteria, invalidation, confidence "
        "FROM discovered_hypotheses WHERE role = 'thesis' AND status = 'active'"
    )).fetchall()

print(f"Found {len(rows)} active theses to generate antitheses for")

count = 0
for r in rows:
    hyp = Hypothesis(
        id=r[0], thesis=r[1], pattern_type=r[2],
        evidence=json.loads(r[3]) if isinstance(r[3], str) else (r[3] or []),
        test_criteria=json.loads(r[4]) if isinstance(r[4], str) else (r[4] or {}),
        invalidation=r[5] or "", confidence=float(r[6] or 0.5),
    )
    anti = gen._make_antithesis(hyp)
    if anti:
        anti._role = "antithesis"
        anti._pair_id = hyp.id
        upsert = text("""
            INSERT INTO discovered_hypotheses
                (id, thesis, pattern_type, evidence, test_criteria,
                 invalidation, confidence, status, role, pair_id)
            VALUES (:id, :thesis, :ptype, :evidence, :criteria,
                    :inv, :conf, :status, :role, :pair_id)
            ON CONFLICT (id) DO NOTHING
        """)
        with engine.begin() as conn2:
            res = conn2.execute(upsert, {
                "id": anti.id, "thesis": anti.thesis,
                "ptype": anti.pattern_type,
                "evidence": json.dumps(anti.evidence),
                "criteria": json.dumps(anti.test_criteria),
                "inv": anti.invalidation,
                "conf": float(anti.confidence),
                "status": "active",
                "role": "antithesis",
                "pair_id": hyp.id,
            })
            if res.rowcount > 0:
                count += 1

print(f"Generated {count} antitheses")
