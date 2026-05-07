# Hypothesis Kill System + Antithesis Tracking — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-[[development]] (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the blanket 7-day scoring gate with per-hypothesis test windows, add named kill conditions that auto-invalidate dead theses with [[Postmortem|postmortem]] records, and generate thesis/antithesis pairs for every hypothesis so both sides of every bet are tracked.

**[[architecture|Architecture]]:** Extend `discovered_hypotheses` with 4 new columns (`role`, `pair_id`, `kill_reason`, `killed_at`). New `hypothesis_postmortems` table stores the full death record. Kill logic lives in a new `_check_kills()` method that runs typed kill checks per `pattern_type`. Antithesis generation happens inline during `_store_hypothesis()` — for every thesis stored, its inverse is auto-generated and linked via `pair_id`. When either side confirms, the other auto-kills with `ANTITHESIS_CONFIRMED`.

**Tech Stack:** Python 3.11, [[SQLAlchemy]] 2.0 (raw `text()` queries), [[PostgreSQL]], pytest

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `intelligence/hypothesis_engine.py` | Modify | Schema migration, kill taxonomy, antithesis generation, per-hypothesis scoring, kill checks, postmortem writes |
| `tests/test_hypothesis_kills.py` | Create | All tests for kill logic, antithesis generation, postmortem creation, per-window scoring |

---

### Task 1: Schema Migration — Add columns + postmortem table

**Files:**
- Modify: `intelligence/hypothesis_engine.py:94-135` (schema section)

- [ ] **Step 1: Write failing test for new schema**

```python
# tests/test_hypothesis_kills.py
"""Tests for hypothesis kill system, antithesis tracking, and postmortems."""

import json
import pytest
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from intelligence.hypothesis_engine import (
    ensure_tables,
    HypothesisGenerator,
    Hypothesis,
    KILL_REASONS,
)


@pytest.fixture
def engine():
    """Create a test engine with hypothesis tables."""
    eng = create_engine("postgresql://grid_user:grid@localhost:5432/grid_intelligence")
    ensure_tables(eng)
    yield eng


def test_schema_has_new_columns(engine):
    """New columns exist on discovered_hypotheses."""
    with engine.connect() as conn:
        cols = {
            row[0]
            for row in conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'discovered_hypotheses'"
            ))
        }
    assert "role" in cols
    assert "pair_id" in cols
    assert "kill_reason" in cols
    assert "killed_at" in cols


def test_postmortem_table_exists(engine):
    """hypothesis_postmortems table exists with expected columns."""
    with engine.connect() as conn:
        cols = {
            row[0]
            for row in conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'hypothesis_postmortems'"
            ))
        }
    expected = {
        "id", "hypothesis_id", "kill_reason", "evidence",
        "thesis_text", "antithesis_text", "confidence_at_death",
        "times_tested", "times_correct", "lifespan_days", "created_at",
    }
    assert expected.issubset(cols)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_hypothesis_kills.py::test_schema_has_new_columns tests/test_hypothesis_kills.py::test_postmortem_table_exists -v`
Expected: FAIL — columns don't exist yet

- [ ] **Step 3: Add kill taxonomy constants and schema DDL**

In `intelligence/hypothesis_engine.py`, after the existing constants (line ~47), add:

```python
# ── Kill Taxonomy ────────────────────────────────────────────────────────────

KILL_REASONS = {
    # Universal kills (all hypothesis types)
    "ANTITHESIS_CONFIRMED":   "The inverse hypothesis was confirmed",
    "CONFIDENCE_COLLAPSED":   "Bayesian confidence dropped below 0.10 after 3+ tests",
    "EXPIRED":                "Exceeded 2x test window with no resolution",
    # lead_lag
    "PATTERN_BROKEN":         "Signal A fired 3+ times, signal B never followed",
    "CORRELATION_COLLAPSED":  "Re-scan shows correlation lost significance (p > 0.05)",
    # convergence
    "WRONG_DIRECTION":        "Ticker moved opposite to predicted direction",
    "NO_MOVE":                "Window expired, ticker stayed flat",
    # volume_anomaly
    "NO_FOLLOW_THROUGH":      "No follow-on activity or price impact in window",
    "FALSE_SPIKE":            "Volume normalized with zero impact",
    # actor_shift
    "ACTOR_RETREATED":        "No further signals in new category within window",
    "NO_CATALYST":            "No related market event within window",
}

CONFIDENCE_KILL_THRESHOLD = 0.10  # Below this after 3+ tests → dead
MIN_TESTS_FOR_CONFIDENCE_KILL = 3
```

Replace the existing `_SCHEMA_SQL` block (lines 94-125) with:

```python
_SCHEMA_SQL = text("""
    CREATE TABLE IF NOT EXISTS discovered_hypotheses (
        id               TEXT PRIMARY KEY,
        thesis           TEXT NOT NULL,
        pattern_type     TEXT,
        evidence         JSONB,
        test_criteria    JSONB,
        invalidation     TEXT,
        confidence       DOUBLE PRECISION DEFAULT 0.5,
        status           TEXT DEFAULT 'active',
        times_tested     INTEGER DEFAULT 0,
        times_correct    INTEGER DEFAULT 0,
        created_at       TIMESTAMPTZ DEFAULT NOW(),
        last_tested      TIMESTAMPTZ,
        role             TEXT DEFAULT 'thesis',
        pair_id          TEXT,
        kill_reason      TEXT,
        killed_at        TIMESTAMPTZ
    )
""")
```

Add the new columns as safe ALTER statements in `ensure_tables()` so existing rows aren't lost:

```python
_ADD_ROLE = text("ALTER TABLE discovered_hypotheses ADD COLUMN IF NOT EXISTS role TEXT DEFAULT 'thesis'")
_ADD_PAIR_ID = text("ALTER TABLE discovered_hypotheses ADD COLUMN IF NOT EXISTS pair_id TEXT")
_ADD_KILL_REASON = text("ALTER TABLE discovered_hypotheses ADD COLUMN IF NOT EXISTS kill_reason TEXT")
_ADD_KILLED_AT = text("ALTER TABLE discovered_hypotheses ADD COLUMN IF NOT EXISTS killed_at TIMESTAMPTZ")

_IDX_PAIR = text("CREATE INDEX IF NOT EXISTS idx_discovered_hypotheses_pair ON discovered_hypotheses (pair_id)")
_IDX_ROLE = text("CREATE INDEX IF NOT EXISTS idx_discovered_hypotheses_role ON discovered_hypotheses (role)")

_POSTMORTEM_SCHEMA = text("""
    CREATE TABLE IF NOT EXISTS hypothesis_postmortems (
        id               SERIAL PRIMARY KEY,
        hypothesis_id    TEXT NOT NULL,
        kill_reason      TEXT NOT NULL,
        evidence         JSONB,
        thesis_text      TEXT,
        antithesis_text  TEXT,
        confidence_at_death DOUBLE PRECISION,
        times_tested     INTEGER DEFAULT 0,
        times_correct    INTEGER DEFAULT 0,
        lifespan_days    INTEGER,
        created_at       TIMESTAMPTZ DEFAULT NOW()
    )
""")
_IDX_PM_HYP = text("CREATE INDEX IF NOT EXISTS idx_hyp_postmortem_hypothesis ON hypothesis_postmortems (hypothesis_id)")
_IDX_PM_KILL = text("CREATE INDEX IF NOT EXISTS idx_hyp_postmortem_kill ON hypothesis_postmortems (kill_reason)")
```

Update `ensure_tables()` to run all of these:

```python
def ensure_tables(engine: Engine) -> None:
    """Create/migrate the discovered_hypotheses and hypothesis_postmortems tables."""
    with engine.begin() as conn:
        conn.execute(_SCHEMA_SQL)
        # Migrate existing tables
        conn.execute(_ADD_ROLE)
        conn.execute(_ADD_PAIR_ID)
        conn.execute(_ADD_KILL_REASON)
        conn.execute(_ADD_KILLED_AT)
        # Indexes
        conn.execute(_IDX_STATUS)
        conn.execute(_IDX_CONFIDENCE)
        conn.execute(_IDX_CREATED)
        conn.execute(_IDX_PAIR)
        conn.execute(_IDX_ROLE)
        # Postmortem table
        conn.execute(_POSTMORTEM_SCHEMA)
        conn.execute(_IDX_PM_HYP)
        conn.execute(_IDX_PM_KILL)
    log.info("hypothesis_engine: tables ensured")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_hypothesis_kills.py::test_schema_has_new_columns tests/test_hypothesis_kills.py::test_postmortem_table_exists -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/anikdang/dev/GRID && git add intelligence/hypothesis_engine.py tests/test_hypothesis_kills.py
git commit -m "feat: add kill taxonomy, role/pair_id columns, hypothesis_postmortems table"
```

---

### Task 2: Antithesis Generation

For every thesis stored, auto-generate and store its inverse, linked via `pair_id`.

**Files:**
- Modify: `intelligence/hypothesis_engine.py` — `_store_hypothesis()` (~line 1154), new `_make_antithesis()` method
- Modify: `tests/test_hypothesis_kills.py` — add antithesis tests

- [ ] **Step 1: Write failing test for antithesis generation**

Add to `tests/test_hypothesis_kills.py`:

```python
def test_antithesis_generated_for_lead_lag(engine):
    """Storing a lead_lag thesis auto-creates its antithesis."""
    gen = HypothesisGenerator(engine)
    hyp = Hypothesis(
        id="hyp_test_ll_thesis",
        thesis="When sig:A spikes, sig:B increases within 5 days",
        pattern_type="lead_lag",
        evidence=[{"signal_a": "sig:A", "signal_b": "sig:B", "lag_days": 5}],
        test_criteria={
            "watch_signal": "sig:A",
            "expect_signal": "sig:B",
            "lag_days": 5,
            "expected_direction": "increases",
        },
        invalidation="If sig:A spikes 3+ times and sig:B does NOT increase",
        confidence=0.65,
    )
    # Clean up from prior runs
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM discovered_hypotheses WHERE id IN (:t, :a)"),
                     {"t": "hyp_test_ll_thesis", "a": "hyp_test_ll_thesis_anti"})

    gen._store_hypothesis(hyp)

    with engine.connect() as conn:
        thesis_row = conn.execute(text(
            "SELECT role, pair_id FROM discovered_hypotheses WHERE id = :id"
        ), {"id": "hyp_test_ll_thesis"}).fetchone()
        anti_row = conn.execute(text(
            "SELECT id, role, pair_id, thesis, pattern_type, test_criteria "
            "FROM discovered_hypotheses WHERE pair_id = :pid AND role = 'antithesis'"
        ), {"pid": "hyp_test_ll_thesis"}).fetchone()

    assert thesis_row is not None
    assert thesis_row[0] == "thesis"
    assert thesis_row[1] == "hyp_test_ll_thesis"  # pair_id = own id for thesis

    assert anti_row is not None
    assert anti_row[1] == "antithesis"
    assert anti_row[2] == "hyp_test_ll_thesis"  # pair_id links to thesis
    assert "does NOT" in anti_row[3] or "fails to" in anti_row[3]


def test_antithesis_generated_for_convergence(engine):
    """Storing a convergence thesis auto-creates its antithesis."""
    gen = HypothesisGenerator(engine)
    hyp = Hypothesis(
        id="hyp_test_conv_thesis",
        thesis="CONVERGENCE: 4 sources agree AAPL is heading bullish",
        pattern_type="convergence",
        evidence=[{"ticker": "AAPL", "direction": "bullish", "n_sources": 4}],
        test_criteria={
            "ticker": "AAPL",
            "expected_direction": "bullish",
            "window_days": 14,
            "min_move_pct": 2.0,
        },
        invalidation="If AAPL moves opposite to bullish by >2% within 14 days",
        confidence=0.72,
    )
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM discovered_hypotheses WHERE pair_id = :pid"),
                     {"pid": "hyp_test_conv_thesis"})
        conn.execute(text("DELETE FROM discovered_hypotheses WHERE id = :id"),
                     {"id": "hyp_test_conv_thesis"})

    gen._store_hypothesis(hyp)

    with engine.connect() as conn:
        anti = conn.execute(text(
            "SELECT thesis, test_criteria FROM discovered_hypotheses "
            "WHERE pair_id = :pid AND role = 'antithesis'"
        ), {"pid": "hyp_test_conv_thesis"}).fetchone()

    assert anti is not None
    anti_criteria = json.loads(anti[1]) if isinstance(anti[1], str) else anti[1]
    # Antithesis expects the OPPOSITE direction
    assert anti_criteria["expected_direction"] in ("bearish", "down")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_hypothesis_kills.py::test_antithesis_generated_for_lead_lag tests/test_hypothesis_kills.py::test_antithesis_generated_for_convergence -v`
Expected: FAIL — no antithesis rows created

- [ ] **Step 3: Implement `_make_antithesis()` and update `_store_hypothesis()`**

Add new method to `HypothesisGenerator` class (after `_make_id`, around line 1187):

```python
    def _make_antithesis(self, hyp: Hypothesis) -> Hypothesis | None:
        """Generate the inverse hypothesis for a given thesis."""
        anti_id = hyp.id + "_anti"
        ptype = hyp.pattern_type
        criteria = dict(hyp.test_criteria)

        if ptype == "lead_lag":
            direction = criteria.get("expected_direction", "increases")
            anti_dir = "decreases" if direction == "increases" else "increases"
            sig_a = criteria.get("watch_signal", "?")
            sig_b = criteria.get("expect_signal", "?")
            lag = criteria.get("lag_days", 7)
            anti_thesis = (
                f"ANTITHESIS: When {sig_a} spikes, {sig_b} does NOT {direction} "
                f"within {lag} days — the lead-lag pattern is noise"
            )
            criteria["expected_direction"] = anti_dir

        elif ptype == "convergence":
            ticker = criteria.get("ticker", "?")
            direction = criteria.get("expected_direction", "unknown")
            flip = {"bullish": "bearish", "bearish": "bullish",
                    "up": "down", "down": "up"}.get(direction, "opposite")
            anti_thesis = (
                f"ANTITHESIS: Despite source convergence, {ticker} moves {flip} "
                f"— convergence signal was misleading"
            )
            criteria["expected_direction"] = flip

        elif ptype == "volume_anomaly":
            cat = criteria.get("watch_category", "?")
            anti_thesis = (
                f"ANTITHESIS: {cat} volume spike was noise — no follow-on "
                f"activity or price impact materialised"
            )
            # Antithesis confirms if NO follow-on occurs
            criteria["expect_no_activity"] = True

        elif ptype == "actor_shift":
            actor = criteria.get("watch_actor", "?")
            anti_thesis = (
                f"ANTITHESIS: '{actor}' new domain appearance was transient "
                f"noise — no persistence or market impact"
            )
            criteria["expect_no_activity"] = True

        else:
            return None

        return Hypothesis(
            id=anti_id,
            thesis=anti_thesis,
            pattern_type=ptype,
            evidence=hyp.evidence,
            test_criteria=criteria,
            invalidation=f"Original thesis confirmed: {hyp.thesis[:120]}",
            confidence=1.0 - hyp.confidence,
            status="active",
        )
```

Update `_store_hypothesis()` to set `role`/`pair_id` on thesis and auto-create antithesis:

```python
    def _store_hypothesis(self, hyp: Hypothesis) -> bool:
        """Upsert a hypothesis into discovered_hypotheses. Returns True if inserted."""
        import re

        def _clean(s: str) -> str:
            return re.sub(r"np\.float64\(([^)]+)\)", r"\1", s) if s else s

        role = getattr(hyp, "_role", "thesis")
        pair_id = getattr(hyp, "_pair_id", hyp.id if role == "thesis" else None)

        upsert = text("""
            INSERT INTO discovered_hypotheses
                (id, thesis, pattern_type, evidence, test_criteria,
                 invalidation, confidence, status, role, pair_id)
            VALUES
                (:id, :thesis, :ptype, :evidence, :criteria,
                 :inv, :conf, :status, :role, :pair_id)
            ON CONFLICT (id) DO NOTHING
        """)
        with self.engine.begin() as conn:
            result = conn.execute(upsert, {
                "id": hyp.id,
                "thesis": _clean(hyp.thesis),
                "ptype": hyp.pattern_type,
                "evidence": json.dumps(hyp.evidence),
                "criteria": json.dumps(hyp.test_criteria),
                "inv": _clean(hyp.invalidation),
                "conf": float(hyp.confidence),
                "status": hyp.status,
                "role": role,
                "pair_id": pair_id,
            })
        inserted = result.rowcount > 0

        # Auto-generate antithesis for new theses
        if inserted and role == "thesis":
            anti = self._make_antithesis(hyp)
            if anti:
                anti._role = "antithesis"
                anti._pair_id = hyp.id
                self._store_hypothesis(anti)

        return inserted
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_hypothesis_kills.py::test_antithesis_generated_for_lead_lag tests/test_hypothesis_kills.py::test_antithesis_generated_for_convergence -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/anikdang/dev/GRID && git add intelligence/hypothesis_engine.py tests/test_hypothesis_kills.py
git commit -m "feat: auto-generate antithesis for every hypothesis, linked via pair_id"
```

---

### Task 3: Per-Hypothesis Scoring Windows

Replace the blanket `INTERVAL '7 days'` in `score_all()` with per-hypothesis windows derived from each hypothesis's `test_criteria`.

**Files:**
- Modify: `intelligence/hypothesis_engine.py` — `score_all()` (line 791)
- Modify: `tests/test_hypothesis_kills.py`

- [ ] **Step 1: Write failing test**

Add to `tests/test_hypothesis_kills.py`:

```python
def test_score_all_uses_per_hypothesis_window(engine):
    """score_all picks up hypotheses whose own test window has elapsed."""
    gen = HypothesisGenerator(engine)

    # Hypothesis with 3-day window, created 5 days ago → should be scoreable
    short_window = Hypothesis(
        id="hyp_test_short_window",
        thesis="Short window test hypothesis",
        pattern_type="lead_lag",
        evidence=[],
        test_criteria={"watch_signal": "x", "expect_signal": "y",
                       "lag_days": 3, "expected_direction": "increases"},
        invalidation="test",
        confidence=0.5,
    )
    # Hypothesis with 30-day window, created 5 days ago → should NOT be scoreable
    long_window = Hypothesis(
        id="hyp_test_long_window",
        thesis="Long window test hypothesis",
        pattern_type="volume_anomaly",
        evidence=[],
        test_criteria={"watch_category": "test_cat", "window_days": 30},
        invalidation="test",
        confidence=0.5,
    )

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM discovered_hypotheses WHERE id LIKE 'hyp_test_%window%'"))

    gen._store_hypothesis(short_window)
    gen._store_hypothesis(long_window)

    # Backdate both to 5 days ago
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE discovered_hypotheses SET created_at = NOW() - INTERVAL '5 days' "
            "WHERE id LIKE 'hyp_test_%_window'"
        ))

    results = gen.score_all()
    scored_ids = {r["id"] for r in results if "id" in r}

    assert "hyp_test_short_window" in scored_ids, "3-day window hypothesis should be scored after 5 days"
    assert "hyp_test_long_window" not in scored_ids, "30-day window hypothesis should NOT be scored after 5 days"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_hypothesis_kills.py::test_score_all_uses_per_hypothesis_window -v`
Expected: FAIL — blanket 7-day gate scores neither (both created 5 days ago)

- [ ] **Step 3: Implement per-hypothesis window scoring**

Replace `score_all()` (lines 791-811) with:

```python
    def score_all(self) -> list[dict]:
        """Score all active theses and antitheses whose test window has elapsed."""
        q = text("""
            SELECT id, test_criteria, created_at
            FROM discovered_hypotheses
            WHERE status = 'active'
            ORDER BY created_at
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(q).fetchall()

        results = []
        now = datetime.now(timezone.utc)
        for row in rows:
            h_id = row[0]
            criteria = row[1] if isinstance(row[1], dict) else json.loads(row[1] or "{}")
            created = row[2]

            # Derive test window from criteria
            window_days = criteria.get("window_days") or criteria.get("lag_days") or 7
            if created + timedelta(days=window_days) > now:
                continue  # Not ready yet

            result = self.score_hypothesis(h_id)
            results.append(result)

        log.info("hypothesis_engine: scored {} hypotheses", len(results))
        return results
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_hypothesis_kills.py::test_score_all_uses_per_hypothesis_window -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/anikdang/dev/GRID && git add intelligence/hypothesis_engine.py tests/test_hypothesis_kills.py
git commit -m "feat: score_all uses per-hypothesis test windows instead of blanket 7 days"
```

---

### Task 4: Kill Checks + Postmortem Writes

The core logic: after scoring, check if any kill condition fires. If it does, mark the hypothesis as `invalidated`, record `kill_reason` and `killed_at`, and write a postmortem.

**Files:**
- Modify: `intelligence/hypothesis_engine.py` — new `_check_kills()`, `_kill_hypothesis()`, `_write_postmortem()` methods; update `score_hypothesis()`
- Modify: `tests/test_hypothesis_kills.py`

- [ ] **Step 1: Write failing tests for kill conditions**

Add to `tests/test_hypothesis_kills.py`:

```python
def test_expired_kill(engine):
    """Hypothesis past 2x window with no resolution gets EXPIRED kill."""
    gen = HypothesisGenerator(engine)
    hyp = Hypothesis(
        id="hyp_test_expired",
        thesis="This should expire",
        pattern_type="convergence",
        evidence=[{"ticker": "TEST"}],
        test_criteria={"ticker": "TEST", "expected_direction": "bullish",
                       "window_days": 10, "min_move_pct": 2.0},
        invalidation="test",
        confidence=0.5,
    )
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM discovered_hypotheses WHERE id = 'hyp_test_expired'"))
        conn.execute(text("DELETE FROM discovered_hypotheses WHERE pair_id = 'hyp_test_expired'"))
        conn.execute(text("DELETE FROM hypothesis_postmortems WHERE hypothesis_id = 'hyp_test_expired'"))

    gen._store_hypothesis(hyp)

    # Backdate to 25 days ago (2x 10-day window = 20, so 25 > 20)
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE discovered_hypotheses SET created_at = NOW() - INTERVAL '25 days' "
            "WHERE id = 'hyp_test_expired'"
        ))

    gen.score_hypothesis("hyp_test_expired")

    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT status, kill_reason, killed_at FROM discovered_hypotheses WHERE id = 'hyp_test_expired'"
        )).fetchone()
        pm = conn.execute(text(
            "SELECT kill_reason, lifespan_days FROM hypothesis_postmortems "
            "WHERE hypothesis_id = 'hyp_test_expired'"
        )).fetchone()

    assert row[0] == "invalidated"
    assert row[1] == "EXPIRED"
    assert row[2] is not None
    assert pm is not None
    assert pm[0] == "EXPIRED"
    assert pm[1] >= 25


def test_confidence_collapsed_kill(engine):
    """Hypothesis with confidence < 0.10 after 3+ tests gets killed."""
    gen = HypothesisGenerator(engine)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM discovered_hypotheses WHERE id = 'hyp_test_conf_kill'"))
        conn.execute(text("DELETE FROM hypothesis_postmortems WHERE hypothesis_id = 'hyp_test_conf_kill'"))
        conn.execute(text("""
            INSERT INTO discovered_hypotheses
                (id, thesis, pattern_type, evidence, test_criteria,
                 invalidation, confidence, status, times_tested, times_correct, role, pair_id)
            VALUES
                ('hyp_test_conf_kill', 'Low confidence test', 'lead_lag',
                 '[]', '{"watch_signal":"x","expect_signal":"y","lag_days":3,"expected_direction":"increases"}',
                 'test', 0.05, 'active', 4, 0, 'thesis', 'hyp_test_conf_kill')
        """))

    result = gen.score_hypothesis("hyp_test_conf_kill")

    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT status, kill_reason FROM discovered_hypotheses WHERE id = 'hyp_test_conf_kill'"
        )).fetchone()

    assert row[0] == "invalidated"
    assert row[1] == "CONFIDENCE_COLLAPSED"


def test_antithesis_confirmed_kills_thesis(engine):
    """When antithesis is confirmed, parent thesis gets ANTITHESIS_CONFIRMED kill."""
    gen = HypothesisGenerator(engine)
    thesis_id = "hyp_test_anti_kill_parent"
    anti_id = thesis_id + "_anti"

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM discovered_hypotheses WHERE id IN (:t, :a)"),
                     {"t": thesis_id, "a": anti_id})
        conn.execute(text("DELETE FROM hypothesis_postmortems WHERE hypothesis_id IN (:t, :a)"),
                     {"t": thesis_id, "a": anti_id})
        # Insert thesis
        conn.execute(text("""
            INSERT INTO discovered_hypotheses
                (id, thesis, pattern_type, evidence, test_criteria,
                 invalidation, confidence, status, role, pair_id)
            VALUES
                (:tid, 'Parent thesis', 'convergence', '[]',
                 '{"ticker":"TEST","expected_direction":"bullish","window_days":14,"min_move_pct":2.0}',
                 'test', 0.7, 'active', 'thesis', :tid)
        """), {"tid": thesis_id})
        # Insert antithesis — already confirmed
        conn.execute(text("""
            INSERT INTO discovered_hypotheses
                (id, thesis, pattern_type, evidence, test_criteria,
                 invalidation, confidence, status, role, pair_id)
            VALUES
                (:aid, 'Antithesis', 'convergence', '[]',
                 '{"ticker":"TEST","expected_direction":"bearish","window_days":14,"min_move_pct":2.0}',
                 'test', 0.8, 'confirmed', 'antithesis', :tid)
        """), {"aid": anti_id, "tid": thesis_id})

    # Score the thesis — should detect confirmed antithesis and kill it
    gen.score_hypothesis(thesis_id)

    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT status, kill_reason FROM discovered_hypotheses WHERE id = :id"
        ), {"id": thesis_id}).fetchone()

    assert row[0] == "invalidated"
    assert row[1] == "ANTITHESIS_CONFIRMED"


def test_postmortem_records_full_context(engine):
    """Postmortem captures thesis text, antithesis text, lifespan, and evidence."""
    with engine.connect() as conn:
        pm = conn.execute(text(
            "SELECT hypothesis_id, kill_reason, thesis_text, confidence_at_death, "
            "lifespan_days FROM hypothesis_postmortems "
            "WHERE hypothesis_id = 'hyp_test_expired' LIMIT 1"
        )).fetchone()

    # This depends on test_expired_kill having run
    if pm is None:
        pytest.skip("Requires test_expired_kill to run first")

    assert pm[0] == "hyp_test_expired"
    assert pm[1] == "EXPIRED"
    assert pm[2] is not None  # thesis_text captured
    assert pm[3] is not None  # confidence_at_death captured
    assert pm[4] >= 25        # lifespan_days
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_hypothesis_kills.py::test_expired_kill tests/test_hypothesis_kills.py::test_confidence_collapsed_kill tests/test_hypothesis_kills.py::test_antithesis_confirmed_kills_thesis -v`
Expected: FAIL

- [ ] **Step 3: Implement kill checking, killing, and postmortem writing**

Add these methods to `HypothesisGenerator` class:

```python
    def _check_kills(
        self,
        h_id: str,
        ptype: str,
        criteria: dict,
        created_at: datetime,
        confidence: float,
        times_tested: int,
        outcome: str,
    ) -> str | None:
        """Check if any kill condition fires. Returns kill reason or None."""
        now = datetime.now(timezone.utc)
        window_days = criteria.get("window_days") or criteria.get("lag_days") or 7

        # Universal: antithesis confirmed?
        with self.engine.connect() as conn:
            pair_id = conn.execute(text(
                "SELECT pair_id FROM discovered_hypotheses WHERE id = :id"
            ), {"id": h_id}).scalar()
            if pair_id:
                anti_status = conn.execute(text(
                    "SELECT status FROM discovered_hypotheses "
                    "WHERE pair_id = :pid AND id != :id AND role != (SELECT role FROM discovered_hypotheses WHERE id = :id)"
                ), {"pid": pair_id, "id": h_id}).scalar()
                if anti_status == "confirmed":
                    return "ANTITHESIS_CONFIRMED"

        # Universal: confidence collapsed?
        if times_tested >= MIN_TESTS_FOR_CONFIDENCE_KILL and confidence < CONFIDENCE_KILL_THRESHOLD:
            return "CONFIDENCE_COLLAPSED"

        # Universal: expired? (past 2x window with no resolution)
        if (now - created_at).days > window_days * 2 and outcome == "inconclusive":
            return "EXPIRED"

        # Type-specific kills
        if ptype == "lead_lag" and outcome == "invalidated":
            return "PATTERN_BROKEN"

        if ptype == "convergence":
            if outcome == "invalidated":
                return "WRONG_DIRECTION"
            if (now - created_at).days > window_days and outcome == "inconclusive":
                return "NO_MOVE"

        if ptype == "volume_anomaly":
            if (now - created_at).days > window_days and outcome == "inconclusive":
                return "NO_FOLLOW_THROUGH"

        if ptype == "actor_shift":
            if (now - created_at).days > window_days and outcome == "inconclusive":
                return "ACTOR_RETREATED"

        return None

    def _kill_hypothesis(self, h_id: str, kill_reason: str) -> None:
        """Mark a hypothesis as invalidated with a named kill reason."""
        update = text("""
            UPDATE discovered_hypotheses
            SET status = 'invalidated',
                kill_reason = :reason,
                killed_at = NOW()
            WHERE id = :id
        """)
        with self.engine.begin() as conn:
            conn.execute(update, {"reason": kill_reason, "id": h_id})
        log.info("hypothesis killed: {} — reason: {}", h_id, kill_reason)

    def _write_postmortem(self, h_id: str, kill_reason: str) -> None:
        """Write a postmortem record for a killed hypothesis."""
        q = text("""
            SELECT thesis, confidence, times_tested, times_correct,
                   created_at, pair_id, evidence
            FROM discovered_hypotheses WHERE id = :id
        """)
        with self.engine.connect() as conn:
            row = conn.execute(q, {"id": h_id}).fetchone()

        if row is None:
            return

        thesis_text, conf, tested, correct, created, pair_id, evidence = row
        lifespan = (datetime.now(timezone.utc) - created).days if created else 0

        # Get antithesis text if it exists
        anti_text = None
        if pair_id:
            with self.engine.connect() as conn:
                anti_text = conn.execute(text(
                    "SELECT thesis FROM discovered_hypotheses "
                    "WHERE pair_id = :pid AND id != :id"
                ), {"pid": pair_id, "id": h_id}).scalar()

        insert = text("""
            INSERT INTO hypothesis_postmortems
                (hypothesis_id, kill_reason, evidence, thesis_text,
                 antithesis_text, confidence_at_death, times_tested,
                 times_correct, lifespan_days)
            VALUES
                (:hid, :reason, :evidence, :thesis, :anti,
                 :conf, :tested, :correct, :lifespan)
        """)
        with self.engine.begin() as conn:
            conn.execute(insert, {
                "hid": h_id,
                "reason": kill_reason,
                "evidence": json.dumps(evidence) if not isinstance(evidence, str) else evidence,
                "thesis": thesis_text,
                "anti": anti_text,
                "conf": float(conf) if conf else 0.0,
                "tested": tested or 0,
                "correct": correct or 0,
                "lifespan": lifespan,
            })
        log.info("postmortem written: {} — {}", h_id, kill_reason)
```

Now update `score_hypothesis()` to call kill checks. Replace the status update logic (lines ~755-779):

```python
    def score_hypothesis(self, hypothesis_id: str) -> dict:
        """Score a hypothesis against new data since it was created."""
        q = text("""
            SELECT id, thesis, pattern_type, evidence, test_criteria,
                   invalidation, confidence, status, times_tested,
                   times_correct, created_at
            FROM discovered_hypotheses
            WHERE id = :hid
        """)
        with self.engine.connect() as conn:
            row = conn.execute(q, {"hid": hypothesis_id}).fetchone()

        if row is None:
            return {"error": f"hypothesis {hypothesis_id} not found"}

        h_id, thesis, ptype, evidence, criteria, inv, conf, status, tested, correct, created = row
        if status != "active":
            return {"id": h_id, "status": status, "message": "not active, skipping"}

        criteria = criteria if isinstance(criteria, dict) else json.loads(criteria or "{}")
        outcome = self._evaluate_criteria(criteria, created)

        # Bayesian update
        tested += 1
        if outcome == "confirmed":
            correct += 1
        new_conf = (correct + 1) / (tested + 2)  # Beta-distribution posterior

        if outcome == "invalidated":
            new_conf = max(new_conf * 0.5, 0.01)

        # Check for kill conditions
        kill_reason = self._check_kills(
            h_id, ptype, criteria, created, new_conf, tested, outcome,
        )

        new_status = status
        if kill_reason:
            new_status = "invalidated"
        elif outcome == "confirmed" and new_conf > 0.75:
            new_status = "confirmed"

        update = text("""
            UPDATE discovered_hypotheses
            SET confidence = :conf,
                status = :status,
                times_tested = :tested,
                times_correct = :correct,
                last_tested = NOW(),
                kill_reason = :kill_reason,
                killed_at = CASE WHEN :kill_reason IS NOT NULL THEN NOW() ELSE killed_at END
            WHERE id = :hid
        """)
        with self.engine.begin() as conn:
            conn.execute(update, {
                "conf": round(new_conf, 4),
                "status": new_status,
                "tested": tested,
                "correct": correct,
                "hid": h_id,
                "kill_reason": kill_reason,
            })

        # Write postmortem for killed hypotheses
        if kill_reason:
            self._write_postmortem(h_id, kill_reason)

        # If thesis confirmed, kill the antithesis (and vice versa)
        if new_status == "confirmed":
            self._kill_counterpart(h_id)

        return {
            "id": h_id,
            "thesis": thesis,
            "outcome": outcome,
            "confidence": round(new_conf, 4),
            "status": new_status,
            "times_tested": tested,
            "times_correct": correct,
            "kill_reason": kill_reason,
        }

    def _kill_counterpart(self, confirmed_id: str) -> None:
        """When a thesis/antithesis is confirmed, kill its counterpart."""
        with self.engine.connect() as conn:
            row = conn.execute(text(
                "SELECT pair_id, role FROM discovered_hypotheses WHERE id = :id"
            ), {"id": confirmed_id}).fetchone()
        if not row or not row[0]:
            return

        pair_id, role = row
        with self.engine.connect() as conn:
            counterpart = conn.execute(text(
                "SELECT id, status FROM discovered_hypotheses "
                "WHERE pair_id = :pid AND id != :id AND status = 'active'"
            ), {"pid": pair_id, "id": confirmed_id}).fetchone()

        if counterpart:
            self._kill_hypothesis(counterpart[0], "ANTITHESIS_CONFIRMED")
            self._write_postmortem(counterpart[0], "ANTITHESIS_CONFIRMED")
```

- [ ] **Step 4: Run all kill tests**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_hypothesis_kills.py -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/anikdang/dev/GRID && git add intelligence/hypothesis_engine.py tests/test_hypothesis_kills.py
git commit -m "feat: named kill conditions with postmortem records, antithesis cross-kill"
```

---

### Task 5: Update CLI + Stats

Add kill stats to `get_stats()` and a `postmortems` CLI command.

**Files:**
- Modify: `intelligence/hypothesis_engine.py` — `get_stats()` (~line 1244), `main()` (~line 1339)

- [ ] **Step 1: Write failing test for kill stats**

Add to `tests/test_hypothesis_kills.py`:

```python
def test_stats_include_kill_breakdown(engine):
    """Stats report includes kill_reason breakdown."""
    from intelligence.hypothesis_engine import get_stats
    stats = get_stats(engine)
    assert "by_kill_reason" in stats
    assert isinstance(stats["by_kill_reason"], dict)


def test_kill_reasons_constant_is_exported():
    """KILL_REASONS dict is importable and non-empty."""
    from intelligence.hypothesis_engine import KILL_REASONS
    assert len(KILL_REASONS) >= 10
    assert "ANTITHESIS_CONFIRMED" in KILL_REASONS
    assert "EXPIRED" in KILL_REASONS
    assert "PATTERN_BROKEN" in KILL_REASONS
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_hypothesis_kills.py::test_stats_include_kill_breakdown tests/test_hypothesis_kills.py::test_kill_reasons_constant_is_exported -v`
Expected: FAIL — `by_kill_reason` not in stats dict

- [ ] **Step 3: Add kill breakdown to get_stats()**

In `get_stats()`, after the `q_types` query block (around line 1310), add:

```python
    # Kill reason breakdown
    q_kills = text("""
        SELECT kill_reason, COUNT(*)
        FROM discovered_hypotheses
        WHERE kill_reason IS NOT NULL
        GROUP BY kill_reason
        ORDER BY COUNT(*) DESC
    """)
    with engine.connect() as conn:
        by_kill = {
            r[0]: r[1]
            for r in conn.execute(q_kills)
        }

    # Postmortem count
    q_pm = text("SELECT COUNT(*) FROM hypothesis_postmortems")
    with engine.connect() as conn:
        pm_count = conn.execute(q_pm).scalar() or 0

    # Thesis/antithesis breakdown
    q_roles = text("""
        SELECT role, COUNT(*) FROM discovered_hypotheses
        GROUP BY role ORDER BY role
    """)
    with engine.connect() as conn:
        by_role = {r[0]: r[1] for r in conn.execute(q_roles)}
```

Add these to the return dict:

```python
    return {
        # ... existing fields ...
        "by_kill_reason": by_kill,
        "postmortem_count": pm_count,
        "by_role": by_role,
    }
```

Add `postmortems` CLI command in `main()`:

```python
    elif cmd == "postmortems":
        q = text("""
            SELECT hypothesis_id, kill_reason, thesis_text, antithesis_text,
                   confidence_at_death, lifespan_days, created_at
            FROM hypothesis_postmortems
            ORDER BY created_at DESC
            LIMIT 20
        """)
        with gen.engine.connect() as conn:
            rows = conn.execute(q).fetchall()
        pms = [
            {
                "hypothesis_id": r[0],
                "kill_reason": r[1],
                "thesis": (r[2] or "")[:120],
                "antithesis": (r[3] or "")[:120],
                "confidence_at_death": round(float(r[4]), 4) if r[4] else None,
                "lifespan_days": r[5],
                "killed_at": str(r[6]),
            }
            for r in rows
        ]
        _print_json(pms)
```

Update the CLI help string and usage line at the bottom to include `postmortems`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_hypothesis_kills.py -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/anikdang/dev/GRID && git add intelligence/hypothesis_engine.py tests/test_hypothesis_kills.py
git commit -m "feat: kill stats in get_stats(), postmortems CLI command"
```

---

### Task 6: Deploy to Server + Migrate Existing Data

**Files:**
- No code changes — [[deployment]] only

- [ ] **Step 1: Push to origin**

```bash
cd /Users/anikdang/dev/GRID && git push origin main
```

- [ ] **Step 2: Pull on server**

```bash
ssh grid-svr 'cd ~/grid_v4/grid_repo && git pull origin main'
```

- [ ] **Step 3: Run schema migration on server**

```bash
ssh grid-svr 'cd ~/grid_v4/grid_repo && PYTHONPATH=. python3 -c "
from db import get_engine
from intelligence.hypothesis_engine import ensure_tables
engine = get_engine()
ensure_tables(engine)
print(\"Schema migrated\")
"'
```

- [ ] **Step 4: Backfill existing hypotheses with role/pair_id**

```bash
ssh grid-svr 'cd ~/grid_v4/grid_repo && PYTHONPATH=. python3 -c "
from sqlalchemy import text
from db import get_engine
engine = get_engine()
with engine.begin() as conn:
    # Set role=thesis and pair_id=id for all existing hypotheses
    result = conn.execute(text(\"\"\"
        UPDATE discovered_hypotheses
        SET role = '"'"'thesis'"'"', pair_id = id
        WHERE role IS NULL OR pair_id IS NULL
    \"\"\"))
    print(f\"Backfilled {result.rowcount} hypotheses with role/pair_id\")
"'
```

- [ ] **Step 5: Generate antitheses for existing hypotheses**

```bash
ssh grid-svr 'cd ~/grid_v4/grid_repo && PYTHONPATH=. python3 -c "
from sqlalchemy import text
from db import get_engine
from intelligence.hypothesis_engine import HypothesisGenerator, Hypothesis
import json

engine = get_engine()
gen = HypothesisGenerator(engine)

with engine.connect() as conn:
    rows = conn.execute(text(
        \"SELECT id, thesis, pattern_type, evidence, test_criteria, invalidation, confidence \"
        \"FROM discovered_hypotheses WHERE role = '"'"'thesis'"'"' AND status = '"'"'active'"'"'\"
    )).fetchall()

count = 0
for r in rows:
    hyp = Hypothesis(
        id=r[0], thesis=r[1], pattern_type=r[2],
        evidence=json.loads(r[3]) if isinstance(r[3], str) else r[3],
        test_criteria=json.loads(r[4]) if isinstance(r[4], str) else r[4],
        invalidation=r[5], confidence=float(r[6]),
    )
    anti = gen._make_antithesis(hyp)
    if anti:
        anti._role = '"'"'antithesis'"'"'
        anti._pair_id = hyp.id
        if gen._store_hypothesis.__wrapped__ if hasattr(gen._store_hypothesis, '"'"'__wrapped__'"'"') else True:
            # Direct insert to avoid recursive antithesis generation
            from intelligence.hypothesis_engine import text as sql_text
            import json as j
            upsert = text(\"\"\"
                INSERT INTO discovered_hypotheses
                    (id, thesis, pattern_type, evidence, test_criteria,
                     invalidation, confidence, status, role, pair_id)
                VALUES (:id, :thesis, :ptype, :evidence, :criteria,
                        :inv, :conf, :status, :role, :pair_id)
                ON CONFLICT (id) DO NOTHING
            \"\"\")
            with engine.begin() as conn2:
                res = conn2.execute(upsert, {
                    '"'"'id'"'"': anti.id, '"'"'thesis'"'"': anti.thesis,
                    '"'"'ptype'"'"': anti.pattern_type,
                    '"'"'evidence'"'"': j.dumps(anti.evidence),
                    '"'"'criteria'"'"': j.dumps(anti.test_criteria),
                    '"'"'inv'"'"': anti.invalidation,
                    '"'"'conf'"'"': float(anti.confidence),
                    '"'"'status'"'"': '"'"'active'"'"',
                    '"'"'role'"'"': '"'"'antithesis'"'"',
                    '"'"'pair_id'"'"': hyp.id,
                })
                if res.rowcount > 0:
                    count += 1

print(f\"Generated {count} antitheses for existing hypotheses\")
"'
```

- [ ] **Step 6: Verify counts**

```bash
ssh grid-svr 'cd ~/grid_v4/grid_repo && PYTHONPATH=. python3 intelligence/hypothesis_engine.py stats'
```

Expected: stats output shows `by_role` with both `thesis` and `antithesis` counts, and `by_kill_reason` (empty for now since no scoring has run yet).

- [ ] **Step 7: Run scoring with new kill system**

```bash
ssh grid-svr 'cd ~/grid_v4/grid_repo && PYTHONPATH=. nohup python3 intelligence/hypothesis_engine.py score-all > /data/grid/logs/hypothesis_score.log 2>&1 & echo "Scoring PID: $!"'
```

---

## Summary

| Task | What it does |
|------|-------------|
| 1 | Schema: 4 new columns + `hypothesis_postmortems` table |
| 2 | Auto-generate antithesis for every thesis, linked via `pair_id` |
| 3 | `score_all()` uses per-hypothesis `window_days`/`lag_days` |
| 4 | Named kill checks + postmortem writes on every kill |
| 5 | Stats + CLI for viewing kill breakdown and postmortems |
| 6 | Deploy, migrate existing data, generate antitheses, run scoring |
