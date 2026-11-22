"""
GRID Oracle — Model Evolver.

Autonomous lifecycle manager: create, mutate, crossover, score, kill models.
Run every 6h via Hermes. Reads stats from oracle_predictions, kills losers,
spawns mutations/crossovers of top performers, seeds from hypothesis discoveries.

Shadow promotion needs a harness that records shadow predictions without voting;
until it exists, shadow models stay inactive and are promoted only by an operator
after review.
"""

from __future__ import annotations
import json
import random
import string
from dataclasses import dataclass, field
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

MAX_ACTIVE_MODELS = 50
MIN_PREDICTIONS_TO_SCORE = 10
KILL_THRESHOLD_HIT_RATE = 0.20
KILL_THRESHOLD_PREDICTIONS = 20
LEADERBOARD_BAND = 5
MIN_SIGNAL_SOURCES = 2
SHADOW_TAG = "[shadow]"

def _rand(n=6): return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))
def _trunc(s, n=20): return s[:n]

def _parse_json_list(v):
    if v is None: return []
    if isinstance(v, list): return [str(x) for x in v]
    if isinstance(v, str):
        try:
            p = json.loads(v)
            return [str(x) for x in p] if isinstance(p, list) else []
        except Exception as e:
            log.debug("_parse_sources JSON parse failed: {e}", e=str(e))
            return []
    return []


def _canonical_families(v) -> tuple[str, ...]:
    """Canonical vote-determining signature read by OracleEngine._load_models."""
    return tuple(sorted({s.strip() for s in _parse_json_list(v) if s and s.strip()}))


def _tag_shadow_description(desc: str) -> str:
    clean = (desc or "").strip()
    if clean.startswith(SHADOW_TAG):
        return clean
    return f"{SHADOW_TAG} {clean}".strip()

@dataclass
class EvolveResult:
    killed: list[str] = field(default_factory=list)
    spawned: list[str] = field(default_factory=list)
    top_models: list[dict] = field(default_factory=list)
    bottom_models: list[dict] = field(default_factory=list)
    active_before: int = 0
    active_after: int = 0
    hypothesis_seeds: int = 0
    errors: list[str] = field(default_factory=list)
    def to_dict(self): return {k: getattr(self, k) for k in self.__dataclass_fields__}


class ModelEvolver:
    def __init__(self, engine: Engine): self.engine = engine

    def evolve_cycle(self) -> dict:
        r = EvolveResult()
        r.active_before = self._count_active()
        log.info("ModelEvolver: start — {n} active", n=r.active_before)

        stats = self._get_stats()
        scored = sorted([s for s in stats if s["total"] >= MIN_PREDICTIONS_TO_SCORE],
                        key=lambda s: s["adj_hr"], reverse=True)
        r.top_models = scored[:LEADERBOARD_BAND]
        r.bottom_models = scored[-LEADERBOARD_BAND:] if len(scored) >= LEADERBOARD_BAND else list(scored)

        # Kill losers
        for s in stats:
            if s["total"] >= KILL_THRESHOLD_PREDICTIONS and s["hr"] < KILL_THRESHOLD_HIT_RATE:
                try:
                    self._kill(s["name"], f"hr={s['hr']:.2%} after {s['total']} preds")
                    r.killed.append(s["name"])
                except Exception as e: r.errors.append(f"kill {s['name']}: {e}")

        # Cap enforcement (new shadow models start inactive, so only trim active surplus)
        current = self._count_active()
        surplus = current - MAX_ACTIVE_MODELS
        if surplus > 0:
            killed_set = set(r.killed)
            killable = [s for s in reversed(scored) if s["name"] not in killed_set]
            for s in killable[:surplus]:
                try:
                    self._kill(s["name"], f"cap enforcement, hr={s['hr']:.2%}")
                    r.killed.append(s["name"])
                except Exception as e: r.errors.append(f"cap-kill {s['name']}: {e}")

        # Spawn mutations + crossover
        if scored:
            top = scored[0]["name"]
            for i in range(2):
                try:
                    c = self._mutate(top)
                    if c: r.spawned.append(c)
                except Exception as e: r.errors.append(f"mutation {i}: {e}")
            if len(scored) >= 2:
                try:
                    c = self._crossover(top, scored[1]["name"])
                    if c: r.spawned.append(c)
                except Exception as e: r.errors.append(f"crossover: {e}")

        # Hypothesis seed
        try:
            c = self._from_hypothesis()
            if c:
                r.spawned.append(c)
                r.hypothesis_seeds += 1
        except Exception as e: r.errors.append(f"hypothesis: {e}")

        r.active_after = self._count_active()
        self._log_iteration(r, scored[0] if scored else None, scored[-1] if scored else None)
        log.info("ModelEvolver: done — killed={k} spawned={s} active {b}->{a}",
                 k=len(r.killed), s=len(r.spawned), b=r.active_before, a=r.active_after)
        return r.to_dict()

    def _get_stats(self) -> list[dict]:
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT m.name, m.signal_sources, m.signal_families,
                       COALESCE(p.hits,0), COALESCE(p.misses,0), COALESCE(p.partials,0)
                FROM oracle_models m LEFT JOIN (
                    SELECT model_name, COUNT(*) FILTER (WHERE verdict='hit') AS hits,
                           COUNT(*) FILTER (WHERE verdict='miss') AS misses,
                           COUNT(*) FILTER (WHERE verdict='partial') AS partials
                    FROM oracle_predictions
                    WHERE verdict IN ('hit','miss','partial')
                      AND dedup_keep = TRUE
                      AND id NOT LIKE 'astrogrid:%'
                    GROUP BY model_name
                ) p ON p.model_name=m.name WHERE m.active=TRUE
            """)).fetchall()
        out = []
        for name, src, fam, h, m, p in rows:
            t = h + m + p
            out.append({"name": name, "sources": _parse_json_list(src), "families": _parse_json_list(fam),
                         "hits": h, "misses": m, "partials": p, "total": t,
                         "hr": round(h/t, 4) if t else 0.0,
                         "adj_hr": round((h + p*0.5)/t, 4) if t else 0.0})
        return out

    def _mutate(self, parent_name: str) -> str | None:
        row = self._load(parent_name)
        if not row: return None
        sources = _parse_json_list(row.get("signal_sources"))
        parent_families = _parse_json_list(row.get("signal_families"))
        available = self._available_sources()
        can_add = bool(set(available) - set(sources))
        can_rm = len(sources) > MIN_SIGNAL_SOURCES
        if not can_add and not can_rm: return None
        new = list(sources)
        if can_add and (not can_rm or random.random() < 0.5):
            add = random.choice(list(set(available) - set(sources)))
            new.append(add)
            note = f"added {add}"
        else:
            rm = random.choice(new)
            new.remove(rm)
            note = f"removed {rm}"
        name = f"{_trunc(parent_name)}_mut_{_rand()}"
        inserted = self._insert(
            name=name, desc=f"Mutation of {parent_name}: {note}", sources=new,
            families=parent_families, parent=parent_name,
            created_by="evolver:mutation", parent_families=parent_families,
        )
        return name if inserted else None

    def _crossover(self, a: str, b: str) -> str | None:
        ra, rb = self._load(a), self._load(b)
        if not ra or not rb: return None
        sa = set(_parse_json_list(ra.get("signal_sources")))
        sb = set(_parse_json_list(rb.get("signal_sources")))
        fa = _canonical_families(ra.get("signal_families"))
        fb = _canonical_families(rb.get("signal_families"))
        new_families = sorted(set(fa) | set(fb))
        new_sig = _canonical_families(new_families)
        if not new_sig or new_sig in (fa, fb):
            log.info(
                "ModelEvolver: refusing crossover {a} x {b} — vote-determining "
                "signal_families {f} does not differ from parents",
                a=a, b=b, f=list(new_sig),
            )
            return None
        name = f"cross_{_trunc(a,10)}_{_trunc(b,10)}_{_rand(4)}"
        inserted = self._insert(
            name=name, desc=f"Crossover {a} x {b}", sources=sorted(sa | sb),
            families=new_families, parent=a, created_by="evolver:crossover",
        )
        return name if inserted else None

    def _from_hypothesis(self) -> str | None:
        # Fails closed (families=[] is refused as empty, creating nothing); the 301 existing evolver:hypothesis models are untouched.
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT id, thesis, evidence, confidence FROM discovered_hypotheses
                    WHERE (status = 'active'
                           OR (status = 'confirmed' AND scoring_version IS NOT NULL))
                    AND confidence >= 0.65
                    AND created_at >= NOW() - INTERVAL '7 days' ORDER BY confidence DESC LIMIT 10
                """)).fetchall()
        except Exception as e:
            log.warning("ModelEvolver: hypothesis query failed: {e}", e=str(e))
            return None
        for hid, thesis, ev_raw, conf in (rows or []):
            name = f"hyp_{str(hid)[:12]}"
            if self._exists(name): continue
            ev = json.loads(ev_raw) if isinstance(ev_raw, str) else (ev_raw or [])
            sources = []
            for item in (ev if isinstance(ev, list) else []):
                if isinstance(item, dict):
                    for k in ("source_module", "source", "signal_a", "signal_b"):
                        v = item.get(k)
                        if v and isinstance(v, str) and v not in sources: sources.append(v)
            if not sources: sources = self._available_sources()[:3]
            inserted = self._insert(
                name=name, desc=f"Hypothesis {str(hid)[:12]}: {str(thesis)[:100]}",
                sources=sources, families=[], parent=None, created_by="evolver:hypothesis",
            )
            if inserted:
                return name
        return None

    def _kill(self, name, reason=""):
        with self.engine.begin() as conn:
            conn.execute(text("UPDATE oracle_models SET active=FALSE, last_updated=NOW() WHERE name=:n"), {"n": name})
        log.info("ModelEvolver: killed {n} — {r}", n=name, r=reason)

    def _count_active(self):
        with self.engine.connect() as conn:
            return int(conn.execute(text("SELECT COUNT(*) FROM oracle_models WHERE active=TRUE")).fetchone()[0])

    def _exists(self, name):
        with self.engine.connect() as conn:
            return conn.execute(text("SELECT 1 FROM oracle_models WHERE name=:n LIMIT 1"), {"n": name}).fetchone() is not None

    def _available_sources(self):
        try:
            with self.engine.connect() as conn:
                return [r[0] for r in conn.execute(text("SELECT DISTINCT source_module FROM signal_registry ORDER BY source_module")).fetchall()]
        except Exception as e:
            log.warning("ModelEvolver: available sources query failed: {e}", e=str(e))
            return []

    def _load(self, name):
        with self.engine.connect() as conn:
            r = conn.execute(text("SELECT name, signal_sources, signal_families, description, parent_model FROM oracle_models WHERE name=:n"), {"n": name}).fetchone()
        return {"name": r[0], "signal_sources": r[1], "signal_families": r[2], "description": r[3], "parent_model": r[4]} if r else None

    def _active_or_shadow_family_signatures(self) -> set[tuple[str, ...]] | None:
        """Return canonical signal_families sets for all active or [shadow] models, or None on DB error."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT signal_families FROM oracle_models "
                        "WHERE active = TRUE OR description LIKE :shadow_prefix"
                    ),
                    {"shadow_prefix": f"{SHADOW_TAG}%"},
                ).fetchall()
            return {_canonical_families(r[0]) for r in rows}
        except Exception as e:
            log.warning(
                "ModelEvolver: active/shadow signal_families query failed (failing closed): {e}",
                e=str(e),
            )
            return None

    def _is_functional_clone(
        self,
        families: list[str],
        parent_families: list[str] | None = None,
    ) -> tuple[bool, str]:
        """Refuse models whose vote-determining fields (signal_families) clone an active or shadow model."""
        # The comparison uses signal_families only because GRID_SIGNAL_REGISTRY
        # defaults off (oracle/engine.py ~L44); if registry mode is enabled,
        # signal_sources becomes vote-relevant.
        cand_sig = _canonical_families(families)
        if not cand_sig:
            return True, "empty vote-determining signal_families (OracleEngine._load_models reads only signal_families)"
        if parent_families is not None and cand_sig == _canonical_families(parent_families):
            return True, f"unchanged vote-determining signal_families {list(cand_sig)} (mutation only modified unread fields)"
        existing_sigs = self._active_or_shadow_family_signatures()
        if existing_sigs is None:
            return True, "unable to verify active/shadow model signal_families (fail closed)"
        if cand_sig in existing_sigs:
            return True, f"vote-determining signal_families {list(cand_sig)} equals an existing active or shadow model"
        return False, ""

    def _insert(self, *, name, desc, sources, families, parent, created_by, parent_families=None) -> bool:
        is_clone, reason = self._is_functional_clone(families, parent_families=parent_families)
        if is_clone:
            log.info("ModelEvolver: refusing to create '{n}' — {r}", n=name, r=reason)
            return False
        shadow_desc = _tag_shadow_description(desc)
        canon_families = list(_canonical_families(families))
        with self.engine.begin() as conn:
            res = conn.execute(text("""
                INSERT INTO oracle_models (name, version, description, signal_families, signal_sources, weight, active, parent_model, created_by, last_updated)
                VALUES (:name, '1.0', :desc, :fam, :src, 0.0, FALSE, :parent, :cb, NOW()) ON CONFLICT (name) DO NOTHING
            """), {"name": name, "desc": shadow_desc, "fam": json.dumps(canon_families), "src": json.dumps(sources), "parent": parent, "cb": created_by})
        if getattr(res, "rowcount", 1) == 0:
            return False
        log.info("ModelEvolver: staged shadow model '{n}' (active=FALSE, weight=0.0)", n=name)
        return True

    def _log_iteration(self, r, best, worst):
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO oracle_iterations (models_updated, predictions_scored, best_model, best_hit_rate, worst_model, worst_hit_rate, weight_changes, notes)
                    VALUES (:mu, 0, :bm, :bhr, :wm, :whr, :wc, :notes)
                """), {"mu": len(r.spawned), "bm": best["name"] if best else None, "bhr": best["hr"] if best else None,
                       "wm": worst["name"] if worst else None, "whr": worst["hr"] if worst else None,
                       "wc": json.dumps({"killed": r.killed, "spawned": r.spawned}),
                       "notes": f"killed={len(r.killed)} spawned={len(r.spawned)} active {r.active_before}->{r.active_after}"})
        except Exception as e:
            log.warning("ModelEvolver: failed to log iteration: {e}", e=e)
