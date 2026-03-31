"""
GRID Oracle — Model Evolver.

Autonomous lifecycle manager: create, mutate, crossover, score, kill models.
Run every 6h via Hermes. Reads stats from oracle_predictions, kills losers,
spawns mutations/crossovers of top performers, seeds from hypothesis discoveries.
"""

from __future__ import annotations
import json, random, string
from dataclasses import dataclass, field
from typing import Any
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

MAX_ACTIVE_MODELS = 50
MIN_PREDICTIONS_TO_SCORE = 10
KILL_THRESHOLD_HIT_RATE = 0.20
KILL_THRESHOLD_PREDICTIONS = 20
LEADERBOARD_BAND = 5
MIN_SIGNAL_SOURCES = 2

def _rand(n=6): return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))
def _trunc(s, n=20): return s[:n]

def _parse_json_list(v):
    if v is None: return []
    if isinstance(v, list): return [str(x) for x in v]
    if isinstance(v, str):
        try:
            p = json.loads(v)
            return [str(x) for x in p] if isinstance(p, list) else []
        except Exception:
            return []
    return []

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

        # Cap enforcement
        current = self._count_active()
        surplus = (current + 4) - MAX_ACTIVE_MODELS
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
                    FROM oracle_predictions WHERE verdict IN ('hit','miss','partial') GROUP BY model_name
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
        self._insert(name=name, desc=f"Mutation of {parent_name}: {note}", sources=new,
                     families=_parse_json_list(row.get("signal_families")),
                     parent=parent_name, created_by="evolver:mutation")
        return name

    def _crossover(self, a: str, b: str) -> str | None:
        ra, rb = self._load(a), self._load(b)
        if not ra or not rb: return None
        sa = set(_parse_json_list(ra.get("signal_sources")))
        sb = set(_parse_json_list(rb.get("signal_sources")))
        if sa == sb: return None
        name = f"cross_{_trunc(a,10)}_{_trunc(b,10)}_{_rand(4)}"
        self._insert(name=name, desc=f"Crossover {a} x {b}", sources=sorted(sa | sb),
                     families=sorted(set(_parse_json_list(ra.get("signal_families"))) | set(_parse_json_list(rb.get("signal_families")))),
                     parent=a, created_by="evolver:crossover")
        return name

    def _from_hypothesis(self) -> str | None:
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT id, thesis, evidence, confidence FROM discovered_hypotheses
                    WHERE status IN ('confirmed','active') AND confidence >= 0.65
                    AND created_at >= NOW() - INTERVAL '7 days' ORDER BY confidence DESC LIMIT 10
                """)).fetchall()
        except Exception:
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
            self._insert(name=name, desc=f"Hypothesis {str(hid)[:12]}: {str(thesis)[:100]}",
                         sources=sources, families=[], parent=None, created_by="evolver:hypothesis")
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
        except Exception:
            return []

    def _load(self, name):
        with self.engine.connect() as conn:
            r = conn.execute(text("SELECT name, signal_sources, signal_families, description, parent_model FROM oracle_models WHERE name=:n"), {"n": name}).fetchone()
        return {"name": r[0], "signal_sources": r[1], "signal_families": r[2], "description": r[3], "parent_model": r[4]} if r else None

    def _insert(self, *, name, desc, sources, families, parent, created_by):
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO oracle_models (name, version, description, signal_families, signal_sources, weight, active, parent_model, created_by, last_updated)
                VALUES (:name, '1.0', :desc, :fam, :src, 1.0, TRUE, :parent, :cb, NOW()) ON CONFLICT (name) DO NOTHING
            """), {"name": name, "desc": desc, "fam": json.dumps(families), "src": json.dumps(sources), "parent": parent, "cb": created_by})

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
