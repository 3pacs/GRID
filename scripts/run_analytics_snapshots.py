#!/usr/bin/env python3
"""GRID analytics snapshots — the analytics-only slice of run_full_pipeline.

Refreshes the ``analytical_snapshots`` categories the discovery / options /
models views read, and nothing else:

    feature_engineering   derived macro/options features (FeatureLab)
    orthogonality         correlation / PCA dimensionality audit
    clustering            global unsupervised clustering (GMM on PCA)
    clustering_sector     the same clustering, one run per sector of
                          analysis/sector_map.py (subcategory = sector)
    feature_importance    importance report for the PRODUCTION model
    options_scan          summary of the latest options_mispricing_scans

Every value read goes through ``store.pit.PITStore`` (release_date <= as_of;
retractions are honoured once PR #683's reader lands), except
``options_scan``, which summarises rows the grid-scheduler options job
already persisted. Each snapshot payload carries a ``provenance`` block
(job, release SHA, as-of date, vintage policy, readiness-gate result,
per-input freshness / excluded stale features).

Deliberately NOT run here (see the PR for the reasoning):
    * ingestion, conflict resolution, the resolution audit, digest email
    * scripts/auto_regime.py (``regime_detection``) — already scheduled by
      grid-scheduler and fresh; the global regime label is not expanded here
    * smart discovery insights (STEP 7b) — its only outputs are emails, it
      re-runs orthogonality, and its dimensionality query selects columns
      (snapshot_type, result_data) that analytical_snapshots does not have
    * the options scanner itself — grid-scheduler runs and persists it daily;
      re-scanning would add a second writer to options_mispricing_scans
    * regime_state_vectors — intelligence/regime/state_vector.py reads
      raw_series directly (not store/pit.py) and z-scores against full
      history; it stays compute-on-read
    * held learning writes: hypothesis registry, weights, autoresearch, and
      feature_importance_log (importance is computed with persist=False)
    * LLM calls: clustering interpretation and orthogonality embeddings are
      switched off

Readiness gate (checked first; the job refuses to run unless all pass —
see ``check_readiness``):
    G1  resolved_series_retractions exists
    G2  it holds rows with run_tag = $GRID_ANALYTICS_REQUIRED_RUN_TAG
        (default reresolve_20260927)
    G3  the newest of those rows is older than
        $GRID_ANALYTICS_SETTLE_MINUTES (default 60) — insertion finished
    G4  the operator flag file $GRID_ANALYTICS_READY_FLAG (default
        /data/grid/state/analytics-snapshots.ready) exists and its first
        line is the run tag; written after 07_verify_retractions.sql passes
    G5  the resolver's daily refresh has landed: spy_full has a PIT
        observation within $GRID_ANALYTICS_MAX_RESOLVER_LAG_DAYS (default 4)

Usage:
    python3 scripts/run_analytics_snapshots.py                 # gated run
    python3 scripts/run_analytics_snapshots.py --check-only    # gate only
    python3 scripts/run_analytics_snapshots.py --dry-run       # compute, no writes
    python3 scripts/run_analytics_snapshots.py --steps orthogonality,clustering
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from loguru import logger as log  # noqa: E402
from sqlalchemy import text  # noqa: E402

JOB_NAME = "run_analytics_snapshots"

DEFAULT_RUN_TAG = "reresolve_20260927"
DEFAULT_READY_FLAG = "/data/grid/state/analytics-snapshots.ready"
DEFAULT_SETTLE_MINUTES = 60
DEFAULT_MAX_RESOLVER_LAG_DAYS = 4
RESOLVER_CANARY_FEATURE = "spy_full"

# A feature whose last PIT observation is older than this is excluded from a
# matrix (and listed) rather than truncating it. Same value the API uses.
MAX_STALENESS_DAYS = 10
# FeatureLab inputs: daily series must be this fresh; monthly releases get a
# publication-lag allowance instead of being reported as today's value.
FEATURE_INPUT_MAX_AGE_DAYS = 10
FEATURE_INPUT_AGE_OVERRIDES = {"cpi_yoy": 75}
# options_mispricing_scans older than this is not summarised as current.
OPTIONS_SCAN_MAX_AGE_DAYS = 4
OPTIONS_TOP_N = 10

# The orthogonality step's PIT query (get_feature_matrix -> get_pit) builds a
# ~2,480-feature matrix through the retraction anti-join in store/pit.py.
# That single statement hit the app-wide 120s default (db.py's
# statement_timeout, set on every pooled connection) once in prod and
# succeeded at 79s on retry -- it is legitimately heavier than the rest of
# this job's queries, not a runaway. db.py documents the escape hatch for
# exactly this ("Override per-call with `SET LOCAL statement_timeout = 0`
# for jobs that legitimately need longer") and scripts/enrich_connections.py
# is the established pattern: SET LOCAL as the first statement of the same
# connection/transaction the heavy query runs in, so the bump is
# transaction-scoped and never touches the shared engine default any other
# step or caller gets.
ORTHOGONALITY_STATEMENT_TIMEOUT = os.getenv("GRID_ANALYTICS_ORTHOGONALITY_STATEMENT_TIMEOUT", "600s")

CLUSTER_COMPONENTS = 3
MIN_SECTOR_FEATURES = 3
# Name shapes that belong to one ticker in feature_registry (e.g. nvda_full,
# xlk_iv_atm). Exact names only — a prefix LIKE would let one-letter
# tickers (C, E, K ...) swallow unrelated features.
SECTOR_FEATURE_SUFFIXES = (
    "_full", "_close", "_etf_close",
    "_pcr", "_max_pain", "_oi_conc", "_opt_vol", "_total_oi",
    "_iv_atm", "_iv_skew", "_iv_25d_put", "_iv_25d_call", "_term_slope",
)

ALL_STEPS = (
    "feature_engineering",
    "orthogonality",
    "clustering",
    "clustering_sector",
    "feature_importance",
    "options_scan",
)


# ---------------------------------------------------------------------------
# Provenance helpers
# ---------------------------------------------------------------------------

def release_sha() -> str | None:
    """Best-effort code identity: env, release-dir name, or git HEAD."""
    env_sha = os.getenv("GRID_RELEASE_SHA")
    if env_sha:
        return env_sha
    name = Path(_GRID_DIR).resolve().name
    if re.fullmatch(r"[0-9a-f]{40}", name):
        return name
    try:
        out = subprocess.run(
            ["git", "-C", _GRID_DIR, "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        sha = out.stdout.strip()
        return sha if re.fullmatch(r"[0-9a-f]{40}", sha) else None
    except Exception:
        return None


def _jsonable(obj: Any) -> Any:
    return json.loads(json.dumps(obj, default=str))


# ---------------------------------------------------------------------------
# Readiness gate
# ---------------------------------------------------------------------------

def _check(name: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"check": name, "ok": bool(ok), "detail": detail}


def check_readiness(
    engine: Any,
    as_of: date,
    run_tag: str | None = None,
    flag_path: str | None = None,
    settle_minutes: int | None = None,
    max_resolver_lag_days: int | None = None,
    pit_store: Any = None,
) -> dict[str, Any]:
    """Evaluate the readiness gate (G1-G5). Read-only.

    Returns ``{"ok": bool, "run_tag": ..., "checks": [...]}``. Every check is
    evaluated and reported even after one fails, so the log says exactly
    what is missing.
    """
    run_tag = run_tag or os.getenv("GRID_ANALYTICS_REQUIRED_RUN_TAG", DEFAULT_RUN_TAG)
    flag_path = flag_path or os.getenv("GRID_ANALYTICS_READY_FLAG", DEFAULT_READY_FLAG)
    if settle_minutes is None:
        settle_minutes = int(os.getenv("GRID_ANALYTICS_SETTLE_MINUTES", DEFAULT_SETTLE_MINUTES))
    if max_resolver_lag_days is None:
        max_resolver_lag_days = int(
            os.getenv("GRID_ANALYTICS_MAX_RESOLVER_LAG_DAYS", DEFAULT_MAX_RESOLVER_LAG_DAYS)
        )

    checks: list[dict[str, Any]] = []

    # G1-G3: the retraction run for run_tag has landed and stopped writing.
    try:
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT to_regclass('resolved_series_retractions') IS NOT NULL")
            ).scalar()
            checks.append(_check("G1_retractions_table", exists, "resolved_series_retractions"))
            if exists:
                row = conn.execute(
                    text(
                        "SELECT COUNT(*), MAX(retracted_at), "
                        "       MAX(retracted_at) <= NOW() - make_interval(mins => :settle) "
                        "FROM resolved_series_retractions WHERE run_tag = :tag"
                    ),
                    {"tag": run_tag, "settle": int(settle_minutes)},
                ).fetchone()
                n, newest, settled = int(row[0]), row[1], bool(row[2])
                checks.append(_check(
                    "G2_run_tag_rows", n > 0, {"run_tag": run_tag, "rows": n},
                ))
                checks.append(_check(
                    "G3_settled", n > 0 and settled,
                    {"newest_retracted_at": str(newest) if newest else None,
                     "settle_minutes": settle_minutes},
                ))
            else:
                checks.append(_check("G2_run_tag_rows", False, "table missing"))
                checks.append(_check("G3_settled", False, "table missing"))
    except Exception as exc:
        checks.append(_check("G1_retractions_table", False, f"query failed: {exc}"))

    # G4: operator's explicit go, written after 07_verify_retractions passes.
    try:
        first_line = Path(flag_path).read_text(encoding="utf-8").splitlines()[0].strip()
        checks.append(_check(
            "G4_ready_flag", first_line == run_tag,
            {"path": flag_path, "first_line": first_line},
        ))
    except Exception as exc:
        checks.append(_check("G4_ready_flag", False, {"path": flag_path, "error": str(exc)}))

    # G5: today's resolver refresh landed (PIT read of the canary feature).
    try:
        if pit_store is None:
            from store.pit import PITStore
            pit_store = PITStore(engine)
        with engine.connect() as conn:
            fid = conn.execute(
                text("SELECT id FROM feature_registry WHERE name = :n"),
                {"n": RESOLVER_CANARY_FEATURE},
            ).scalar()
        last_obs = None
        if fid is not None:
            df = pit_store.get_pit([int(fid)], as_of)
            if not df.empty:
                last_obs = max(df["obs_date"])
                if isinstance(last_obs, datetime):
                    last_obs = last_obs.date()
        ok = last_obs is not None and (as_of - last_obs).days <= max_resolver_lag_days
        checks.append(_check(
            "G5_resolver_fresh", ok,
            {"feature": RESOLVER_CANARY_FEATURE,
             "last_obs_date": last_obs.isoformat() if last_obs else None,
             "max_lag_days": max_resolver_lag_days},
        ))
    except Exception as exc:
        checks.append(_check("G5_resolver_fresh", False, f"query failed: {exc}"))

    return {
        "ok": all(c["ok"] for c in checks) and len(checks) >= 5,
        "run_tag": run_tag,
        "checks": checks,
    }


# ---------------------------------------------------------------------------
# Sector partition
# ---------------------------------------------------------------------------

def _ticker_feature_prefix(ticker: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", ticker.lower()).strip("_")


def sector_candidate_names(sector_map: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    """Map each sector to its ETF and candidate feature names.

    Candidate names are ``<ticker><suffix>`` for the sector ETF and every
    ticker in its sub-sectors (analysis/sector_map.py), for each suffix in
    ``SECTOR_FEATURE_SUFFIXES``.
    """
    if sector_map is None:
        from analysis.sector_map import SECTOR_MAP as sector_map
    out: dict[str, dict[str, Any]] = {}
    for sector, spec in sector_map.items():
        if not isinstance(spec, dict):
            continue
        tickers: set[str] = set()
        if spec.get("etf"):
            tickers.add(str(spec["etf"]))
        for sub in (spec.get("subsectors") or {}).values():
            if not isinstance(sub, dict):
                continue
            for actor in sub.get("actors") or []:
                tk = (actor.get("ticker") or "").strip()
                if tk:
                    tickers.add(tk)
        prefixes = sorted({_ticker_feature_prefix(t) for t in tickers if _ticker_feature_prefix(t)})
        names = [p + sfx for p in prefixes for sfx in SECTOR_FEATURE_SUFFIXES]
        out[sector] = {"etf": spec.get("etf"), "n_tickers": len(tickers), "names": names}
    return out


def resolve_sector_feature_ids(engine: Any, names: list[str]) -> list[int]:
    """Registry ids for exact candidate names (eligibility is checked later)."""
    if not names:
        return []
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id FROM feature_registry WHERE name = ANY(:names) ORDER BY id"),
            {"names": names},
        ).fetchall()
    return [int(r[0]) for r in rows]


class _ScopedTimeoutEngine:
    """Engine proxy that lifts ``statement_timeout`` only for connections
    opened through THIS wrapper, leaving the shared engine (and every other
    step's / caller's connections) on the app-wide default from db.py.

    ``SET LOCAL`` is transaction-scoped, so it must be the first statement
    executed on a connection to cover the query that follows in the same
    connect()/with-block; it is discarded automatically when that connection
    closes, so nothing needs to reset it afterwards. Any attribute other
    than ``connect`` (e.g. ``.dialect``, ``.url``) passes through to the
    real engine unchanged.
    """

    def __init__(self, engine: Any, timeout: str) -> None:
        self._engine = engine
        self._timeout = timeout

    def connect(self) -> Any:
        conn = self._engine.connect()
        conn.execute(text(f"SET LOCAL statement_timeout = '{self._timeout}'"))
        return conn

    def __getattr__(self, name: str) -> Any:
        return getattr(self._engine, name)


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------

class Job:
    """One run of the analytics snapshots job."""

    def __init__(self, engine: Any, as_of: date, dry_run: bool, gate: dict[str, Any]) -> None:
        from store.pit import PITStore
        from store.snapshots import AnalyticalSnapshotStore

        self.engine = engine
        self.as_of = as_of
        self.dry_run = dry_run
        self.pit = PITStore(engine)
        self.store = AnalyticalSnapshotStore(db_engine=engine, ensure_table=not dry_run)
        self.started_at = datetime.now(timezone.utc)
        self.base_provenance = {
            "job": JOB_NAME,
            "release_sha": release_sha(),
            "as_of_date": as_of.isoformat(),
            "run_started_at": self.started_at.isoformat(),
            "pit_reader": "store.pit.PITStore",
            "readiness_gate": {"ok": gate.get("ok"), "run_tag": gate.get("run_tag")},
            "dry_run": dry_run,
        }
        self.results: dict[str, Any] = {}

    # -- persistence ------------------------------------------------------

    def save(
        self,
        category: str,
        payload: dict[str, Any],
        metrics: dict[str, Any] | None = None,
        subcategory: str | None = None,
        provenance: dict[str, Any] | None = None,
    ) -> int | None:
        body = dict(payload)
        body["provenance"] = {**self.base_provenance, **(provenance or {})}
        body = _jsonable(body)
        if self.dry_run:
            log.info("[dry-run] would save {c}/{s}", c=category, s=subcategory)
            return None
        return self.store.save_snapshot(
            category=category,
            subcategory=subcategory,
            payload=body,
            as_of_date=self.as_of,
            metrics=_jsonable(metrics) if metrics else None,
        )

    # -- steps ------------------------------------------------------------

    def feature_engineering(self) -> dict[str, Any]:
        from features.lab import FeatureLab

        lab = FeatureLab(
            db_engine=self.engine,
            pit_store=self.pit,
            max_input_age_days=FEATURE_INPUT_MAX_AGE_DAYS,
            input_age_overrides=FEATURE_INPUT_AGE_OVERRIDES,
        )
        values = lab.compute_derived_features(as_of_date=self.as_of)
        n_non_null = sum(1 for v in values.values() if v is not None)
        if n_non_null == 0:
            return {"status": "skipped", "reason": "every derived feature is missing or stale",
                    "inputs": lab.input_provenance}
        snap_id = self.save(
            "feature_engineering",
            {"values": values},
            metrics={"n_features": len(values), "n_non_null": n_non_null},
            provenance={
                "vintage_policy": "LATEST_AS_OF",
                "input_max_age_days": FEATURE_INPUT_MAX_AGE_DAYS,
                "input_age_overrides": FEATURE_INPUT_AGE_OVERRIDES,
                "inputs": lab.input_provenance,
            },
        )
        return {"status": "ok", "snapshot_id": snap_id,
                "n_features": len(values), "n_non_null": n_non_null}

    def orthogonality(self) -> dict[str, Any]:
        from discovery.orthogonality import OrthogonalityAudit
        from store.pit import PITStore

        # Dedicated engine/PITStore bound to this step only (see
        # ORTHOGONALITY_STATEMENT_TIMEOUT / _ScopedTimeoutEngine above) so
        # the bounded higher timeout never leaks into self.engine /
        # self.pit, which every other step in this job shares.
        scoped_engine = _ScopedTimeoutEngine(self.engine, ORTHOGONALITY_STATEMENT_TIMEOUT)
        scoped_pit = PITStore(scoped_engine)
        audit = OrthogonalityAudit(db_engine=scoped_engine, pit_store=scoped_pit)
        summary = audit.run_full_audit(
            as_of_date=self.as_of,
            max_staleness_days=MAX_STALENESS_DAYS,
            semantic_similarity=False,
            persist=False,
        )
        if "error" in summary or not summary.get("n_features_analyzed"):
            return {"status": "skipped", "reason": summary.get("error", "no features"),
                    "excluded_stale": len(summary.get("excluded_stale_features") or {})}
        snap_id = self.save(
            "orthogonality",
            summary,
            metrics={
                "n_features": summary.get("n_features_analyzed"),
                "true_dimensionality": summary.get("true_dimensionality"),
                "n_correlated_pairs": len(summary.get("highly_correlated_pairs", [])),
                "n_unstable_pairs": len(summary.get("unstable_pairs", [])),
                "variance_at_true_dim": summary.get("variance_explained_by_true_dim"),
                "n_excluded_stale": len(summary.get("excluded_stale_features") or {}),
            },
            provenance={"vintage_policy": "FIRST_RELEASE"},
        )
        return {"status": "ok", "snapshot_id": snap_id,
                "n_features": summary.get("n_features_analyzed"),
                "matrix_end": summary.get("matrix_end")}

    def _cluster(
        self,
        feature_ids: list[int] | None,
        category: str,
        subcategory: str | None,
        partition: dict[str, Any] | None,
    ) -> dict[str, Any]:
        from discovery.clustering import ClusterDiscovery

        cd = ClusterDiscovery(db_engine=self.engine, pit_store=self.pit)
        result = cd.run_cluster_discovery(
            n_components=CLUSTER_COMPONENTS,
            as_of_date=self.as_of,
            feature_ids=feature_ids,
            max_staleness_days=MAX_STALENESS_DAYS,
            interpret=False,
            persist=False,
            partition=partition,
        )
        if "error" in result:
            return {"status": "skipped", "reason": result["error"]}
        best = next(
            (m for m in result.get("all_metrics", []) if m.get("k") == result.get("best_k")),
            {},
        )
        snap_id = self.save(
            category,
            result,
            subcategory=subcategory,
            metrics={
                "best_k": result.get("best_k"),
                "n_observations": result.get("n_observations"),
                "n_features": result.get("n_features"),
                "pca_components": result.get("pca_components_used"),
                "variance_explained": result.get("variance_explained"),
                "best_silhouette": best.get("kmeans_silhouette"),
                "best_persistence": best.get("gmm_persistence"),
            },
            provenance={"vintage_policy": "FIRST_RELEASE", "partition": partition},
        )
        return {"status": "ok", "snapshot_id": snap_id, "best_k": result.get("best_k"),
                "n_features": result.get("n_features"), "matrix_end": result.get("matrix_end")}

    def clustering(self) -> dict[str, Any]:
        return self._cluster(None, "clustering", None, {"scope": "global"})

    def clustering_sector(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for sector, spec in sector_candidate_names().items():
            ids = resolve_sector_feature_ids(self.engine, spec["names"])
            partition = {"scope": "sector", "sector": sector, "etf": spec["etf"],
                         "n_tickers": spec["n_tickers"], "n_registry_features": len(ids)}
            if len(ids) < MIN_SECTOR_FEATURES:
                out[sector] = {"status": "skipped",
                               "reason": f"{len(ids)} registry features < {MIN_SECTOR_FEATURES}"}
                continue
            try:
                out[sector] = self._cluster(ids, "clustering_sector", sector, partition)
            except Exception as exc:  # one sector must not stop the others
                log.warning("Sector clustering failed for {s}: {e}", s=sector, e=str(exc))
                out[sector] = {"status": "failed", "reason": str(exc)}
        n_ok = sum(1 for v in out.values() if v.get("status") == "ok")
        return {"status": "ok" if n_ok else "skipped", "sectors_ok": n_ok,
                "sectors": out}

    def feature_importance(self) -> dict[str, Any]:
        from features.importance import FeatureImportanceTracker

        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM model_registry WHERE state = 'PRODUCTION' ORDER BY id LIMIT 1")
            ).fetchone()
        if row is None:
            return {"status": "skipped", "reason": "no PRODUCTION model"}
        tracker = FeatureImportanceTracker(db_engine=self.engine, pit_store=self.pit)
        report = tracker.get_importance_report(int(row[0]), as_of_date=self.as_of, persist=False)
        if "error" in report:
            return {"status": "skipped", "reason": report["error"]}
        summary = report.get("summary") or []
        top = summary[0] if summary else {}
        snap_id = self.save(
            "feature_importance",
            report,
            metrics={
                "n_features": report.get("n_features"),
                "top_feature": top.get("feature_name"),
                "top_composite": top.get("composite_score"),
                "n_measured": sum(1 for s in summary if s.get("composite_score") is not None),
            },
            provenance={"vintage_policy": "LATEST_AS_OF", "feature_importance_log": "not written"},
        )
        return {"status": "ok", "snapshot_id": snap_id, "model_id": int(row[0])}

    def options_scan(self) -> dict[str, Any]:
        payload = summarize_options_scans(self.engine, self.as_of)
        if payload.get("status") != "ok":
            return payload
        snap_id = self.save(
            "options_scan",
            payload,
            metrics={"opportunities": payload["opportunities"], "n_100x": payload["100x"],
                     "scan_date": payload["scan_date"]},
            provenance={"source": "options_mispricing_scans",
                        "writer": "ingestion/scheduler.py options job (OptionsScanner)"},
        )
        return {"status": "ok", "snapshot_id": snap_id, "scan_date": payload["scan_date"],
                "opportunities": payload["opportunities"]}


def summarize_options_scans(engine: Any, as_of: date) -> dict[str, Any]:
    """Summarise the latest persisted options scan on or before ``as_of``.

    Fails closed (status ``skipped``) when there is no scan or the newest
    one is older than ``OPTIONS_SCAN_MAX_AGE_DAYS``.
    """
    with engine.connect() as conn:
        if not conn.execute(text("SELECT to_regclass('options_mispricing_scans') IS NOT NULL")).scalar():
            return {"status": "skipped", "reason": "options_mispricing_scans missing"}
        scan_date = conn.execute(
            text("SELECT MAX(scan_date) FROM options_mispricing_scans WHERE scan_date <= :aod"),
            {"aod": as_of},
        ).scalar()
        if scan_date is None:
            return {"status": "skipped", "reason": "no scan on or before as_of"}
        if (as_of - scan_date).days > OPTIONS_SCAN_MAX_AGE_DAYS:
            return {"status": "skipped", "reason": f"latest scan {scan_date} is stale"}
        rows = conn.execute(
            text(
                "SELECT ticker, direction, score, payoff_multiple, confidence, is_100x, "
                "       spot_price, iv_atm, expiry "
                "FROM options_mispricing_scans WHERE scan_date = :sd "
                "ORDER BY score DESC, ticker, direction"
            ),
            {"sd": scan_date},
        ).fetchall()
    top = [
        {"ticker": r[0], "direction": r[1], "score": r[2], "payoff_multiple": r[3],
         "confidence": r[4], "is_100x": bool(r[5]), "spot_price": r[6], "iv_atm": r[7],
         "expiry": r[8].isoformat() if r[8] is not None else None}
        for r in rows[:OPTIONS_TOP_N]
    ]
    return {
        "status": "ok",
        "scan_date": scan_date.isoformat(),
        "opportunities": len(rows),
        "100x": sum(1 for r in rows if r[5]),
        "top": top,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(
    engine: Any,
    as_of: date,
    steps: tuple[str, ...] = ALL_STEPS,
    dry_run: bool = False,
    skip_gate: bool = False,
    gate_fn: Callable[..., dict[str, Any]] = check_readiness,
) -> dict[str, Any]:
    """Run the gate, then each requested step; returns a summary dict."""
    if skip_gate and not dry_run:
        raise ValueError("--no-readiness-gate is only allowed with --dry-run")
    gate = {"ok": None, "skipped": True} if skip_gate else gate_fn(engine, as_of)
    summary: dict[str, Any] = {"as_of_date": as_of.isoformat(), "gate": gate, "steps": {}}
    if not skip_gate and not gate["ok"]:
        log.warning("Readiness gate not satisfied — no analytics computed: {g}", g=gate)
        summary["status"] = "not_ready"
        return summary

    job = Job(engine, as_of, dry_run, gate)
    for step in steps:
        t0 = time.monotonic()
        log.info("=== {s} ===", s=step)
        try:
            res = getattr(job, step)()
        except Exception as exc:
            log.error("{s} failed: {e}", s=step, e=str(exc))
            res = {"status": "failed", "reason": str(exc)}
        res["seconds"] = round(time.monotonic() - t0, 1)
        summary["steps"][step] = res
        log.info("{s}: {r}", s=step, r=res.get("status"))
    statuses = [r.get("status") for r in summary["steps"].values()]
    summary["status"] = "failed" if "failed" in statuses else "ok"
    return summary


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--as-of", type=date.fromisoformat, default=None,
                   help="as-of date (default: today, UTC)")
    p.add_argument("--steps", default=",".join(ALL_STEPS),
                   help="comma-separated subset of: " + ", ".join(ALL_STEPS))
    p.add_argument("--dry-run", action="store_true", help="compute but write nothing")
    p.add_argument("--check-only", action="store_true", help="evaluate the readiness gate and exit")
    p.add_argument("--no-readiness-gate", action="store_true",
                   help="skip the gate (only together with --dry-run)")
    args = p.parse_args(argv)

    steps = tuple(s.strip() for s in args.steps.split(",") if s.strip())
    unknown = [s for s in steps if s not in ALL_STEPS]
    if unknown:
        p.error(f"unknown steps: {unknown}")
    if args.no_readiness_gate and not args.dry_run:
        p.error("--no-readiness-gate requires --dry-run")

    as_of = args.as_of or datetime.now(timezone.utc).date()
    from db import get_engine

    engine = get_engine()
    if args.check_only:
        gate = check_readiness(engine, as_of)
        print(json.dumps(gate, indent=2, default=str))
        return 0 if gate["ok"] else 3

    summary = run(engine, as_of, steps=steps, dry_run=args.dry_run,
                  skip_gate=args.no_readiness_gate)
    print(json.dumps(summary, indent=2, default=str))
    if summary["status"] == "not_ready":
        return 3
    return 1 if summary["status"] == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
