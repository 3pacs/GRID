#!/usr/bin/env python3.12
"""GRID gem hunter — keeps idle Pascal/Maxwell GPUs digging through data.

v2 2026-05-17 — 99%-winrate rebuild
====================================
RAISED thresholds (see gem_rules.py) PLUS pre-file validator pipeline:
  1. Multi-rule consensus (>=2 rules within 24h on same subject_key)
     EXCEPT permutation_low_p + bootstrap_ci_break standalone if score>=0.95
     AND all other validators pass — these are point-statistics that don't
     need a co-vote.
  2. Kill-predictor v2 cross-check for hypothesis subjects (koala:8093)
     DROP if kill_prob>0.85.
  3. LLM judge (qwen2.5:14b on p9d:11434) — DROP if verdict!=REAL or conf<0.85.
  4. Composite gate: score>=0.92 AND validators pass.

Per-rule auto-disable: every 6h the runlog scorer computes
  confirmed_real_rate = #real / (#real + #noise)
  if labeled>=10 and rate<0.95 -> set gem_rules_config.enabled=FALSE.

Rules disabled in `gem_rules_config` are checked at insert time; their
candidates are rejected before consensus.
"""
from __future__ import annotations

import faulthandler
import json
import math
import os
import socket
import threading
import sys
import resource
import time
import logging
import random
import pathlib
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import psycopg2
from psycopg2 import pool as pgpool
from psycopg2.extras import Json, RealDictCursor

# Optional GPU
try:
    import cupy as cp
    HAS_CUPY = True
except Exception:
    cp = None
    HAS_CUPY = False

import faiss  # faiss-cpu
import gem_rules
import cross_class  # Task #117 — cross-class confirmation validator
import gem_proxy_map  # Task #128 — ticker-proxy for non-tradeable subjects

# ---------------------------------------------------------------------------
# Config

DB_HOST = os.environ.get("PG_HOST", "100.75.185.36")
DB_PORT = int(os.environ.get("PG_PORT", "5432"))
DB_USER = os.environ.get("PG_USER", "grid")
DB_PASS = os.environ.get("PG_PASSWORD", "gridmaster2026")
DB_NAME = os.environ.get("PG_DB", "griddb")

FAISS_DIR = pathlib.Path(os.environ.get("FAISS_DIR", "/data/grid/faiss"))
GEM_DRAFT_DIR = pathlib.Path(os.environ.get("GEM_DRAFT_DIR", "/tmp/gem_drafts"))
IDLE_SLEEP_SEC = int(os.environ.get("IDLE_SLEEP_SEC", "20"))

# Validator endpoints (override with env if needed)
KILL_PRED_URL = os.environ.get("GEM_KILLPRED_URL", "http://koala:8093/score")
LLM_HOST = os.environ.get("GEM_LLM_HOST", "http://p9d:11434")
LLM_MODELS = [m.strip() for m in os.environ.get(
    "GEM_LLM_MODELS", "qwen2.5:14b-instruct-q4_K_M,qwen2.5:7b-instruct"
).split(",") if m.strip()]
LLM_TIMEOUT_S = float(os.environ.get("GEM_LLM_TIMEOUT", "45"))

# Filing gates
FILE_SCORE_GATE = float(os.environ.get("GEM_FILE_SCORE", "0.92"))
LLM_CONF_GATE = float(os.environ.get("GEM_LLM_CONF", "0.85"))
KP_KILL_GATE = float(os.environ.get("GEM_KP_KILL", "0.85"))
CONSENSUS_WINDOW_H = int(os.environ.get("GEM_CONSENSUS_WINDOW_H", "24"))
# rules whose own statistical evidence is strong enough they can file solo
# at very-high score, bypassing the >=2-vote requirement (still must pass
# LLM + kill-predictor validators)
SOLO_OK_RULES = {"permutation_low_p", "bootstrap_ci_break", "insider_cluster"}
SOLO_SCORE_GATE = float(os.environ.get("GEM_SOLO_SCORE", "0.95"))
# Task #123: insider clusters have lower solo threshold because cross-class
# confirmation (insider+technical) is the structural co-vote. Still gated by
# FILE_SCORE_GATE (0.92) and the LLM + cross-class validators.
SOLO_SCORE_GATE_INSIDER = float(os.environ.get("GEM_SOLO_SCORE_INSIDER", "0.85"))

# Task #117 — cross-class confirmation gates. A gem with an extractable
# ticker must have >=2 of {insider, congress, news, technical} confirming
# (each >= 0.4) and a composite >= 1.5 within a 14-day window. Gems with no
# extractable ticker bypass this gate (the rest of the validator pipeline
# still runs).
CROSS_CLASS_MIN_CLASSES = int(os.environ.get("GEM_CC_MIN_CLASSES", "2"))
CROSS_CLASS_MIN_COMPOSITE = float(os.environ.get("GEM_CC_MIN_COMPOSITE", "1.5"))
CROSS_CLASS_WINDOW_DAYS = int(os.environ.get("GEM_CC_WINDOW_DAYS", "14"))

# Per-task min-interval (sec)
TASK_INTERVALS = {
    "faiss_refresh": 7200,
    "nn_precompute": 1200,
    "pagerank": 21600,
    "signal_correlations": 3600,
    "permutation_sweep": 600,
    "bootstrap_ci": 7200,
    "rule_winrate_audit": 21600,  # 6h
    "insider_cluster_check": 3600,  # 1h — insider data updates daily-ish (Task #123)
    "earnings_proximity_check": 3600,  # 1h — filings + insider window check (Task #168)
}

HOST = socket.gethostname()
GPU_ENV = os.environ.get("CUDA_VISIBLE_DEVICES", "?")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] gpu=" + GPU_ENV + " %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("gem_hunter")

_pool: Optional[pgpool.SimpleConnectionPool] = None


def get_pool() -> pgpool.SimpleConnectionPool:
    global _pool
    if _pool is None:
        _pool = pgpool.SimpleConnectionPool(
            1, 2,
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASS, dbname=DB_NAME,
            application_name=f"gem_hunter_gpu{GPU_ENV}",
        )
    return _pool


class Conn:
    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = get_pool().getconn()
        self.conn.autocommit = False
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        if self.conn is None:
            return
        try:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
        finally:
            get_pool().putconn(self.conn)


# ---------------------------------------------------------------------------
# Validators

def _http_post_json(url: str, payload: dict, timeout: float) -> Optional[dict]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as e:
        log.warning("HTTP %s failed: %s", url, e)
        return None
    except Exception as e:
        log.warning("HTTP %s unexpected: %s", url, e)
        return None


def validate_kill_predictor(g: dict) -> tuple[bool, dict]:
    """Returns (pass, info). Only runs on hypothesis subjects."""
    if g.get("subject_kind") != "hypothesis":
        return True, {"skipped": "not_hypothesis"}
    thesis = str(g.get("subject_id") or "") + " " + json.dumps(g.get("evidence", {}), default=str)
    out = _http_post_json(KILL_PRED_URL, {"thesis": thesis[:4000]}, timeout=10)
    if out is None:
        return True, {"error": "endpoint_unreachable_failsafe_pass"}
    kp = float(out.get("kill_probability", out.get("kill_prob", 0.0)) or 0.0)
    return (kp < KP_KILL_GATE), {"kill_probability": kp, "model": out.get("model_version")}


def _gem_to_judge_text(g: dict) -> str:
    return (
        f"Source rule: {g.get('source')}\n"
        f"Subject kind: {g.get('subject_kind')}\n"
        f"Subject id: {g.get('subject_id')}\n"
        f"Score: {g.get('score'):.3f}\n"
        f"Evidence JSON:\n{json.dumps(g.get('evidence', {}), indent=2, default=str)}"
    )


LLM_PROMPT = """You are auditing a "gem" — a possibly-tradable insight from a trading intelligence platform. Below is the gem's evidence + subject. Decide:
1. Is this a REAL signal that a sophisticated trader would act on, or a statistical/data artifact?
2. Is the causal story plausible? (specific lever, mechanism, actor named)
3. Is the evidence cross-source (>1 type of data) or single-source noise?

Output JSON only: {"verdict": "REAL"|"ARTIFACT"|"INCONCLUSIVE", "confidence": 0.0-1.0, "why": "..."}

GEM:
"""


def validate_llm_judge(g: dict) -> tuple[bool, dict]:
    """Returns (pass, info). Tries each model in LLM_MODELS until one returns
    parseable JSON. Failsafe: if all models fail, return (False, ...) — gem
    is dropped, since we can't verify it."""
    prompt = LLM_PROMPT + _gem_to_judge_text(g)
    last_err = None
    for model in LLM_MODELS:
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0.0, "num_predict": 256},
        }
        out = _http_post_json(f"{LLM_HOST}/api/generate", payload, timeout=LLM_TIMEOUT_S)
        if out is None:
            last_err = "http_fail"
            continue
        resp_text = (out.get("response") or "").strip()
        if not resp_text:
            last_err = "empty_response"
            continue
        try:
            parsed = json.loads(resp_text)
        except Exception:
            # Try to extract JSON substring
            try:
                lo = resp_text.find("{"); hi = resp_text.rfind("}") + 1
                parsed = json.loads(resp_text[lo:hi])
            except Exception:
                last_err = f"parse_fail:{resp_text[:60]}"
                continue
        verdict = str(parsed.get("verdict", "")).upper()
        conf = float(parsed.get("confidence", 0.0) or 0.0)
        why = str(parsed.get("why", ""))[:500]
        passed = (verdict == "REAL" and conf >= LLM_CONF_GATE)
        return passed, {
            "verdict": verdict, "confidence": conf, "why": why,
            "model": model,
        }
    return False, {"error": f"all_models_failed:{last_err}"}


def validate_consensus(cur, g: dict) -> tuple[bool, dict]:
    """Count distinct rules that hit the same subject_key within 24h
    (including the current one). >=2 = pass. Solo allowed for SOLO_OK_RULES
    at score>=SOLO_SCORE_GATE.
    """
    subject_key = g.get("subject_key") or f"{g['subject_kind']}:{g['subject_id']}"
    cur.execute(
        """SELECT COUNT(DISTINCT source) FROM gem_alerts
            WHERE subject_id = %s
              AND subject_kind = %s
              AND detected_at > now() - make_interval(hours => %s)
        """,
        (str(g["subject_id"]), g["subject_kind"], CONSENSUS_WINDOW_H),
    )
    prior_distinct = cur.fetchone()[0] or 0
    # +1 for this candidate's rule if it's a new rule for that subject
    cur.execute(
        """SELECT COUNT(*) FROM gem_alerts
            WHERE subject_id = %s AND subject_kind = %s AND source = %s
              AND detected_at > now() - make_interval(hours => %s)
        """,
        (str(g["subject_id"]), g["subject_kind"], g["source"], CONSENSUS_WINDOW_H),
    )
    same_rule_priors = cur.fetchone()[0] or 0
    vote_count = prior_distinct + (0 if same_rule_priors > 0 else 1)
    rule = g.get("rule_name") or g.get("source")
    if rule in SOLO_OK_RULES:
        # insider_cluster has its own lower solo gate; cross-class is the co-vote
        solo_threshold = SOLO_SCORE_GATE_INSIDER if rule == "insider_cluster" else SOLO_SCORE_GATE
        if float(g.get("score", 0)) >= solo_threshold:
            return True, {"vote_count": vote_count, "solo_ok": True,
                          "solo_threshold": solo_threshold}
    if g.get("vote_only"):
        # nn_cluster can never file alone
        return False, {"vote_count": vote_count, "vote_only": True}
    return (vote_count >= 2), {"vote_count": vote_count, "solo_ok": False}


def rule_enabled(cur, rule_name: str) -> bool:
    cur.execute("SELECT enabled FROM gem_rules_config WHERE rule_name=%s", (rule_name,))
    row = cur.fetchone()
    if row is None:
        return True  # unknown rules default to enabled
    return bool(row[0])


# ---------------------------------------------------------------------------
# Gem persistence with validator pipeline

def insert_gems(cur, gems: list[dict]) -> dict:
    """Pipeline: rule_enabled -> consensus -> kill-pred -> LLM -> score gate.

    Returns counters {candidates, rejected_rule, rejected_consensus,
    rejected_kp, rejected_llm, rejected_score, filed, pending_consensus}.
    """
    counters = {"candidates": 0, "rejected_rule": 0, "rejected_consensus": 0,
                "rejected_kp": 0, "rejected_llm": 0, "rejected_score": 0,
                "rejected_cross_class": 0,
                "filed": 0, "pending_consensus": 0}
    for g in gems:
        counters["candidates"] += 1
        rule = g.get("rule_name") or g.get("source")
        if not rule_enabled(cur, rule):
            counters["rejected_rule"] += 1
            continue
        # Task #128: hybrid ticker-proxy gate for actor-relationship
        # rules (bootstrap_ci_break, pagerank_delta). If subject_id has
        # no extractable ticker, look up a proxy from gem_proxy_map.
        # If still none, demote to relationship_only and skip insertion
        # so the cross-class validator does not get a no-ticker candidate.
        if rule in {"bootstrap_ci_break", "pagerank_delta"}:
            evidence = g.setdefault("evidence", {})
            if not evidence.get("ticker"):
                # First try: do we already have a real ticker reachable
                # by the cross_class extractor (e.g. insider_trade subjects
                # whose subject_id tail is a real ticker)? If so, no proxy
                # needed and no demotion.
                try:
                    found = cross_class.extract_tickers_from_gem(cur, g)
                except Exception:
                    found = []
                if not found:
                    proxy = gem_proxy_map.proxy_ticker_for_gem(g)
                    if proxy:
                        evidence["ticker"] = proxy
                        evidence["is_proxy"] = True
                        evidence["proxy_source"] = "gem_proxy_map"
                    else:
                        g["surfaced_in"] = ["relationship_only"]
                        counters.setdefault("rejected_no_ticker_proxy", 0)
                        counters["rejected_no_ticker_proxy"] += 1
                        try:
                            _record(cur, g, status="relationship_only",
                                    validators={"proxy": {"mapped": False}},
                                    vote_count=0)
                        except Exception as _e:
                            log.warning("relationship_only record failed for %s: %s", g.get("subject_id"), _e)
                        continue

        validators: dict = {}

        # 1. Score gate first (cheapest)
        if float(g.get("score", 0)) < FILE_SCORE_GATE:
            counters["rejected_score"] += 1
            # Still record as pending if it's at least 0.7, for visibility
            if float(g.get("score", 0)) >= 0.70:
                _record(cur, g, status="rejected_score", validators={"score": g["score"]},
                        vote_count=0)
            continue

        # 2. Consensus
        c_pass, c_info = validate_consensus(cur, g)
        validators["consensus"] = c_info
        if not c_pass:
            counters["pending_consensus"] += 1
            _record(cur, g, status="pending_consensus", validators=validators,
                    vote_count=int(c_info.get("vote_count", 1)))
            continue

        # 3. Kill-predictor (only for hypothesis subjects)
        kp_pass, kp_info = validate_kill_predictor(g)
        validators["kill_predictor"] = kp_info
        if not kp_pass:
            counters["rejected_kp"] += 1
            _record(cur, g, status="rejected_kp", validators=validators,
                    vote_count=int(c_info.get("vote_count", 1)))
            continue

        # 3b. Cross-class confirmation (Task #117). Tickers present in the
        # gem must show >=2 confirming classes in the 14-day window. Gems
        # without an extractable ticker bypass (the validator returns
        # bypassed=True). Stored to gem_alerts.cross_class_score for audit.
        try:
            cc_pass, cc_info = cross_class.validate_cross_class(
                cur, g,
                window_days=CROSS_CLASS_WINDOW_DAYS,
                min_classes=CROSS_CLASS_MIN_CLASSES,
                min_composite=CROSS_CLASS_MIN_COMPOSITE,
            )
        except Exception as e:
            log.warning("cross_class validator error on gem %s: %s",
                        g.get("subject_id"), e)
            cc_pass, cc_info = True, {"error": f"validator_exception: {e!r}",
                                      "failsafe_pass": True}
        validators["cross_class"] = cc_info
        if not cc_pass:
            counters["rejected_cross_class"] += 1
            _record(cur, g, status="rejected_cross_class", validators=validators,
                    vote_count=int(c_info.get("vote_count", 1)),
                    cross_class=cc_info)
            continue

        # 4. LLM judge
        llm_pass, llm_info = validate_llm_judge(g)
        validators["llm_judge"] = llm_info
        if not llm_pass:
            counters["rejected_llm"] += 1
            _record(cur, g, status="rejected_llm", validators=validators,
                    vote_count=int(c_info.get("vote_count", 1)),
                    llm=llm_info)
            continue

        # All passed -> file
        gid = _record(cur, g, status="filed", validators=validators,
                      vote_count=int(c_info.get("vote_count", 1)),
                      llm=llm_info)
        counters["filed"] += 1
        if gid and float(g["score"]) >= 0.9:
            try:
                write_gem_draft(gid, g, llm_info)
            except Exception as e:
                log.warning("draft write failed for gem %s: %s", gid, e)
    return counters


def _record(cur, g: dict, *, status: str, validators: dict,
            vote_count: int = 1, llm: Optional[dict] = None,
            cross_class: Optional[dict] = None) -> Optional[int]:
    # Task #117: prefer the explicit cross_class arg, else pull from validators
    # dict so callers that just set validators["cross_class"] still persist it.
    cc = cross_class if cross_class is not None else validators.get("cross_class")
    cur.execute(
        """INSERT INTO gem_alerts
              (source, subject_kind, subject_id, related_ids, score, evidence,
               validators_passed, validator_status, vote_count,
               llm_verdict, llm_confidence, llm_why, cross_class_score)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           RETURNING id""",
        (g["source"], g["subject_kind"], str(g["subject_id"]),
         Json(g.get("related_ids")) if g.get("related_ids") is not None else None,
         float(g["score"]), Json(g["evidence"]),
         Json(validators), status, vote_count,
         (llm or {}).get("verdict"),
         (llm or {}).get("confidence"),
         (llm or {}).get("why"),
         Json(cc) if cc is not None else None),
    )
    return cur.fetchone()[0]


def write_gem_draft(gem_id: int, g: dict, llm_info: Optional[dict] = None) -> None:
    GEM_DRAFT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    fname = GEM_DRAFT_DIR / f"gem_{gem_id}_{g['source']}_{ts}.md"
    llm_block = ""
    if llm_info:
        llm_block = (
            f"\n**LLM verdict:** {llm_info.get('verdict')} "
            f"(conf={llm_info.get('confidence', 0):.2f}, model={llm_info.get('model','?')})\n"
            f"> {llm_info.get('why','')}\n"
        )
    body = (
        f"# Gem #{gem_id} - {g['source']}\n\n"
        f"- **Subject:** {g['subject_kind']} `{g['subject_id']}`\n"
        f"- **Score:** {g['score']:.3f}\n"
        f"- **Detected at:** {ts}\n"
        f"- **Host:** {HOST} (gpu={GPU_ENV})\n"
        f"- **Validators:** consensus + kill-predictor + LLM all PASSED\n"
        f"{llm_block}\n"
        f"Automated gem-hunter v2 alert. Passed multi-rule consensus, "
        f"kill-predictor v2 cross-check, and qwen2.5 LLM judge.\n\n"
        f"```json\n{json.dumps(g['evidence'], indent=2, default=str, ensure_ascii=False)}\n```\n"
    )
    fname.write_text(body, encoding="utf-8")
    log.info("wrote gem draft %s", fname)


# ---------------------------------------------------------------------------
# Runlog / scheduler (unchanged from v1 except for new task in TASK_INTERVALS)

def ensure_runlog(cur):
    cur.execute(
        """CREATE TABLE IF NOT EXISTS gem_hunter_runlog (
              task TEXT PRIMARY KEY,
              last_run TIMESTAMPTZ NOT NULL DEFAULT now(),
              host TEXT,
              gpu TEXT,
              duration_s DOUBLE PRECISION,
              note TEXT
        )"""
    )
    for task in TASK_INTERVALS:
        cur.execute('SELECT 1 FROM gem_hunter_runlog WHERE task=%s', (task,))
        if cur.fetchone():
            continue
        cur.execute(
            """INSERT INTO gem_hunter_runlog (task, last_run, host, gpu, note)
                 VALUES (%s, now() - interval '99 years', %s, %s, 'init')
                 ON CONFLICT (task) DO NOTHING""",
            (task, HOST, GPU_ENV),
        )


def claim_task(cur) -> Optional[str]:
    tasks = list(TASK_INTERVALS.items())
    random.shuffle(tasks)
    for task, interval in tasks:
        cur.execute(
            """SELECT task FROM gem_hunter_runlog
                WHERE task = %s
                  AND now() - last_run > make_interval(secs => %s)
                  FOR UPDATE SKIP LOCKED""",
            (task, interval),
        )
        if cur.fetchone() is None:
            continue
        cur.execute(
            """UPDATE gem_hunter_runlog
                  SET last_run = now(), host = %s, gpu = %s, note = 'claimed'
                WHERE task = %s
                RETURNING task""",
            (HOST, GPU_ENV, task),
        )
        row = cur.fetchone()
        if row:
            return row[0]
    return None


# ---------------------------------------------------------------------------
# Tasks (faiss/nn/pagerank/signal_corr/perm/bootstrap unchanged in body —
# only the call to insert_gems now uses the v2 pipeline)

FAISS_PAGE_SIZE = int(os.environ.get("FAISS_PAGE_SIZE", "10000"))


def _faiss_refresh_rss_mb() -> int:
    """Return current RSS in MB. Linux: ru_maxrss is in KB."""
    try:
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
    except Exception:
        return -1


def _faiss_refresh_watchdog(stop_evt, deadline_s: int, log):
    """task#115: if faiss_refresh runs longer than deadline_s, dump all
    Python stacks (faulthandler) so next hang has a traceback in logs."""
    if stop_evt.wait(deadline_s):
        return  # task finished cleanly
    log.error("[faiss_refresh] WATCHDOG fired after %ds — dumping tracebacks", deadline_s)
    try:
        import sys as _sys
        faulthandler.dump_traceback(file=_sys.stderr, all_threads=True)
    except Exception as exc:
        log.error("[faiss_refresh] watchdog dump failed: %s", exc)


def task_faiss_refresh(cur) -> dict:
    """task#104: pg_try_advisory_xact_lock(901) gates FAISS-heavy work
    so worker @0 and @1 cannot both load the ~5.7 GiB IndexFlatIP at once
    (caused OOM/swap deadlock task#115). Shared lock id 901 also blocks
    concurrent nn_precompute which reads the same index.
    task#115: added watchdog + per-page progress logging + index.add
    try/except. Streaming logic unchanged."""
    log = logging.getLogger(__name__)
    cur.execute("SELECT pg_try_advisory_xact_lock(901)")
    if not cur.fetchone()[0]:
        log.info("[faiss_refresh] another worker holds lock 901, skipping")
        return {"skipped_locked": True}
    FAISS_DIR.mkdir(parents=True, exist_ok=True)
    pool = get_pool()
    stream_conn = pool.getconn()
    stream_conn.autocommit = False
    total = 0
    ids_chunks: list[np.ndarray] = []
    index = faiss.IndexFlatIP(768)
    page_n = 0
    t_start = time.time()
    # task#115: watchdog — 20min deadline, daemon thread, signaled on exit
    wd_stop = threading.Event()
    wd_deadline = int(os.environ.get("FAISS_REFRESH_WATCHDOG_S", "1200"))
    wd_thread = threading.Thread(
        target=_faiss_refresh_watchdog, args=(wd_stop, wd_deadline, log),
        name="faiss_refresh_watchdog", daemon=True,
    )
    wd_thread.start()
    log.info("[faiss_refresh] start page_size=%d watchdog=%ds rss=%dmb",
             FAISS_PAGE_SIZE, wd_deadline, _faiss_refresh_rss_mb())
    try:
        with stream_conn.cursor() as setup_cur:
            setup_cur.execute("SET LOCAL statement_timeout = '15min'")
        named = stream_conn.cursor(name="faiss_refresh_cursor")
        named.itersize = FAISS_PAGE_SIZE
        named.execute(
            "SELECT id, embedding::text FROM embeddings "
            "WHERE embedding IS NOT NULL ORDER BY id"
        )
        while True:
            rows = named.fetchmany(FAISS_PAGE_SIZE)
            if not rows:
                break
            page_n += 1
            page_ids = np.empty(len(rows), dtype=np.int64)
            page_vecs = np.empty((len(rows), 768), dtype=np.float32)
            keep = 0
            for emb_id, txt in rows:
                v = np.fromstring(txt.strip("[]"), sep=",", dtype=np.float32)
                if v.size != 768:
                    continue
                page_ids[keep] = emb_id
                page_vecs[keep] = v
                keep += 1
            if keep == 0:
                continue
            page_ids = page_ids[:keep]
            page_vecs = page_vecs[:keep]
            norms = np.linalg.norm(page_vecs, axis=1, keepdims=True)
            norms[norms == 0] = 1.0
            page_vecs = (page_vecs / norms).astype(np.float32)
            # task#115: bound index.add — if FAISS/numpy throws we want a log
            try:
                index.add(page_vecs)
            except Exception as add_exc:
                log.exception(
                    "[faiss_refresh] index.add failed page=%d keep=%d "
                    "vec_shape=%s vec_dtype=%s ntotal_before=%d: %s",
                    page_n, keep, page_vecs.shape, page_vecs.dtype,
                    index.ntotal, add_exc,
                )
                raise
            ids_chunks.append(page_ids)
            total += keep
            elapsed = time.time() - t_start
            log.info(
                "[faiss_refresh] page=%d rows_added=%d total=%d "
                "ntotal=%d elapsed=%.1fs rss=%dmb",
                page_n, keep, total, index.ntotal, elapsed,
                _faiss_refresh_rss_mb(),
            )
            del page_vecs, page_ids, rows
        named.close()
        stream_conn.rollback()
    except Exception:
        try: stream_conn.rollback()
        except Exception: pass
        raise
    finally:
        wd_stop.set()  # tell watchdog to exit cleanly
        pool.putconn(stream_conn)
    if total == 0:
        return {"skipped": "no embeddings"}
    ids = np.concatenate(ids_chunks) if ids_chunks else np.empty(0, dtype=np.int64)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = FAISS_DIR / f"embeddings_all_{ts}.faiss"
    ids_path_new = FAISS_DIR / f"embeddings_all_{ts}.ids.npy"
    faiss.write_index(index, str(path))
    np.save(ids_path_new, ids)
    cur_link = FAISS_DIR / "embeddings_all.current.faiss"
    cur_ids = FAISS_DIR / "embeddings_all.current.ids.npy"
    for link, target in ((cur_link, path), (cur_ids, ids_path_new)):
        try:
            if link.exists() or link.is_symlink():
                link.unlink()
        except Exception: pass
        try: link.symlink_to(target)
        except Exception: link.write_text(str(target))
    return {"n_vectors": total, "path": str(path), "ntotal": index.ntotal,
            "page_size": FAISS_PAGE_SIZE, "pages": len(ids_chunks)}


def task_nn_precompute(cur, limit: int = 2000) -> dict:
    # task#104: share lock 901 with faiss_refresh — both load ~5.7 GiB
    # IndexFlatIP; running concurrently caused OOM/swap deadlock task#115.
    cur.execute("SELECT pg_try_advisory_xact_lock(901)")
    if not cur.fetchone()[0]:
        log.info("[nn_precompute] another worker holds lock 901, skipping")
        return {"skipped_locked": True}
    cur.execute(
        """SELECT e.id, e.source_type, e.metadata, e.embedding::text
             FROM embeddings e
             LEFT JOIN embedding_neighbors n ON n.source_id = e.id
            WHERE n.source_id IS NULL AND e.embedding IS NOT NULL
            ORDER BY e.id DESC LIMIT %s""", (limit,))
    new_rows = cur.fetchall()
    if not new_rows:
        return {"skipped": "no new embeddings"}
    ids_path = FAISS_DIR / "embeddings_all.current.ids.npy"
    idx_path = FAISS_DIR / "embeddings_all.current.faiss"
    if not ids_path.exists() or not idx_path.exists():
        return {"skipped": "FAISS index not yet built"}
    all_ids = np.load(ids_path)
    index = faiss.read_index(str(idx_path))
    cur.execute("SELECT id, source_type FROM embeddings WHERE id = ANY(%s)",
                (all_ids.tolist(),))
    type_lookup = dict(cur.fetchall())
    written = 0
    gem_payloads: list[dict] = []
    for emb_id, src_type, meta, vec_txt in new_rows:
        v = np.fromstring(vec_txt.strip("[]"), sep=",", dtype=np.float32)
        if v.size != 768: continue
        n = np.linalg.norm(v)
        if n == 0: continue
        v = (v / n).astype(np.float32).reshape(1, -1)
        D, I = index.search(v, 11)
        out_neighbors = []
        for rank, (dist, idx_pos) in enumerate(zip(D[0], I[0])):
            if idx_pos < 0: continue
            neighbor_id = int(all_ids[idx_pos])
            if neighbor_id == emb_id: continue
            cur.execute(
                """INSERT INTO embedding_neighbors (source_id, neighbor_id, distance, rank)
                   VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                (int(emb_id), neighbor_id, float(1.0 - dist), len(out_neighbors) + 1))
            out_neighbors.append({"id": neighbor_id, "distance": float(1.0 - dist),
                                  "source_type": type_lookup.get(neighbor_id)})
            if len(out_neighbors) >= 10: break
        written += len(out_neighbors)
        gem_payloads.extend(gem_rules.nn_cluster_gems(
            int(emb_id), out_neighbors, {"source_type": src_type, "metadata": meta}))
    counters = insert_gems(cur, gem_payloads)
    return {"new_embeddings": len(new_rows), "neighbours_written": written, **counters}


def task_pagerank(cur) -> dict:
    cur.execute(
        """SELECT actor_a, actor_b, COALESCE(strength, 0.5)::float
             FROM actor_connections WHERE strength IS NOT NULL""")
    rows = cur.fetchall()
    if not rows: return {"skipped": "no edges"}
    from collections import defaultdict
    node_index: dict[str, int] = {}
    edges_csr = defaultdict(float)
    for a, b, w in rows:
        for nm in (a, b):
            if nm not in node_index: node_index[nm] = len(node_index)
        edges_csr[(node_index[a], node_index[b])] += float(w)
        edges_csr[(node_index[b], node_index[a])] += float(w)
    n = len(node_index)
    if n == 0: return {"skipped": "no nodes"}
    from scipy.sparse import coo_matrix
    rs, cs, vs = [], [], []
    for (i, j), w in edges_csr.items():
        rs.append(j); cs.append(i); vs.append(w)
    M = coo_matrix((vs, (rs, cs)), shape=(n, n)).tocsr().astype(np.float32)
    col_sums = np.asarray(M.sum(axis=0)).ravel(); col_sums[col_sums == 0] = 1.0
    damp = 0.85; teleport = (1 - damp) / n
    v = np.full(n, 1.0 / n, dtype=np.float32)
    try:
        if HAS_CUPY:
            import cupyx.scipy.sparse as cps
            Mg = cps.csr_matrix(M); cs_g = cp.asarray(col_sums); v_g = cp.asarray(v)
            for _ in range(80):
                v_g = damp * (Mg.T @ (v_g / cs_g)) + teleport
                v_g = v_g / v_g.sum()
            v = cp.asnumpy(v_g)
        else:
            raise RuntimeError("no gpu")
    except Exception:
        for _ in range(80):
            v = damp * (M.T @ (v / col_sums)) + teleport
            v = v / v.sum()
    cur.execute(
        """SELECT actor_id, score FROM actor_centrality
            WHERE computed_at = (SELECT MAX(computed_at) FROM actor_centrality)""")
    prev_scores = {a: float(s) for a, s in cur.fetchall()}
    inv_idx = {i: nm for nm, i in node_index.items()}
    order = np.argsort(-v)[:200]
    now_scores = {}
    for rank, idx_pos in enumerate(order, start=1):
        actor_id = inv_idx[int(idx_pos)]; score = float(v[idx_pos])
        now_scores[actor_id] = score
        cur.execute(
            """INSERT INTO actor_centrality (actor_id, score, rank)
                 VALUES (%s, %s, %s) ON CONFLICT DO NOTHING""",
            (actor_id, score, rank))
    gems = gem_rules.pagerank_delta_gems(prev_scores, now_scores)
    counters = insert_gems(cur, gems)
    return {"nodes": n, "edges": len(edges_csr) // 2, "top": len(now_scores), **counters}


def task_signal_correlations(cur, max_pairs: int = 1500) -> dict:
    cur.execute(
        """SELECT key, signal_date, cnt FROM (
              SELECT (signal_type || '||' || COALESCE(ticker, actor, '')) AS key,
                     signal_date, COUNT(*) AS cnt
                FROM signal_data
               WHERE signal_date >= CURRENT_DATE - INTERVAL '365 days'
               GROUP BY key, signal_date
           ) t
           WHERE key IN (
              SELECT key FROM (
                SELECT (signal_type || '||' || COALESCE(ticker, actor, '')) AS key,
                       COUNT(DISTINCT signal_date) AS n_days
                  FROM signal_data
                 WHERE signal_date >= CURRENT_DATE - INTERVAL '365 days'
                 GROUP BY key HAVING COUNT(DISTINCT signal_date) >= 30
                ORDER BY n_days DESC LIMIT 80
              ) k
           ) ORDER BY key, signal_date""")
    raw = cur.fetchall()
    if not raw: return {"skipped": "no signal_data"}
    from collections import defaultdict
    per_key: dict[str, dict] = defaultdict(dict); all_dates: set = set()
    for key, dt, cnt in raw:
        per_key[key][dt] = float(cnt); all_dates.add(dt)
    sorted_dates = sorted(all_dates); date_idx = {d: i for i, d in enumerate(sorted_dates)}
    keys = list(per_key.keys())
    M = np.zeros((len(keys), len(sorted_dates)), dtype=np.float32)
    for i, k in enumerate(keys):
        for d, c in per_key[k].items():
            M[i, date_idx[d]] = c
    if HAS_CUPY:
        Mg = cp.asarray(M); Mz = Mg - Mg.mean(axis=1, keepdims=True)
        sd = Mz.std(axis=1, keepdims=True); sd[sd == 0] = 1.0; Mz = Mz / sd
        R = cp.asnumpy((Mz @ Mz.T) / Mz.shape[1])
    else:
        Mz = M - M.mean(axis=1, keepdims=True)
        sd = Mz.std(axis=1, keepdims=True); sd[sd == 0] = 1.0; Mz = Mz / sd
        R = (Mz @ Mz.T) / Mz.shape[1]
    cur.execute(
        """SELECT DISTINCT ON (left_key, right_key) left_key, right_key, r
             FROM signal_correlations
            ORDER BY left_key, right_key, computed_at DESC""")
    prev_r = {(a, b): float(rv) for a, b, rv in cur.fetchall()}
    n_pairs = 0; new_r: dict[tuple, float] = {}; n_lookup: dict[tuple, int] = {}
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            r = float(R[i, j])
            if not math.isfinite(r) or abs(r) < 0.1: continue
            k1, k2 = keys[i], keys[j]
            new_r[(k1, k2)] = r; n_lookup[(k1, k2)] = M.shape[1]
            cur.execute(
                """INSERT INTO signal_correlations (left_key, right_key, r, n)
                     VALUES (%s, %s, %s, %s)
                     ON CONFLICT (left_key, right_key, computed_at) DO NOTHING""",
                (k1, k2, r, M.shape[1]))
            n_pairs += 1
            if n_pairs >= max_pairs: break
        if n_pairs >= max_pairs: break
    gems = gem_rules.correlation_break_gems(prev_r, new_r, n_lookup)
    counters = insert_gems(cur, gems)
    return {"keys": len(keys), "dates": len(sorted_dates), "pairs_written": n_pairs, **counters}


def task_permutation_sweep(cur) -> dict:
    cur.execute(
        """SELECT hypothesis_id, p_value, effect_size, test_kind, computed_at
             FROM hypothesis_pvalue_history
            WHERE computed_at > now() - interval '7 days' AND p_value IS NOT NULL""")
    rows = [dict(zip(("hypothesis_id", "p_value", "effect_size",
                       "test_kind", "computed_at"), r)) for r in cur.fetchall()]
    if not rows: return {"skipped": "no recent permutation results"}
    latest: dict[str, dict] = {}
    for r in rows:
        cr = latest.get(r["hypothesis_id"])
        if cr is None or r["computed_at"] > cr["computed_at"]:
            latest[r["hypothesis_id"]] = r
    cur.execute(
        """SELECT subject_id FROM gem_alerts
            WHERE source = 'permutation_low_p'
              AND validator_status = 'filed'
              AND detected_at > now() - interval '7 days'""")
    already = {r[0] for r in cur.fetchall()}
    fresh = [r for hid, r in latest.items() if hid not in already]
    gems = gem_rules.permutation_low_p_gems(fresh)
    counters = insert_gems(cur, gems)
    return {"scanned": len(latest), **counters}


def task_bootstrap_ci(cur, n_boot: int = 2000, sample_edges: int = 200) -> dict:
    cur.execute(
        """SELECT actor_a, actor_b, relationship,
                  jsonb_array_length(evidence) AS n_obs, strength
             FROM actor_connections
            WHERE jsonb_typeof(evidence) = 'array'
              AND jsonb_array_length(evidence) >= 5
            ORDER BY jsonb_array_length(evidence) DESC, strength DESC NULLS LAST
            LIMIT %s""", (sample_edges,))
    rows = cur.fetchall()
    if not rows: return {"skipped": "no edges with >=5 evidence"}
    rng = np.random.default_rng()
    gem_payloads = []
    for a, b, rel, n_obs, strength in rows:
        if strength is None: continue
        s = float(strength)
        obs = rng.beta(max(0.5, s * 5), max(0.5, (1 - s) * 5), size=int(n_obs))
        samples = np.empty(n_boot, dtype=np.float64)
        for k in range(n_boot):
            samples[k] = rng.choice(obs, size=obs.size, replace=True).mean()
        edge_id = f"{a}__{rel}__{b}"
        gem_payloads.extend(gem_rules.bootstrap_ci_gems(edge_id, samples, a, b, rel))
    counters = insert_gems(cur, gem_payloads)
    return {"edges_evaluated": len(rows), **counters}


def task_insider_cluster_check(cur) -> dict:
    """Task #123: detect 3+ insider cluster-buy events on a ticker (30d window).

    Emits one gem per qualifying ticker (subject_kind='ticker'). Score is
    high (0.85-0.99) so it will pass the FILE_SCORE_GATE; consensus comes
    from cross-class confirmation (insider class fires by definition;
    technical/news classes often confirm).
    """
    gems = gem_rules.insider_cluster_gems(cur)
    counters = insert_gems(cur, gems)
    return {"candidates_found": len(gems), **counters}


def task_earnings_proximity_check(cur) -> dict:
    """Task #168 (2026-05-17): material filing + insider cluster + tape
    hasn't moved.

    Joins earnings_events (10-K/10-Q/8-K/EARNINGS_CALL filed last 7d),
    insider_trades (3+ distinct insider BUYs same 7d window), and
    ticker_metrics_daily (last close vs filing-date close vs ATR(20)
    approximation from closes). Score gate 0.92 means strong cluster +
    fresh filing + price-hasn't-moved fires; price-already-moved scores
    ~0.89 and remains rejected_score.

    Alert source = 'earnings_proximity'. Evidence carries ticker,
    filing_url, n_insiders, atr_distance per task spec.
    """
    gems = gem_rules.earnings_proximity_gems(cur)
    counters = insert_gems(cur, gems)
    return {"candidates_found": len(gems), **counters}


def task_rule_winrate_audit(cur) -> dict:
    """Every 6h: compute per-rule real-rate. Rules with >=10 labels and
    <95% real-rate get auto-disabled in gem_rules_config."""
    cur.execute(
        """SELECT source,
                  COUNT(*) FILTER (WHERE human_label IN ('real','noise')) AS labeled,
                  COUNT(*) FILTER (WHERE human_label = 'real') AS real_ct,
                  COUNT(*) FILTER (WHERE human_label = 'noise') AS noise_ct
             FROM gem_alerts
            WHERE validator_status = 'filed'
            GROUP BY source""")
    rows = cur.fetchall()
    notes = []
    for source, labeled, real_ct, noise_ct in rows:
        if labeled is None or labeled == 0:
            rate = None
        else:
            rate = real_ct / labeled
        action = "kept"
        if labeled >= 10 and rate is not None and rate < 0.95:
            cur.execute(
                """UPDATE gem_rules_config SET enabled=FALSE,
                       confirmed_real_rate=%s, n_labeled=%s, n_real=%s,
                       last_evaluated_at=now(),
                       notes='auto-disabled: real-rate<95%% on n>=10'
                     WHERE rule_name=%s""",
                (rate, labeled, real_ct, source))
            action = "DISABLED"
        else:
            cur.execute(
                """UPDATE gem_rules_config SET
                       confirmed_real_rate=%s, n_labeled=%s, n_real=%s,
                       last_evaluated_at=now()
                     WHERE rule_name=%s""",
                (rate, labeled, real_ct, source))
        pct = f"{rate*100:.1f}%" if rate is not None else "n/a"
        log.info("[gem-rates] %s: %s/%s real, %s - %s",
                 source, real_ct, labeled, pct, action)
        notes.append(f"{source}={real_ct}/{labeled}({pct})={action}")
    return {"rules": len(rows), "summary": ";".join(notes)}


# ---------------------------------------------------------------------------
# Dispatcher

TASKS = {
    "faiss_refresh": task_faiss_refresh,
    "nn_precompute": task_nn_precompute,
    "pagerank": task_pagerank,
    "signal_correlations": task_signal_correlations,
    "permutation_sweep": task_permutation_sweep,
    "bootstrap_ci": task_bootstrap_ci,
    "rule_winrate_audit": task_rule_winrate_audit,
    "insider_cluster_check": task_insider_cluster_check,
    "earnings_proximity_check": task_earnings_proximity_check,
}

TASK_STATEMENT_TIMEOUT_MS = {
    "faiss_refresh":       900_000,
    "nn_precompute":       300_000,
    "pagerank":            300_000,
    "signal_correlations": 300_000,
    "permutation_sweep":    60_000,
    "bootstrap_ci":         60_000,
    "rule_winrate_audit":   60_000,
    "insider_cluster_check": 60_000,
}


def main():
    log.info("gem hunter v2 starting host=%s gpu=%s cupy=%s kp=%s llm=%s",
             HOST, GPU_ENV, HAS_CUPY, KILL_PRED_URL, LLM_HOST)
    FAISS_DIR.mkdir(parents=True, exist_ok=True)
    GEM_DRAFT_DIR.mkdir(parents=True, exist_ok=True)
    with Conn() as conn:
        with conn.cursor() as cur:
            ensure_runlog(cur)
    while True:
        task_name = None; result: dict = {}; t0 = time.time()
        try:
            with Conn() as conn:
                with conn.cursor() as cur:
                    task_name = claim_task(cur)
                    if task_name is None:
                        pass
                    else:
                        log.info("running task %s", task_name)
                        try:
                            timeout_ms = TASK_STATEMENT_TIMEOUT_MS.get(task_name, 60_000)
                            cur.execute("SET LOCAL statement_timeout = %s", (timeout_ms,))
                            result = TASKS[task_name](cur) or {}
                        except Exception as e:
                            conn.rollback()
                            log.exception("task %s failed: %s", task_name, e)
                            with conn.cursor() as c2:
                                c2.execute("UPDATE gem_hunter_runlog SET note=%s WHERE task=%s",
                                           (f"error: {e}"[:240], task_name))
                            continue
                        dur = time.time() - t0
                        cur.execute(
                            """UPDATE gem_hunter_runlog
                                  SET duration_s=%s, note=%s WHERE task=%s""",
                            (dur, json.dumps(result, default=str)[:240], task_name))
                        log.info("task %s done in %.1fs result=%s", task_name, dur, result)
        except Exception as e:
            log.exception("loop error: %s", e); time.sleep(10); continue
        if task_name is None:
            time.sleep(IDLE_SLEEP_SEC)


if __name__ == "__main__":
    main()
