"""
grid/signals/trial_signal.py

GRID Signal Module: Clinical Trial Catalyst Intelligence

Reads trial_cache (populated by trial_ingestor.py), scores trial quality,
resolves sponsor→ticker through the layered ``grid.signals.sponsor_resolver``
(INDUSTRY sponsors only), and surfaces near-term catalyst opportunities as
GRID signals.

Conforms to GRID signal interface:
  - generate() → list[SignalResult]
  - score is normalized [0, 1]
  - regime-aware (reads from regime_history)
  - writes to trial_signals (separate domain from GRID features) and a compact
    READOUT row per signal into catalyst_calendar (deduped on nct_id)

Company data is GRID-first (no paid quote API):
  ticker_metrics_daily.market_cap_usd → company_profiles.profile->>'market_cap'
  → raw_series TIINGO_FUND:{T}:market_cap → FMP profile (only if FMP_API_KEY)
  → None. Cash runway comes from company_profiles.profile (cash, quarterly_burn,
  cash_runway_months — written by ingestion.altdata.small_cap_enrichment).

The mcap < $2B gate is ENFORCED: > $2B is skipped, unknown cap is capped at
WATCHLIST with red flag ``market_cap_unknown``.

Usage (standalone):
    python3 -m grid.signals.trial_signal --output table
    python3 -m grid.signals.trial_signal --output db --top-n 20

Usage (via GRID pipeline / Hermes):
    from grid.signals.trial_signal import TrialGemSignal, run_daily
    sig = TrialGemSignal(db_conn)
    results = sig.generate()
    run_daily(engine)   # Hermes registry entry `trial_signal`
"""

from __future__ import annotations

import os
import json
import logging
import datetime
import time
import requests
import psycopg2
import psycopg2.extras
from dataclasses import dataclass, asdict, field
from typing import Any, Optional

from grid.signals.sponsor_resolver import (
    ResolvedSponsor,
    is_industry_class,
    normalize_sponsor_name,
    resolve_sponsor,
    resolve_ticker_sec,
    _load_sec_tickers as _load_sec_tickers,  # re-exported for back-compat
)

log = logging.getLogger("grid.signals.trial_signal")

# ── Config ────────────────────────────────────────────────────────────────────

CT_GOV_BASE = "https://clinicaltrials.gov/api/v2/studies"
SEC_COMPANY_TICKERS = "https://www.sec.gov/files/company_tickers.json"
SEC_UA = "GRID Research grid@stepdad.finance"
FMP_PROFILE_URL = "https://financialmodelingprep.com/api/v3/profile/{ticker}"
FMP_CALL_BUDGET = 60           # per process — FMP free tier is 250 req/day
FMP_PAUSE_S = 0.3

MARKET_CAP_MAX_MM = 2000.0     # the < $2B gate
MARKET_CAP_MIN_MM = 10.0       # shells / delisted
RUN_DAILY_TOP_N = 60

# Regimes trial_signals.regime_at_signal accepts (CHECK constraint)
ALLOWED_REGIMES = frozenset({"GROWTH", "NEUTRAL", "FRAGILE", "CRISIS", "UNKNOWN"})

# Disease areas with priority scores (higher = bigger abnormal return history)
DISEASE_PRIORITY = {
    "neoplasm":          1.00,
    "cancer":            1.00,
    "carcinoma":         1.00,
    "lymphoma":          1.00,
    "leukemia":          1.00,
    "melanoma":          1.00,
    "glioblastoma":      1.00,
    "sarcoma":           1.00,
    "myeloma":           0.95,
    "rare":              0.90,
    "orphan":            0.90,
    "cns":               0.80,
    "neurological":      0.80,
    "alzheimer":         0.80,
    "parkinson":         0.80,
    "autoimmune":        0.75,
    "inflammatory":      0.75,
    "psoriasis":         0.70,
    "lupus":             0.70,
    "diabetes":          0.65,
    "obesity":           0.65,
    "cardiovascular":    0.60,
    "fibrosis":          0.70,
    "nash":              0.70,
}

# Signal scoring weights (AutoAgent hill-climbs these via instruction.md)
SCORE_WEIGHTS = {
    "endpoint_clarity":  0.25,
    "phase_weight":      0.20,
    "disease_priority":  0.20,
    "enrollment_pct":    0.15,
    "fda_designation":   0.10,
    "cash_runway":       0.10,
}

FAVORABLE_REGIMES = {"GROWTH", "NEUTRAL"}


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class TrialRecord:
    nct_id: str
    title: str
    sponsor: str
    sponsor_class: str
    phase: str
    status: str
    conditions: list[str]
    interventions: list[str]
    enrollment_target: Optional[int]
    enrollment_actual: Optional[int]
    primary_completion: Optional[datetime.date]
    start_date: Optional[datetime.date]
    why_stopped: Optional[str]
    has_results: bool


@dataclass
class SignalResult:
    nct_id: str
    ticker: str
    company_name: str
    trial_phase: str
    primary_indication: str
    primary_endpoint: str
    endpoint_type: str
    fda_designation: str
    trial_strength_score: float
    endpoint_clarity: float
    phase_weight: float
    disease_priority_score: float
    enrollment_pct: float
    days_to_completion: int
    market_cap_mm: Optional[float]
    cash_runway_months: Optional[float]
    pipeline_depth: Optional[int]
    signal_type: str          # BUY | WATCHLIST | AVOID
    regime_at_signal: str
    confidence: float
    suggested_position_pct: Optional[float]
    rationale: str
    red_flags: list[str] = field(default_factory=list)
    catalysts: list[str] = field(default_factory=list)
    penalty_factors: dict = field(default_factory=dict)


# ── Sponsor → ticker (moved to grid.signals.sponsor_resolver) ─────────────────
# Thin aliases so existing callers / test patches keep working.

_resolve_ticker_sec = resolve_ticker_sec


# ── Main signal class ─────────────────────────────────────────────────────────

class TrialGemSignal:
    """
    GRID signal module for clinical trial catalyst discovery.
    Reads from trial_cache (populated by trial_ingestor.py) and CT.gov live API.

    ``engine`` (optional SQLAlchemy Engine) feeds the sponsor resolver's
    persistent cache (``sponsor_ticker_map``); all other reads go through the
    psycopg2 connection.
    """

    def __init__(self, db_conn=None, db_config: dict | None = None, engine: Any = None):
        if db_conn:
            self.conn = db_conn
        elif db_config:
            self.conn = psycopg2.connect(**db_config)
        else:
            self.conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=int(os.getenv("DB_PORT", 5432)),
                dbname=os.getenv("DB_NAME", "griddb"),
                user=os.getenv("DB_USER", "grid"),
                password=os.getenv("DB_PASSWORD", ""),
            )
        self.engine = engine
        self._fmp_calls = 0  # FMP budget tracker (per process)
        self.stats: dict[str, int] = {
            "trials": 0, "skipped_non_industry": 0, "skipped_unresolved": 0,
            "skipped_cap_gate": 0, "cap_unknown": 0, "scored": 0, "deduped": 0,
        }

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001
            pass

    # ── Public interface ──────────────────────────────────────────────────────

    def _resolve(self, trial: TrialRecord) -> ResolvedSponsor:
        """Resolve the trial's lead sponsor (layered resolver; never raises)."""
        try:
            return resolve_sponsor(self.engine, trial.sponsor, trial.sponsor_class)
        except Exception as e:  # noqa: BLE001
            log.warning(f"Sponsor resolution failed for {trial.sponsor!r}: {e}")
            return ResolvedSponsor(None, "error", 0.0, "resolver_error")

    def generate(self, top_n: int = 10) -> list[SignalResult]:
        """
        Main entry point. Returns ranked list of trial signal candidates.
        Regime-gated: only BUY in GROWTH/NEUTRAL, WATCHLIST otherwise.
        Non-INDUSTRY sponsors are skipped before any resolution; the mcap
        gate is enforced (unknown cap → WATCHLIST at most + red flag).
        """
        regime = self._get_regime()
        log.info(f"Current GRID regime: {regime}")

        # Try DB cache first, fall back to live API
        trials = self._load_from_cache()
        if not trials:
            log.info("No cached trials, fetching live from ClinicalTrials.gov")
            trials = self._fetch_candidates_live()
        log.info(f"Processing {len(trials)} candidate trials")
        self.stats["trials"] = len(trials)

        results = []
        seen: set[tuple[str, str]] = set()
        company_cache: dict[str, dict] = {}
        for trial in trials:
            if is_industry_class(trial.sponsor_class) is not True:
                # Universities / NIH / hospitals / networks are not investable.
                self.stats["skipped_non_industry"] += 1
                continue

            resolved = self._resolve(trial)
            ticker = resolved.ticker
            if not ticker:
                # Skip trials we can't map to a ticker — no point scoring garbage
                self.stats["skipped_unresolved"] += 1
                continue
            if (trial.nct_id, ticker) in seen:
                self.stats["deduped"] += 1
                continue
            seen.add((trial.nct_id, ticker))

            if ticker not in company_cache:
                company_cache[ticker] = self._fetch_company_data(ticker)
            company_data = company_cache[ticker]
            gate = self._company_gate(company_data)
            if gate == "skip":
                self.stats["skipped_cap_gate"] += 1
                continue
            cap_unknown = gate == "cap_unknown"
            if cap_unknown:
                self.stats["cap_unknown"] += 1

            score_components = self._score_trial(trial, company_data)
            total_score = self._weighted_score(score_components)
            penalties, penalty_mult = self._apply_penalties(trial, company_data)
            final_score = total_score * penalty_mult

            signal_type = self._determine_signal(final_score, regime)
            red_flags = list(penalties.keys())
            if cap_unknown:
                # Enforced gate: without a known market cap nothing is a BUY.
                if signal_type == "BUY":
                    signal_type = "WATCHLIST"
                red_flags.append("market_cap_unknown")
                penalties["market_cap_unknown"] = 1.0
            confidence = self._compute_confidence(final_score, regime)
            position = self._position_size(final_score, confidence) if signal_type == "BUY" else None

            days_out = self._days_to_completion(trial.primary_completion)
            indication = self._extract_indication(trial.conditions)

            result = SignalResult(
                nct_id                 = trial.nct_id,
                ticker                 = ticker,
                company_name           = trial.sponsor,
                trial_phase            = trial.phase,
                primary_indication     = indication,
                primary_endpoint       = self._extract_endpoint(trial),
                endpoint_type          = self._endpoint_type(trial),
                fda_designation        = self._check_fda_designation(trial),
                trial_strength_score   = round(final_score, 4),
                endpoint_clarity       = score_components.get("endpoint_clarity", 0.5),
                phase_weight           = score_components.get("phase_weight", 0.6),
                disease_priority_score = score_components.get("disease_priority", 0.5),
                enrollment_pct         = score_components.get("enrollment_pct", 0.0),
                days_to_completion     = days_out,
                market_cap_mm          = company_data.get("market_cap_mm"),
                cash_runway_months     = company_data.get("cash_runway_months"),
                pipeline_depth         = company_data.get("pipeline_depth"),
                signal_type            = signal_type,
                regime_at_signal       = regime,
                confidence             = round(confidence, 4),
                suggested_position_pct = position,
                rationale              = self._build_rationale(trial, score_components, regime),
                red_flags              = red_flags,
                catalysts              = self._extract_catalysts(trial),
                penalty_factors        = penalties,
            )
            results.append(result)

        results.sort(key=lambda r: (-r.trial_strength_score, r.days_to_completion))
        self.stats["scored"] = len(results)
        log.info(
            "trial_signal: trials=%d industry=%d resolved=%d scored=%d "
            "skipped_cap=%d cap_unknown=%d deduped=%d",
            self.stats["trials"],
            self.stats["trials"] - self.stats["skipped_non_industry"],
            self.stats["trials"] - self.stats["skipped_non_industry"] - self.stats["skipped_unresolved"],
            self.stats["scored"], self.stats["skipped_cap_gate"],
            self.stats["cap_unknown"], self.stats["deduped"],
        )
        return results[:top_n]

    # Dedupe: one trial_signals row per (nct_id, ticker) per day; re-runs on
    # the same day do not stack duplicates (the April run had several per NCT).
    _INSERT_SIGNAL_SQL = """
        INSERT INTO trial_signals (
            run_id, nct_id, ticker, company_name, sponsor_name,
            trial_phase, primary_indication, primary_endpoint,
            endpoint_type, fda_designation,
            primary_completion_date,
            enrollment_pct, days_to_completion,
            market_cap_mm, cash_runway_months, pipeline_depth,
            trial_strength_score, endpoint_clarity, phase_weight,
            disease_priority, cash_runway_score, penalty_factors,
            signal_type, regime_at_signal, confidence,
            suggested_position_pct, rationale, red_flags, catalysts
        )
        SELECT
            %(run_id)s, %(nct_id)s, %(ticker)s, %(company_name)s, %(sponsor_name)s,
            %(trial_phase)s, %(primary_indication)s, %(primary_endpoint)s,
            %(endpoint_type)s, %(fda_designation)s,
            %(primary_completion_date)s,
            %(enrollment_pct)s, %(days_to_completion)s,
            %(market_cap_mm)s, %(cash_runway_months)s, %(pipeline_depth)s,
            %(trial_strength_score)s, %(endpoint_clarity)s, %(phase_weight)s,
            %(disease_priority)s, %(cash_runway_score)s, %(penalty_factors)s,
            %(signal_type)s, %(regime_at_signal)s, %(confidence)s,
            %(suggested_position_pct)s, %(rationale)s, %(red_flags)s, %(catalysts)s
        WHERE NOT EXISTS (
            SELECT 1 FROM trial_signals
            WHERE nct_id = %(nct_id)s AND ticker = %(ticker)s
              AND created_at >= CURRENT_DATE
        )
    """

    # Compact READOUT row per signal; deduped on (nct_id, ticker) against any
    # active row (the ingestor may already have written one for this NCT).
    _INSERT_CALENDAR_SQL = """
        INSERT INTO catalyst_calendar
            (ticker, nct_id, event_type, expected_date,
             confidence_window_days, source, notes, is_active)
        SELECT %(ticker)s, %(nct_id)s, 'READOUT', %(expected_date)s,
               30, 'trial_signal', %(notes)s, TRUE
        WHERE NOT EXISTS (
            SELECT 1 FROM catalyst_calendar
            WHERE nct_id = %(nct_id)s AND ticker = %(ticker)s AND is_active = TRUE
        )
    """

    def write_to_db(self, results: list[SignalResult], run_id: str | None = None) -> int:
        """Persist signals to griddb trial_signals (+ compact catalyst_calendar rows).

        Returns the number of trial_signals rows inserted (rows already present
        for the same (nct_id, ticker) today are skipped, not duplicated).
        """
        if not results:
            return 0

        cur = self.conn.cursor()
        written = 0
        calendar_rows = 0
        run_id = run_id or datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        seen: set[tuple[str, str]] = set()

        for r in results:
            key = (r.nct_id, r.ticker)
            if key in seen:
                continue
            seen.add(key)
            try:
                # Compute completion date from days_to_completion
                completion_date = None
                if r.days_to_completion is not None and r.days_to_completion < 999:
                    completion_date = (
                        datetime.date.today()
                        + datetime.timedelta(days=r.days_to_completion)
                    )

                cur.execute(self._INSERT_SIGNAL_SQL, {
                    "run_id": run_id, "nct_id": r.nct_id, "ticker": r.ticker,
                    "company_name": r.company_name, "sponsor_name": r.company_name,
                    "trial_phase": r.trial_phase, "primary_indication": r.primary_indication,
                    "primary_endpoint": r.primary_endpoint, "endpoint_type": r.endpoint_type,
                    "fda_designation": r.fda_designation,
                    "primary_completion_date": completion_date,
                    "enrollment_pct": r.enrollment_pct, "days_to_completion": r.days_to_completion,
                    "market_cap_mm": r.market_cap_mm, "cash_runway_months": r.cash_runway_months,
                    "pipeline_depth": r.pipeline_depth,
                    "trial_strength_score": r.trial_strength_score,
                    "endpoint_clarity": r.endpoint_clarity, "phase_weight": r.phase_weight,
                    "disease_priority": r.disease_priority_score,
                    "cash_runway_score": r.cash_runway_months,
                    "penalty_factors": json.dumps(r.penalty_factors),
                    "signal_type": r.signal_type,
                    "regime_at_signal": _storage_regime(r.regime_at_signal),
                    "confidence": r.confidence,
                    "suggested_position_pct": r.suggested_position_pct,
                    "rationale": r.rationale, "red_flags": r.red_flags, "catalysts": r.catalysts,
                })
                written += _rowcount(cur)

                if completion_date is not None:
                    cur.execute(self._INSERT_CALENDAR_SQL, {
                        "ticker": r.ticker, "nct_id": r.nct_id,
                        "expected_date": completion_date,
                        "notes": (r.rationale or "")[:200],
                    })
                    calendar_rows += _rowcount(cur)
            except Exception as e:
                log.warning(f"Failed to write {r.ticker}/{r.nct_id}: {e}")
                self.conn.rollback()
                continue

        self.conn.commit()
        cur.close()
        log.info(
            f"Wrote {written} trial signals + {calendar_rows} catalyst_calendar rows "
            f"to griddb (run_id={run_id})"
        )
        return written

    # ── Data sources ──────────────────────────────────────────────────────────

    def _load_from_cache(self) -> list[TrialRecord]:
        """
        Read from trial_cache in griddb (populated by trial_ingestor.py).
        Filters to 30-180 day readout window, INDUSTRY sponsors, no stopped trials.
        """
        trials = []
        today = datetime.date.today()
        try:
            cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT nct_id, raw_json FROM trial_cache WHERE expires_at > NOW()")
            for row in cur:
                s = row["raw_json"] if isinstance(row["raw_json"], dict) else json.loads(row["raw_json"])
                trial = self._parse_study(s, today)
                if trial:
                    trials.append(trial)
            cur.close()
            log.info(f"Loaded {len(trials)} candidate trials from trial_cache")
        except Exception as e:
            log.warning(f"Failed to read trial_cache: {e}")
        return trials

    def _fetch_candidates_live(self) -> list[TrialRecord]:
        """Fetch directly from CT.gov API v2 with pagination."""
        trials = []
        today = datetime.date.today()
        params = {
            "filter.overallStatus": "ACTIVE_NOT_RECRUITING",
            "filter.advanced": "AREA[Phase](PHASE2 OR PHASE3) AND AREA[StudyType]INTERVENTIONAL",
            "pageSize": 1000,
        }
        next_token = None

        while True:
            if next_token:
                params["pageToken"] = next_token
            try:
                resp = requests.get(CT_GOV_BASE, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                studies = data.get("studies", [])
                for s in studies:
                    trial = self._parse_study(s, today)
                    if trial:
                        trials.append(trial)
                next_token = data.get("nextPageToken")
                if not next_token or len(studies) < 1000:
                    break
            except Exception as e:
                log.error(f"CT.gov API error: {e}")
                break

        return trials

    def _parse_study(self, s: dict, today: datetime.date) -> Optional[TrialRecord]:
        """Parse a CT.gov v2 study JSON into a TrialRecord, applying hard filters."""
        proto = s.get("protocolSection", {})
        id_mod = proto.get("identificationModule", {})
        status_mod = proto.get("statusModule", {})
        sponsor_mod = proto.get("sponsorCollaboratorsModule", {})
        design_mod = proto.get("designModule", {})
        cond_mod = proto.get("conditionsModule", {})
        interv_mod = proto.get("armsInterventionsModule", {})

        nct_id = id_mod.get("nctId", "")
        if not nct_id:
            return None

        pc_date = self._parse_date(
            status_mod.get("primaryCompletionDateStruct", {}).get("date")
        )
        if not pc_date:
            return None

        days = (pc_date - today).days
        if not (30 <= days <= 180):
            return None

        sponsor_class = sponsor_mod.get("leadSponsor", {}).get("class", "")

        why_stopped = status_mod.get("whyStopped")
        if why_stopped:
            return None

        start_date = self._parse_date(
            status_mod.get("startDateStruct", {}).get("date")
        )

        phases = design_mod.get("phases", [])
        conditions = cond_mod.get("conditions", [])
        interventions = [
            i.get("name", "") for i in interv_mod.get("interventions", [])
        ]

        enroll_info = design_mod.get("enrollmentInfo", {})

        return TrialRecord(
            nct_id=nct_id,
            title=id_mod.get("briefTitle", ""),
            sponsor=sponsor_mod.get("leadSponsor", {}).get("name", ""),
            sponsor_class=sponsor_class,
            phase=phases[0] if phases else "",
            status=status_mod.get("overallStatus", ""),
            conditions=conditions,
            interventions=interventions,
            enrollment_target=enroll_info.get("count"),
            enrollment_actual=enroll_info.get("count"),
            primary_completion=pc_date,
            start_date=start_date,
            why_stopped=None,
            has_results=bool(s.get("resultsSection")),
        )

    # ── Company data (GRID-first; no paid quote API) ──────────────────────────

    def _query_one(self, sql: str, params: tuple) -> Optional[tuple]:
        """Run a single-row read on the psycopg2 connection; None on any failure."""
        try:
            cur = self.conn.cursor()
            cur.execute(sql, params)
            row = cur.fetchone()
            cur.close()
            return tuple(row) if row is not None else None
        except Exception as e:  # noqa: BLE001
            log.debug(f"company lookup failed: {e}")
            try:
                self.conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            return None

    def _market_cap_from_metrics(self, ticker: str, as_of: datetime.date) -> Optional[float]:
        """ticker_metrics_daily.market_cap_usd (USD), PIT-bounded by ``as_of``."""
        row = self._query_one(
            "SELECT market_cap_usd FROM ticker_metrics_daily "
            "WHERE ticker = %s AND market_cap_usd IS NOT NULL AND obs_date <= %s "
            "ORDER BY obs_date DESC LIMIT 1",
            (ticker, as_of),
        )
        return _pos_float(row[0]) if row else None

    def _profile(self, ticker: str) -> dict:
        """company_profiles.profile JSONB as a dict ({} when absent)."""
        row = self._query_one("SELECT profile FROM company_profiles WHERE ticker = %s", (ticker,))
        if not row or row[0] is None:
            return {}
        p = row[0]
        if isinstance(p, str):
            try:
                p = json.loads(p)
            except ValueError:
                return {}
        return p if isinstance(p, dict) else {}

    def _market_cap_from_tiingo(self, ticker: str, as_of: datetime.date) -> Optional[float]:
        """Latest raw_series TIINGO_FUND:{T}:market_cap (USD) at or before ``as_of``."""
        row = self._query_one(
            "SELECT value FROM raw_series "
            "WHERE series_id = %s AND pull_status = 'SUCCESS' AND obs_date <= %s "
            "ORDER BY obs_date DESC, pull_timestamp DESC LIMIT 1",
            (f"TIINGO_FUND:{ticker}:market_cap", as_of),
        )
        return _pos_float(row[0]) if row else None

    def _market_cap_from_fmp(self, ticker: str) -> Optional[float]:
        """FMP profile ``mktCap`` (USD) — only when FMP_API_KEY is set, within budget."""
        key = _fmp_api_key()
        if not key or self._fmp_calls >= FMP_CALL_BUDGET:
            return None
        self._fmp_calls += 1
        try:
            resp = requests.get(
                FMP_PROFILE_URL.format(ticker=ticker), params={"apikey": key}, timeout=10,
            )
            if resp.status_code != 200:
                log.warning(f"FMP profile {ticker}: HTTP {resp.status_code}")
                return None
            data = resp.json()
            first = data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else {})
            time.sleep(FMP_PAUSE_S)
            return _pos_float(first.get("mktCap"))
        except Exception as e:  # noqa: BLE001
            log.warning(f"FMP profile {ticker} failed: {e}")
            return None

    def _fetch_company_data(self, ticker: str, as_of: Optional[datetime.date] = None) -> dict:
        """GRID-first market cap + cash runway for ``ticker``.

        Order: ticker_metrics_daily → company_profiles.profile.market_cap →
        raw_series TIINGO_FUND market_cap → FMP profile (key required) → None.
        Runway from company_profiles.profile (cash_runway_months, or
        cash / quarterly_burn * 3). Never raises.
        """
        as_of = as_of or datetime.date.today()
        profile = self._profile(ticker)

        mcap_usd = self._market_cap_from_metrics(ticker, as_of)
        source = "ticker_metrics_daily" if mcap_usd else None
        if mcap_usd is None:
            mcap_usd = _pos_float(profile.get("market_cap"))
            source = "company_profiles" if mcap_usd else None
        if mcap_usd is None:
            mcap_usd = self._market_cap_from_tiingo(ticker, as_of)
            source = "tiingo_fundamentals" if mcap_usd else None
        if mcap_usd is None:
            mcap_usd = self._market_cap_from_fmp(ticker)
            source = "fmp" if mcap_usd else None

        runway = _pos_float(profile.get("cash_runway_months"))
        if runway is None:
            cash = _pos_float(profile.get("cash"))
            burn = _pos_float(profile.get("quarterly_burn"))
            if cash is not None and burn is not None:
                runway = round(cash / max(burn, 1.0) * 3.0, 1)

        return {
            "market_cap_mm": round(mcap_usd / 1e6, 2) if mcap_usd else None,
            "market_cap_source": source,
            "cash_runway_months": runway,
            "pipeline_depth": _int_or_none(profile.get("pipeline_depth")),
            "cash": _pos_float(profile.get("cash")),
            "quarterly_burn": _pos_float(profile.get("quarterly_burn")),
        }

    def _company_gate(self, company_data: dict) -> str:
        """Enforced mcap gate: 'pass' | 'skip' (>$2B or <$10M) | 'cap_unknown'."""
        mc = company_data.get("market_cap_mm")
        if mc is None:
            return "cap_unknown"
        if mc > MARKET_CAP_MAX_MM or mc < MARKET_CAP_MIN_MM:
            return "skip"
        return "pass"

    def _passes_company_gates(self, company_data: dict) -> bool:
        """Back-compat boolean view of :meth:`_company_gate` (unknown cap passes, but is capped)."""
        return self._company_gate(company_data) != "skip"

    # ── Scoring ────────────────────────────────────────────────────────────────

    def _score_trial(self, trial: TrialRecord, company_data: dict) -> dict:
        phase_str = trial.phase.upper().replace(" ", "")
        phase_w = 1.0 if "PHASE3" in phase_str else 0.6 if "PHASE2" in phase_str else 0.3

        disease_score = self._disease_priority(trial.conditions)

        enroll_pct = 1.0
        if trial.enrollment_target and trial.enrollment_actual:
            enroll_pct = min(1.0, trial.enrollment_actual / trial.enrollment_target)

        cash_score = 0.5
        if company_data.get("cash_runway_months"):
            cash_score = min(1.0, company_data["cash_runway_months"] / 24)

        endpoint_c = self._endpoint_clarity_score(trial)
        fda_score = 1.0 if self._check_fda_designation(trial) else 0.0

        return {
            "endpoint_clarity": endpoint_c,
            "phase_weight":     phase_w,
            "disease_priority": disease_score,
            "enrollment_pct":   enroll_pct,
            "fda_designation":  fda_score,
            "cash_runway":      cash_score,
        }

    def _weighted_score(self, components: dict) -> float:
        total = sum(components.get(k, 0.0) * w for k, w in SCORE_WEIGHTS.items())
        return round(min(1.0, total), 4)

    def _apply_penalties(self, trial: TrialRecord, company_data: dict) -> tuple[dict, float]:
        penalties = {}
        mult = 1.0

        if trial.start_date and trial.primary_completion:
            actual_months = (datetime.date.today() - trial.start_date).days / 30
            if actual_months > 48:
                penalties["slow_enrollment"] = 0.6
                mult *= 0.6

        if trial.has_results:
            penalties["results_already_posted"] = 0.1
            mult *= 0.1

        return penalties, mult

    def _disease_priority(self, conditions: list[str]) -> float:
        text = " ".join(conditions).lower()
        for keyword, priority in DISEASE_PRIORITY.items():
            if keyword in text:
                return priority
        return 0.50

    def _endpoint_clarity_score(self, trial: TrialRecord) -> float:
        text = (trial.title + " " + " ".join(trial.interventions)).lower()
        if any(t in text for t in ["overall survival", "progression-free",
                                    "complete response", "event-free"]):
            return 1.0
        if any(t in text for t in ["composite", "combined"]):
            return 0.7
        if any(t in text for t in ["biomarker", "surrogate", "imaging"]):
            return 0.5
        return 0.4

    def _check_fda_designation(self, trial: TrialRecord) -> str:
        text = trial.title.lower()
        if "breakthrough" in text:
            return "Breakthrough Therapy"
        if "fast track" in text:
            return "Fast Track"
        if "orphan" in text:
            return "Orphan Drug"
        if "accelerated" in text:
            return "Accelerated Approval"
        return ""

    # ── Signal determination ───────────────────────────────────────────────────

    def _determine_signal(self, score: float, regime: str) -> str:
        if regime in FAVORABLE_REGIMES:
            if score >= 0.65:
                return "BUY"
            elif score >= 0.40:
                return "WATCHLIST"
            return "AVOID"
        else:
            if score >= 0.70:
                return "WATCHLIST"
            return "AVOID"

    def _compute_confidence(self, score: float, regime: str) -> float:
        regime_adj = {"GROWTH": 1.0, "NEUTRAL": 0.9, "FRAGILE": 0.6, "CRISIS": 0.4}
        return round(score * regime_adj.get(regime, 0.7), 4)

    def _position_size(self, score: float, confidence: float) -> float:
        """Kelly-inspired position sizing: max 5% for any single trial bet."""
        base = score * confidence * 0.05
        return round(min(0.05, max(0.005, base)), 4)

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _get_regime(self) -> str:
        """Read latest regime from regime_history (the actual GRID table)."""
        try:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT regime FROM regime_history ORDER BY obs_date DESC LIMIT 1"
            )
            row = cur.fetchone()
            cur.close()
            return row[0] if row else "UNKNOWN"
        except Exception:
            return "UNKNOWN"

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime.date]:
        if not date_str:
            return None
        for fmt in ("%Y-%m-%d", "%B %d, %Y", "%B %Y", "%Y"):
            try:
                d = datetime.datetime.strptime(date_str, fmt).date()
                if fmt in ("%B %Y", "%Y"):
                    d = d.replace(day=1)
                return d
            except ValueError:
                continue
        return None

    def _days_to_completion(self, date: Optional[datetime.date]) -> int:
        if not date:
            return 999
        return max(0, (date - datetime.date.today()).days)

    def _extract_indication(self, conditions: list[str]) -> str:
        if not conditions:
            return "Unknown"
        text = " ".join(conditions).lower()
        for kw, _ in sorted(DISEASE_PRIORITY.items(), key=lambda x: -x[1]):
            if kw in text:
                return kw.title()
        return conditions[0][:30] if conditions else "Unknown"

    def _extract_endpoint(self, trial: TrialRecord) -> str:
        text = trial.title.lower()
        for ep in ["overall survival", "progression-free survival",
                    "complete response", "event-free survival",
                    "objective response rate", "disease-free survival"]:
            if ep in text:
                return ep.title()
        return "See Protocol"

    def _endpoint_type(self, trial: TrialRecord) -> str:
        score = self._endpoint_clarity_score(trial)
        if score >= 0.9:  return "binary"
        if score >= 0.6:  return "composite"
        if score >= 0.4:  return "surrogate"
        return "other"

    def _build_rationale(self, trial: TrialRecord, components: dict, regime: str) -> str:
        parts = [
            f"{trial.phase}",
            f"readout {trial.primary_completion}",
            f"{self._extract_indication(trial.conditions)}",
            f"endpoint={components.get('endpoint_clarity', 0):.2f}",
            f"regime={regime}",
        ]
        fda = self._check_fda_designation(trial)
        if fda:
            parts.append(f"FDA: {fda}")
        return " | ".join(parts)

    def _extract_catalysts(self, trial: TrialRecord) -> list[str]:
        catalysts = []
        if trial.primary_completion:
            catalysts.append(f"Primary readout: {trial.primary_completion}")
        fda = self._check_fda_designation(trial)
        if fda:
            catalysts.append(f"FDA designation: {fda}")
        return catalysts


# ── Module helpers ────────────────────────────────────────────────────────────


def _pos_float(value: Any) -> Optional[float]:
    """Finite positive float or None."""
    if value is None or value == "":
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f != f or f in (float("inf"), float("-inf")) or f <= 0:
        return None
    return f


def _int_or_none(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _fmp_api_key() -> str:
    """FMP key from the environment, else config.settings; '' when unset."""
    key = os.getenv("FMP_API_KEY", "")
    if key:
        return key
    try:
        from config import settings

        return str(getattr(settings, "FMP_API_KEY", "") or "")
    except Exception:  # noqa: BLE001
        return ""


def _storage_regime(regime: Optional[str]) -> str:
    """Map any regime label onto the trial_signals CHECK set (unknown labels → UNKNOWN)."""
    r = str(regime or "UNKNOWN").strip().upper()
    return r if r in ALLOWED_REGIMES else "UNKNOWN"


def _rowcount(cur: Any) -> int:
    """psycopg2 rowcount as an insert count (unknown/-1 → 1)."""
    rc = getattr(cur, "rowcount", None)
    if rc is None or not isinstance(rc, int) or rc < 0:
        return 1
    return rc


def _db_config_from_engine(engine: Any) -> dict:
    """psycopg2 connect kwargs from a SQLAlchemy engine URL (falls back to env)."""
    url = getattr(engine, "url", None)
    host = getattr(url, "host", None)
    if url is None or not host:
        return {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", 5432)),
            "dbname": os.getenv("DB_NAME", "griddb"),
            "user": os.getenv("DB_USER", "grid"),
            "password": os.getenv("DB_PASSWORD", ""),
        }
    return {
        "host": host,
        "port": int(getattr(url, "port", None) or 5432),
        "dbname": getattr(url, "database", None) or "griddb",
        "user": getattr(url, "username", None) or "grid",
        "password": getattr(url, "password", None) or "",
    }


def run_daily(engine: Any, top_n: int = RUN_DAILY_TOP_N) -> dict:
    """Hermes entry point (registry ``trial_signal``): generate top ``top_n`` and persist.

    Instantiates :class:`TrialGemSignal` against the GRID engine's DSN (the
    engine itself feeds the sponsor resolver cache), writes to
    ``trial_signals`` + ``catalyst_calendar`` and returns counts. Never raises.
    """
    started = time.monotonic()
    sig: Optional[TrialGemSignal] = None
    try:
        sig = TrialGemSignal(db_config=_db_config_from_engine(engine), engine=engine)
        results = sig.generate(top_n=top_n)
        written = sig.write_to_db(results)
        counts = {
            "status": "SUCCESS",
            "scored": len(results),
            "written": written,
            "buy": sum(1 for r in results if r.signal_type == "BUY"),
            "watchlist": sum(1 for r in results if r.signal_type == "WATCHLIST"),
            "avoid": sum(1 for r in results if r.signal_type == "AVOID"),
            "elapsed_s": round(time.monotonic() - started, 1),
        }
        counts.update({k: v for k, v in sig.stats.items() if k != "scored"})
        log.info("trial_signal.run_daily: %s", counts)
        return counts
    except Exception as e:  # noqa: BLE001
        log.warning(f"trial_signal.run_daily failed: {e}")
        return {"status": "FAILED", "error": str(e)[:200], "scored": 0, "written": 0,
                "elapsed_s": round(time.monotonic() - started, 1)}
    finally:
        if sig is not None:
            sig.close()


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="GRID Trial Gem Signal Generator")
    parser.add_argument("--output", choices=["jsonl", "table", "db"], default="table")
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--run-id", type=str, default=None)
    args = parser.parse_args()

    sig = TrialGemSignal()
    results = sig.generate(top_n=args.top_n)

    if args.output == "db":
        written = sig.write_to_db(results, run_id=args.run_id)
        print(f"Wrote {written} signals to griddb")

    elif args.output == "jsonl":
        for r in results:
            print(json.dumps(asdict(r), default=str))

    else:  # table
        buy = [r for r in results if r.signal_type == "BUY"]
        watch = [r for r in results if r.signal_type == "WATCHLIST"]
        avoid = [r for r in results if r.signal_type == "AVOID"]

        regime = results[0].regime_at_signal if results else "N/A"
        w = 110

        print(f"\n{'='*w}")
        print(f"{'GRID Trial Gem Hunter':^{w}}")
        print(f"{'Regime: ' + regime:^{w}}")
        print(f"{'='*w}")
        print(f"{'TICKER':<8} {'COMPANY':<25} {'INDICATION':<20} {'PHASE':<8} "
              f"{'SCORE':>6} {'SIGNAL':<10} {'DAYS':>5}  {'MCAP($M)':>9}  {'FDA'}")
        print(f"{'-'*w}")

        for section, label in [(buy, "BUY"), (watch, "WATCHLIST"), (avoid, "AVOID")]:
            if not section:
                continue
            for r in section:
                mcap = f"{r.market_cap_mm:>8.0f}" if r.market_cap_mm else "     N/A"
                fda = r.fda_designation or ""
                print(
                    f"{r.ticker:<8} {r.company_name[:24]:<25} "
                    f"{r.primary_indication[:19]:<20} {r.trial_phase:<8} "
                    f"{r.trial_strength_score:>6.3f} {r.signal_type:<10} "
                    f"{r.days_to_completion:>5}  {mcap}  {fda}"
                )
            if section != list(results)[-1:]:
                print(f"{'-'*w}")

        print(f"{'='*w}")
        print(f"  {len(buy)} BUY | {len(watch)} WATCHLIST | {len(avoid)} AVOID | "
              f"{len(results)} total scored")
        print()
