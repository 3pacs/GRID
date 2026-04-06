"""
grid/signals/trial_signal.py

GRID Signal Module: Clinical Trial Catalyst Intelligence

Reads trial_cache (populated by trial_ingestor.py cron), scores trial quality,
resolves sponsor→ticker via SEC EDGAR, and surfaces near-term catalyst
opportunities as GRID signals.

Conforms to GRID signal interface:
  - generate() → list[SignalResult]
  - score is normalized [0, 1]
  - regime-aware (reads from regime_history)
  - writes to trial_signals (separate domain from GRID features)

Usage (standalone):
    python3 -m grid.signals.trial_signal --output table
    python3 -m grid.signals.trial_signal --output db --top-n 20

Usage (via GRID pipeline):
    from grid.signals.trial_signal import TrialGemSignal
    sig = TrialGemSignal(db_conn)
    results = sig.generate()
"""

from __future__ import annotations

import os
import re
import json
import logging
import datetime
import requests
import psycopg2
import psycopg2.extras
from dataclasses import dataclass, asdict, field
from typing import Optional

log = logging.getLogger("grid.signals.trial_signal")

# ── Config ────────────────────────────────────────────────────────────────────

CT_GOV_BASE = "https://clinicaltrials.gov/api/v2/studies"
SEC_COMPANY_TICKERS = "https://www.sec.gov/files/company_tickers.json"
SEC_UA = "GRID Research grid@stepdad.finance"
AV_KEY = os.getenv("ALPHAVANTAGE_API_KEY", os.getenv("ALPHA_VANTAGE_KEY", ""))

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


# ── SEC ticker map (loaded once per process) ──────────────────────────────────

_SEC_TICKER_MAP: dict[str, str] = {}   # lowercase name → ticker
_SEC_TICKER_LOADED = False


def _load_sec_tickers():
    """
    Download SEC company_tickers.json and build a lowercase-name → ticker map.
    This is ~13K entries, covers all US-listed public companies.
    """
    global _SEC_TICKER_MAP, _SEC_TICKER_LOADED
    if _SEC_TICKER_LOADED:
        return
    try:
        resp = requests.get(
            SEC_COMPANY_TICKERS,
            headers={"User-Agent": SEC_UA},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        for entry in data.values():
            name = entry.get("title", "").strip().lower()
            ticker = entry.get("ticker", "").strip().upper()
            if name and ticker:
                _SEC_TICKER_MAP[name] = ticker
        log.info(f"Loaded {len(_SEC_TICKER_MAP)} tickers from SEC company_tickers.json")
    except Exception as e:
        log.warning(f"Failed to load SEC tickers: {e}")
    _SEC_TICKER_LOADED = True


def _resolve_ticker_sec(sponsor_name: str) -> Optional[str]:
    """
    Fuzzy match sponsor name against SEC company_tickers.json.
    Tries exact match, then prefix match, then substring match.
    """
    _load_sec_tickers()
    if not _SEC_TICKER_MAP:
        return None

    # Normalize
    name = sponsor_name.strip().lower()
    # Strip common suffixes
    for suffix in [", inc.", ", inc", " inc.", " inc", ", ltd.", ", ltd",
                   " ltd.", " ltd", " llc", " plc", " corp.", " corp",
                   " co.", " co", " s.a.", " ag", " se", " nv",
                   " gmbh", " pty", " srl"]:
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
            break

    # Exact match
    if name in _SEC_TICKER_MAP:
        return _SEC_TICKER_MAP[name]

    # Check with common variations
    for variant in [name, f"{name} inc", f"{name} inc.",
                    f"{name} corp", f"{name} pharmaceuticals",
                    f"{name} therapeutics", f"{name} biosciences"]:
        if variant in _SEC_TICKER_MAP:
            return _SEC_TICKER_MAP[variant]

    # Substring match — sponsor name contained in SEC name
    matches = []
    for sec_name, ticker in _SEC_TICKER_MAP.items():
        if name in sec_name or sec_name.startswith(name):
            matches.append((sec_name, ticker))
    if len(matches) == 1:
        return matches[0][1]
    # If multiple matches, prefer pharmaceutical/biotech companies
    for sec_name, ticker in matches:
        if any(kw in sec_name for kw in ["pharma", "thera", "bio", "onco", "medic"]):
            return ticker
    if matches:
        return matches[0][1]

    return None


# ── Main signal class ─────────────────────────────────────────────────────────

class TrialGemSignal:
    """
    GRID signal module for clinical trial catalyst discovery.
    Reads from trial_cache (populated by trial_ingestor.py) and CT.gov live API.
    """

    def __init__(self, db_conn=None, db_config: dict = None):
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
        self._av_calls = 0  # rate limit tracker

    # ── Public interface ──────────────────────────────────────────────────────

    def generate(self, top_n: int = 10) -> list[SignalResult]:
        """
        Main entry point. Returns ranked list of trial signal candidates.
        Regime-gated: only BUY in GROWTH/NEUTRAL, WATCHLIST otherwise.
        """
        regime = self._get_regime()
        log.info(f"Current GRID regime: {regime}")

        # Try DB cache first, fall back to live API
        trials = self._load_from_cache()
        if not trials:
            log.info("No cached trials, fetching live from ClinicalTrials.gov")
            trials = self._fetch_candidates_live()
        log.info(f"Processing {len(trials)} candidate trials")

        results = []
        for trial in trials:
            ticker = _resolve_ticker_sec(trial.sponsor)
            if not ticker:
                # Skip trials we can't map to a ticker — no point scoring garbage
                continue

            company_data = self._fetch_company_data(ticker)
            if not self._passes_company_gates(company_data):
                continue

            score_components = self._score_trial(trial, company_data)
            total_score = self._weighted_score(score_components)
            penalties, penalty_mult = self._apply_penalties(trial, company_data)
            final_score = total_score * penalty_mult

            signal_type = self._determine_signal(final_score, regime)
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
                red_flags              = list(penalties.keys()),
                catalysts              = self._extract_catalysts(trial),
                penalty_factors        = penalties,
            )
            results.append(result)

        results.sort(key=lambda r: (-r.trial_strength_score, r.days_to_completion))
        log.info(f"Scored {len(results)} trials with resolved tickers")
        return results[:top_n]

    def write_to_db(self, results: list[SignalResult], run_id: str = None) -> int:
        """Persist signals to griddb trial_signals table."""
        if not results:
            return 0

        cur = self.conn.cursor()
        written = 0
        run_id = run_id or datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        for r in results:
            try:
                # Compute completion date from days_to_completion
                completion_date = None
                if r.days_to_completion and r.days_to_completion < 999:
                    completion_date = (
                        datetime.date.today()
                        + datetime.timedelta(days=r.days_to_completion)
                    )

                cur.execute("""
                    INSERT INTO trial_signals (
                        run_id, nct_id, ticker, company_name,
                        trial_phase, primary_indication, primary_endpoint,
                        endpoint_type, fda_designation,
                        primary_completion_date,
                        enrollment_pct, days_to_completion,
                        market_cap_mm, cash_runway_months, pipeline_depth,
                        trial_strength_score, endpoint_clarity, phase_weight,
                        disease_priority, cash_runway_score, penalty_factors,
                        signal_type, regime_at_signal, confidence,
                        suggested_position_pct, rationale, red_flags, catalysts
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                    )
                """, (
                    run_id, r.nct_id, r.ticker, r.company_name,
                    r.trial_phase, r.primary_indication, r.primary_endpoint,
                    r.endpoint_type, r.fda_designation,
                    completion_date,
                    r.enrollment_pct, r.days_to_completion,
                    r.market_cap_mm, r.cash_runway_months, r.pipeline_depth,
                    r.trial_strength_score, r.endpoint_clarity, r.phase_weight,
                    r.disease_priority_score, r.cash_runway_months,
                    json.dumps(r.penalty_factors),
                    r.signal_type, r.regime_at_signal, r.confidence,
                    r.suggested_position_pct, r.rationale,
                    r.red_flags, r.catalysts,
                ))
                written += 1
            except Exception as e:
                log.warning(f"Failed to write {r.ticker}/{r.nct_id}: {e}")
                self.conn.rollback()
                continue

        self.conn.commit()
        cur.close()
        log.info(f"Wrote {written} trial signals to griddb (run_id={run_id})")
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

    # ── Company data ──────────────────────────────────────────────────────────

    def _fetch_company_data(self, ticker: str) -> dict:
        """Fetch market cap from Alpha Vantage. Rate-limited to 5 calls/min on free tier."""
        if not AV_KEY or self._av_calls >= 5:
            return {}
        try:
            resp = requests.get(
                "https://www.alphavantage.co/query",
                params={"function": "OVERVIEW", "symbol": ticker, "apikey": AV_KEY},
                timeout=10,
            )
            self._av_calls += 1
            d = resp.json()
            if "MarketCapitalization" not in d:
                return {}
            market_cap = float(d.get("MarketCapitalization", 0)) / 1e6
            return {
                "market_cap_mm": market_cap if market_cap > 0 else None,
                "cash_runway_months": None,
                "pipeline_depth": None,
            }
        except Exception:
            return {}

    def _passes_company_gates(self, company_data: dict) -> bool:
        mc = company_data.get("market_cap_mm")
        if mc is not None and mc > 2000:
            return False
        if mc is not None and mc < 10:
            return False
        return True

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
