"""
grid/signals/trial_signal.py

GRID Signal Module: Clinical Trial Catalyst Intelligence

Ingests ClinicalTrials.gov data, scores trial quality,
and surfaces near-term catalyst opportunities as GRID signals.

Conforms to GRID signal interface:
  - generate() → list[SignalResult]
  - score is normalized [0, 1]
  - regime-aware (reads from regime_states)
  - writes to trial_signals (not features — separate domain)

Usage (standalone):
    python -m grid.signals.trial_signal --output jsonl

Usage (via GRID pipeline):
    from grid.signals.trial_signal import TrialGemSignal
    sig = TrialGemSignal(db_conn)
    results = sig.generate()
"""

from __future__ import annotations

import os
import json
import logging
import datetime
import requests
import psycopg2
import psycopg2.extras
from dataclasses import dataclass, asdict
from typing import Optional

log = logging.getLogger("grid.signals.trial_signal")

# ── Config ────────────────────────────────────────────────────────────────────

CT_GOV_BASE   = "https://clinicaltrials.gov/api/v2/studies"
AV_KEY        = os.getenv("ALPHA_VANTAGE_KEY", "SPT9IOAEYVUT7X6H")
EDGAR_SEARCH  = "https://efts.sec.gov/LATEST/search-index"

# Phases to screen
TARGET_PHASES = {"PHASE2", "PHASE3"}

# Disease areas with priority scores (higher = bigger abnormal return history)
DISEASE_PRIORITY = {
    "neoplasm":          1.00,
    "cancer":            1.00,
    "carcinoma":         1.00,
    "lymphoma":          1.00,
    "leukemia":          1.00,
    "glioblastoma":      1.00,
    "rare":              0.90,
    "orphan":            0.90,
    "cns":               0.80,
    "neurological":      0.80,
    "alzheimer":         0.80,
    "parkinson":         0.80,
    "autoimmune":        0.75,
    "inflammatory":      0.75,
    "diabetes":          0.65,
    "cardiovascular":    0.60,
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
    red_flags: list[str]
    catalysts: list[str]
    penalty_factors: dict


# ── Main signal class ─────────────────────────────────────────────────────────

class TrialGemSignal:
    """
    GRID signal module for clinical trial catalyst discovery.
    Plugs into the GRID pipeline alongside macro/momentum signals.
    Orthogonal domain: trial events are uncorrelated with FRED/EIA signals.
    """

    def __init__(self, db_conn=None, db_config: dict = None):
        if db_conn:
            self.conn = db_conn
        elif db_config:
            self.conn = psycopg2.connect(**db_config)
        else:
            self.conn = psycopg2.connect(
                host="localhost", port=5432,
                dbname="griddb", user="grid", password="grid2026"
            )

    # ── Public interface ──────────────────────────────────────────────────────

    def generate(self, top_n: int = 10) -> list[SignalResult]:
        """
        Main entry point. Returns ranked list of trial signal candidates.
        Regime-gated: only BUY in GROWTH/NEUTRAL, WATCHLIST otherwise.
        """
        regime = self._get_regime()
        log.info(f"Current GRID regime: {regime}")

        trials = self._fetch_candidates()
        log.info(f"Fetched {len(trials)} candidate trials from ClinicalTrials.gov")

        results = []
        for trial in trials:
            ticker, company = self._resolve_ticker(trial.sponsor)
            if not ticker:
                continue

            company_data = self._fetch_company_data(ticker)
            if not self._passes_company_gates(company_data):
                continue

            score_components = self._score_trial(trial, company_data)
            total_score = self._weighted_score(score_components)
            penalties, penalty_mult = self._apply_penalties(trial, company_data)
            final_score = total_score * penalty_mult

            signal_type = self._determine_signal(final_score, regime)
            confidence  = self._compute_confidence(final_score, regime)
            position    = self._position_size(final_score, confidence) if signal_type == "BUY" else None

            days_out = self._days_to_completion(trial.primary_completion)
            indication = self._extract_indication(trial.conditions)

            result = SignalResult(
                nct_id                 = trial.nct_id,
                ticker                 = ticker,
                company_name           = company or trial.sponsor,
                trial_phase            = trial.phase,
                primary_indication     = indication,
                primary_endpoint       = self._extract_endpoint(trial),
                endpoint_type          = self._endpoint_type(trial),
                fda_designation        = self._check_fda_designation(trial, ticker),
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
                catalysts              = self._extract_catalysts(trial, company_data),
                penalty_factors        = penalties,
            )
            results.append(result)

        # Sort by trial strength, then by proximity to readout
        results.sort(key=lambda r: (-r.trial_strength_score, r.days_to_completion))
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
                cur.execute("""
                    INSERT INTO trial_signals (
                        run_id, nct_id, ticker, company_name,
                        trial_phase, primary_indication, primary_endpoint,
                        endpoint_type, fda_designation,
                        enrollment_pct, days_to_completion,
                        market_cap_mm, cash_runway_months, pipeline_depth,
                        trial_strength_score, endpoint_clarity, phase_weight,
                        disease_priority, cash_runway_score, penalty_factors,
                        signal_type, regime_at_signal, confidence,
                        suggested_position_pct, rationale, red_flags, catalysts
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                    )
                """, (
                    run_id, r.nct_id, r.ticker, r.company_name,
                    r.trial_phase, r.primary_indication, r.primary_endpoint,
                    r.endpoint_type, r.fda_designation,
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
                log.warning(f"Failed to write {r.ticker}: {e}")
                self.conn.rollback()
                continue

        self.conn.commit()
        cur.close()
        log.info(f"Wrote {written} trial signals to griddb (run_id={run_id})")
        return written

    # ── ClinicalTrials.gov fetch ───────────────────────────────────────────────

    def _fetch_candidates(self) -> list[TrialRecord]:
        trials = []
        params = {
            "filter.overallStatus": "ACTIVE_NOT_RECRUITING",
            "filter.phase":         "PHASE2,PHASE3",
            "filter.studyType":     "INTERVENTIONAL",
            "fields": (
                "NCTId,BriefTitle,Condition,InterventionName,Phase,"
                "EnrollmentCount,PrimaryCompletionDate,StartDate,"
                "SponsorName,LeadSponsorClass,OverallStatus,WhyStopped,"
                "ResultsFirstPostDate,DesignPrimaryPurpose"
            ),
            "pageSize": 100,
        }
        try:
            resp = requests.get(CT_GOV_BASE, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            studies = data.get("studies", [])

            for s in studies:
                proto = s.get("protocolSection", {})
                id_mod = proto.get("identificationModule", {})
                status_mod = proto.get("statusModule", {})
                sponsor_mod = proto.get("sponsorCollaboratorsModule", {})
                design_mod = proto.get("designModule", {})
                cond_mod = proto.get("conditionsModule", {})
                interv_mod = proto.get("armsInterventionsModule", {})

                nct_id = id_mod.get("nctId", "")
                if not nct_id:
                    continue

                # Parse dates
                pc_date = self._parse_date(
                    status_mod.get("primaryCompletionDateStruct", {}).get("date")
                )
                start_date = self._parse_date(
                    status_mod.get("startDateStruct", {}).get("date")
                )

                # Only want readouts within 30–180 days
                if pc_date:
                    days = (pc_date - datetime.date.today()).days
                    if not (30 <= days <= 180):
                        continue

                trial = TrialRecord(
                    nct_id          = nct_id,
                    title           = id_mod.get("briefTitle", ""),
                    sponsor         = sponsor_mod.get("leadSponsor", {}).get("name", ""),
                    sponsor_class   = sponsor_mod.get("leadSponsor", {}).get("class", ""),
                    phase           = design_mod.get("phaseList", {}).get("phase", [""])[0] if design_mod.get("phaseList") else "",
                    status          = status_mod.get("overallStatus", ""),
                    conditions      = cond_mod.get("conditionList", {}).get("condition", []),
                    interventions   = [
                        i.get("interventionName", "")
                        for i in interv_mod.get("interventionList", {}).get("intervention", [])
                    ],
                    enrollment_target = design_mod.get("enrollmentInfo", {}).get("count"),
                    enrollment_actual = design_mod.get("enrollmentInfo", {}).get("count"),  # best available
                    primary_completion = pc_date,
                    start_date      = start_date,
                    why_stopped     = status_mod.get("whyStopped"),
                    has_results     = bool(s.get("resultsSection")),
                )

                # Exclude terminated/stopped trials
                if trial.why_stopped:
                    continue
                # Exclude INDUSTRY-only filter
                if trial.sponsor_class not in ("INDUSTRY", ""):
                    continue

                trials.append(trial)

        except Exception as e:
            log.error(f"ClinicalTrials.gov fetch failed: {e}")

        return trials

    # ── Ticker resolution ──────────────────────────────────────────────────────

    def _resolve_ticker(self, sponsor_name: str) -> tuple[Optional[str], Optional[str]]:
        """
        Map sponsor name → equity ticker via EDGAR company search.
        Returns (ticker, company_name) or (None, None).
        """
        try:
            resp = requests.get(
                "https://efts.sec.gov/LATEST/search-index",
                params={"q": f'"{sponsor_name}"', "dateRange": "custom",
                        "startdt": "2020-01-01", "forms": "10-K,10-Q"},
                timeout=10
            )
            hits = resp.json().get("hits", {}).get("hits", [])
            if hits:
                entity = hits[0].get("_source", {})
                ticker = entity.get("period_of_report", "")  # fallback
                # Try extracting ticker from entity_name field
                name = entity.get("entity_name", sponsor_name)
                return ticker or None, name
        except Exception:
            pass
        return None, None

    # ── Company data ───────────────────────────────────────────────────────────

    def _fetch_company_data(self, ticker: str) -> dict:
        """Fetch market cap, cash runway from Alpha Vantage overview."""
        try:
            resp = requests.get(
                "https://www.alphavantage.co/query",
                params={"function": "OVERVIEW", "symbol": ticker, "apikey": AV_KEY},
                timeout=10
            )
            d = resp.json()
            market_cap = float(d.get("MarketCapitalization", 0)) / 1e6
            return {
                "market_cap_mm": market_cap if market_cap > 0 else None,
                "cash_runway_months": None,   # requires 10-Q parsing
                "pipeline_depth": None,
                "52w_change": float(d.get("52WeekHigh", 0)),
                "30d_price_change": None,
            }
        except Exception:
            return {}

    def _passes_company_gates(self, company_data: dict) -> bool:
        """Hard filters on company quality."""
        mc = company_data.get("market_cap_mm")
        if mc and mc > 2000:   # > $2B = too big, lower move potential
            return False
        if mc and mc < 10:     # < $10M = too small, likely shell
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

        cash_score = 0.5  # default when unknown
        if company_data.get("cash_runway_months"):
            cash_score = min(1.0, company_data["cash_runway_months"] / 24)

        endpoint_c = self._endpoint_clarity_score(trial)
        fda_score  = 1.0 if self._check_fda_designation(trial, "") else 0.0

        return {
            "endpoint_clarity": endpoint_c,
            "phase_weight":     phase_w,
            "disease_priority": disease_score,
            "enrollment_pct":   enroll_pct,
            "fda_designation":  fda_score,
            "cash_runway":      cash_score,
        }

    def _weighted_score(self, components: dict) -> float:
        total = 0.0
        for key, weight in SCORE_WEIGHTS.items():
            total += components.get(key, 0.0) * weight
        return round(min(1.0, total), 4)

    def _apply_penalties(self, trial: TrialRecord, company_data: dict) -> tuple[dict, float]:
        """Apply multiplicative penalties for known failure signals."""
        penalties = {}
        mult = 1.0

        # Enrollment duration check
        if trial.start_date and trial.primary_completion:
            planned_months = 24  # assume 24mo as baseline
            actual_months = (datetime.date.today() - trial.start_date).days / 30
            if actual_months > planned_months * 2:
                penalties["slow_enrollment"] = 0.6
                mult *= 0.6

        # High short interest (proxy: skip if we can't get data)
        si = company_data.get("short_interest_pct")
        if si and si > 30:
            penalties["high_short_interest"] = 0.6
            mult *= 0.6

        # Trial already has results posted (news is out)
        if trial.has_results:
            penalties["results_already_posted"] = 0.1
            mult *= 0.1

        return penalties, mult

    def _disease_priority(self, conditions: list[str]) -> float:
        text = " ".join(conditions).lower()
        for keyword, priority in DISEASE_PRIORITY.items():
            if keyword in text:
                return priority
        return 0.50   # unknown indication

    def _endpoint_clarity_score(self, trial: TrialRecord) -> float:
        title = trial.title.lower()
        if any(t in title for t in ["overall survival", " os ", "progression-free",
                                     "complete response", " pfr ", "event-free"]):
            return 1.0
        if any(t in title for t in ["composite", "combined", "multiple"]):
            return 0.7
        if any(t in title for t in ["biomarker", "surrogate", "imaging"]):
            return 0.5
        return 0.4   # default unknown

    def _check_fda_designation(self, trial: TrialRecord, ticker: str) -> str:
        title = trial.title.lower()
        if "breakthrough" in title:
            return "Breakthrough Therapy"
        if "fast track" in title:
            return "Fast Track"
        if "orphan" in title:
            return "Orphan Drug"
        return ""

    # ── Signal determination ───────────────────────────────────────────────────

    def _determine_signal(self, score: float, regime: str) -> str:
        if regime in FAVORABLE_REGIMES:
            if score >= 0.65:
                return "BUY"
            elif score >= 0.40:
                return "WATCHLIST"
            else:
                return "AVOID"
        else:
            # FRAGILE or CRISIS — never BUY, only WATCHLIST high quality
            if score >= 0.70:
                return "WATCHLIST"
            return "AVOID"

    def _compute_confidence(self, score: float, regime: str) -> float:
        regime_adj = {"GROWTH": 1.0, "NEUTRAL": 0.9, "FRAGILE": 0.6, "CRISIS": 0.4}
        return round(score * regime_adj.get(regime, 0.8), 4)

    def _position_size(self, score: float, confidence: float) -> float:
        """Kelly-inspired position sizing: max 5% for any single trial bet."""
        base = score * confidence * 0.05
        return round(min(0.05, max(0.005, base)), 4)

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _get_regime(self) -> str:
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT regime FROM regime_states ORDER BY timestamp DESC LIMIT 1")
            row = cur.fetchone()
            cur.close()
            return row[0] if row else "UNKNOWN"
        except Exception:
            return "UNKNOWN"

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime.date]:
        if not date_str:
            return None
        for fmt in ("%Y-%m-%d", "%B %Y", "%Y"):
            try:
                d = datetime.datetime.strptime(date_str, fmt).date()
                if fmt == "%B %Y" or fmt == "%Y":
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
        return conditions[0] if conditions else "Unknown"

    def _extract_endpoint(self, trial: TrialRecord) -> str:
        title = trial.title.lower()
        for ep in ["overall survival", "progression-free survival",
                   "complete response", "event-free survival",
                   "objective response rate"]:
            if ep in title:
                return ep.title()
        return "See Protocol"

    def _endpoint_type(self, trial: TrialRecord) -> str:
        score = self._endpoint_clarity_score(trial)
        if score >= 0.9:  return "binary"
        if score >= 0.6:  return "composite"
        if score >= 0.4:  return "surrogate"
        return "other"

    def _build_rationale(self, trial: TrialRecord, components: dict, regime: str) -> str:
        parts = []
        parts.append(f"Phase {trial.phase}, readout {trial.primary_completion}")
        parts.append(f"Indication: {self._extract_indication(trial.conditions)}")
        parts.append(f"Endpoint clarity: {components.get('endpoint_clarity', 0):.2f}")
        parts.append(f"Enrollment: {components.get('enrollment_pct', 0)*100:.0f}%")
        parts.append(f"Regime: {regime}")
        return " | ".join(parts)

    def _extract_catalysts(self, trial: TrialRecord, company_data: dict) -> list[str]:
        catalysts = []
        if trial.primary_completion:
            catalysts.append(f"Primary readout: {trial.primary_completion}")
        if self._check_fda_designation(trial, ""):
            catalysts.append(f"FDA designation: {self._check_fda_designation(trial, '')}")
        return catalysts


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="GRID Trial Gem Signal Generator")
    parser.add_argument("--output", choices=["jsonl", "table", "db"], default="table")
    parser.add_argument("--top-n", type=int, default=10)
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
        print(f"\n{'='*90}")
        print(f"{'GRID Trial Gem Hunter':^90}")
        print(f"{'='*90}")
        print(f"{'TICKER':<8} {'INDICATION':<18} {'PHASE':<8} {'SCORE':<7} "
              f"{'SIGNAL':<10} {'DAYS':<6} {'REGIME'}")
        print(f"{'-'*90}")
        for r in results:
            print(f"{r.ticker:<8} {r.primary_indication:<18} {r.trial_phase:<8} "
                  f"{r.trial_strength_score:<7.3f} {r.signal_type:<10} "
                  f"{r.days_to_completion:<6} {r.regime_at_signal}")
        print(f"{'='*90}\n")
