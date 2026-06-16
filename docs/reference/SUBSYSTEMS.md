# Subsystems — Options, Oracle, Trial Gem Hunter (reference)

> Load when: working on options recommendations, the oracle prediction engine, or the
> clinical-trial signal domain.

## Options Edge

- `trading/options_recommender.py` — generates specific trade recommendations (strike,
  expiry, entry, target, stop, Kelly)
- `trading/options_tracker.py` — outcome tracking + self-improving scanner weights
- `discovery/options_scanner.py` — 7-signal [[Options Scanner|mispricing detector]] (now
  with LLM sanity check)
- `physics/dealer_gamma.py` — [[Dealer Gamma|GEX]], [[Dealer Gamma|vanna]], charm,
  [[Dealer Gamma|gamma walls]]

## Oracle Engine

- `oracle/engine.py` — 5 competing models, signal/anti-signal weighting, dynamic weight
  evolution
- `oracle/calibration.py` — [[Oracle Calibration|Brier score]], expected calibration
  error (ECE), reliability metrics
- `oracle/report.py` — email digest sent after each prediction cycle
- **615 predictions locked, scoring begins Apr 17 2026**
- Runs every 6 hours via [[Hermes Scheduler|Hermes operator]]

## Trial Gem Hunter (Clinical Trial Signal Domain)

Orthogonal signal domain: ClinicalTrials.gov Phase 2/3 → biotech equity prediction.

- `grid/signals/trial_signal.py` — main signal class (score, regime gate, position sizing)
- `grid/ingestors/trial_ingestor.py` — daily CT.gov ingestor (cron job #9, 6am)
- `grid/scripts/migrations/add_trial_signals.sql` — DB schema (trial_signals, trial_cache,
  catalyst_calendar)
- `tasks/trial-gem-hunter/` — AutoAgent self-improvement harness

### Signal Logic
1. Fetch Phase 2/3 trials (ACTIVE_NOT_RECRUITING, readout 30-180d, industry sponsor,
   mcap < $2B)
2. Score: endpoint clarity x phase x disease priority x enrollment x FDA flags
3. Regime gate: BUY only in GROWTH/NEUTRAL, WATCHLIST in FRAGILE/CRISIS
4. Position sizing: Kelly-inspired, max 5% per trial bet

### DB Tables
- `trial_signals` — scored picks
- `trial_cache` — raw CT.gov JSON (24h TTL)
- `catalyst_calendar` — upcoming readout dates
- Views: `trial_gems`, `trial_signal_performance`, `upcoming_catalysts`
