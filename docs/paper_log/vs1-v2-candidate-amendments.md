# VS1 v2 candidate amendments (not in force)

Status: **candidates only.** Nothing here changes VS1 v1. The v1 pre-registration
(`vs1-insider-density-v1-preregistration.md`, body sha256 pinned in
`analysis/panel_insider_density.py`) governs every v1 run exactly as written,
including the behaviour these items criticise. Adopting any item means writing a
v2 pre-registration with a new body hash, registered before any price read of
the scope it changes (v1 §15). After a VS1 price read, a change is not an
amendment but a new study on windows the ledger already marks as used (v1 §7).

Source: the independent review of PR #697 (head d1e13f6e, 2026-09-27), items 5-7,
which it rated non-blocking for v1. Its blocking items (1-4) and items 8-9 were
fixed in the harness, not here.

## C1. Ticker-reuse admission rule (review item 5)

**Problem.** v1 resolves each sector-map ticker to *today's* issuer CIK through
`company_tickers.json` (§2.2 rule 2) and reads prices by *today's* ticker (§2.3).
When a ticker was carried by a different company earlier in 2012-2026, the price
history under that ticker can belong to the earlier company while the Form 4
events and the Section 16 mask belong to today's issuer. Examples in the 88-ticker
list flagged by the review: **ESI** and **GEN**, both tickers that were used by
another issuer before the current one (exact reassignment dates to be confirmed
from the filings, see the rule below). v1 declares this only in general terms
("pre-change histories depend on the price source's ticker continuity", §2.2
Bias); it does not detect or exclude such spans.

**Candidate rule.** Admit an issuer-date only while the ticker demonstrably
belongs to that issuer:

- Build a point-in-time ticker interval per issuer CIK from its own Section 16
  filings: `ISSUERTRADINGSYMBOL` on the SUBMISSION table
  (`derived/submissions.parquet` already carries it as `issuer_ticker`). The
  interval for (CIK, ticker) runs from the first to the last filing that names
  that ticker, extended to the next filing naming another ticker.
- Before the interval starts, the issuer's feature abstains and no label is
  computed for it (listed per ticker in the run manifest, never silent).
- Cross-check against the admitted price source's own listing dates where the
  source provides them; a disagreement larger than N sessions excludes the span
  and is reported.
- Rule and its exclusions are computed before any price read (they use filings
  only), so they can be frozen with the other inputs.

**Why not in v1.** It changes the universe rule of §2.2 and therefore the trial
data; under v1 it would be an unregistered change.

## C2. Delisting-return sensitivity (review item 6)

**Problem.** v1 has no delisting returns. A label whose end close is missing is
excluded, counted and reported (§2.3 "No silent drops"), with a
`SURVIVORSHIP_WARNING` when more than 5% of the primary trial's buyer
issuer-dates lack a label. That flags the size of the hole but not its possible
effect on the IC: if insider-bought firms that later delisted for poor
performance are the missing labels, the buyer mean is biased upward.

**Candidate rule (reported only, never selects).** For every trial and window,
recompute the mean IC and the buyer-minus-non-buyer magnitude with each missing
label imputed under stated bounds:

- *pessimistic:* a missing buyer label = the worst observed relative return of
  that decision date (or a fixed -30%, the conventional performance-delisting
  proxy), a missing non-buyer label = the best;
- *neutral:* missing label = 0 relative return;
- *optimistic:* the mirror of pessimistic.

Report the three ICs next to the primary one, plus whether the sign of the
primary trial's discovery IC survives the pessimistic bound. If a delisting-reason
source becomes available (performance vs merger), replace the fixed bound with
reason-specific values. The selection statistic stays the v1 one.

**Why not in v1.** v1 fixes the reported-only list (§9); adding a new reported
statistic is harmless to the test but is still a change to the registered text.

## C3. A less noisy CONTRARY / MACHINERY_SUSPECT alarm (review item 7)

**Problem.** The v1 calibration alarm (§11) is sensitive to chance:

- `CONTRARY` fires when *any* of the 4 trials has mean IC < 0 with an unadjusted
  two-sided p <= 0.05. Under the global null each trial trips it with
  probability about 0.025, so the four together (correlated, two features x two
  horizons) raise it with probability up to about 0.10 -- and `CONTRARY` alone
  forces the verdict `MACHINERY_SUSPECT`.
- `ABSENT` while the Stage-0 gate passed also forces `MACHINERY_SUSPECT`. The
  gate only guarantees power >= 0.50 at IC 0.01 under an optimistic
  (idiosyncratic-noise) model, so a true IC near 0.01 can leave the primary
  discovery IC at or below 0 with non-trivial probability.

A machinery alarm raised this often teaches readers to ignore it.

**Candidate rules.**

- `CONTRARY` only when a trial's *negative* one-sided p survives Holm at the run
  alpha over the 4 trials (the same multiplicity the positive direction gets), or
  restrict the alarm to the primary trial at one-sided 0.025.
- Replace "ABSENT while powered" by a check on the primary trial's discovery IC
  against the lower end of the pre-stated expectation: alarm only when the
  primary IC is below 0 with one-sided p <= 0.05 in the negative direction,
  i.e. evidence *against* the expected sign, not merely absence of evidence for it.
- Keep `MACHINERY_SUSPECT` as the verdict name and its meaning (audit parse,
  dates, universe and prices before trusting anything else), and report the v1
  rule's outcome alongside for continuity.
- Calibrate the alarm's false-positive rate on the synthetic null of
  `tests/test_panel_insider_density.py` (four correlated trials with a persistent
  common factor) and state it in the v2 text.

**Why not in v1.** The calibration and verdict rules are part of the registered
body (§11); v1 runs with them unchanged and its verdict must be read with the
false-alarm rate above in mind.
