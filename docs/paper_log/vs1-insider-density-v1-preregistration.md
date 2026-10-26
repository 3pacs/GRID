# VS1 pre-registration: Technology insider open-market-buy density vs XLK (v1)

The body between the two markers below is the pre-registration. Its sha256 (the
UTF-8 bytes strictly between the markers, line endings normalised to LF) is
pinned in `analysis/panel_insider_density.py` as `PREREG_BODY_SHA256` and is
recorded in the trailer after the end marker. Any change to the body is a new
version (v2) with a new hash; this version then stays in the registry as
superseded.

<!-- PREREG-BODY-START -->

## 0. Status and what was seen before writing this

- Written 2026-09-27, before any price, return or outcome was read. `raw_series`
  was not queried. No Form 4 row of any Technology issuer was opened: the Form
  3/4/5 file was known only through its download report (schema, whole-file row
  counts, code distribution, lag summary for all issuers pooled).
- Read before writing: the granular-discovery plan
  (`GRID-GRANULAR-DISCOVERY-PLAN-20260927.md`), the GD0 security-master audit
  (`GRID-GD0-SECURITY-MASTER-AUDIT-20260927.md`), the Form 3/4/5 download report
  (`GRID-SEC-FORM345-DOWNLOAD-20260927.md`), the trade_edge insider
  paper-tracker audit (`GRID-TRADE-EDGE-TRACKER-AUDIT-20260927.md`; its
  statistics concern a different construct, recent 2026 events and a mostly
  small-cap sample, and are used only as a calibration reference in §11),
  `analysis/sector_map_data.yaml`
  (tickers and weights only), and the repository's statistical code
  (`analysis/offline_research_proof.py`, `analysis/ledger_steered_exploration.py`,
  `analysis/research_forward_log.py`).
- Harness: `analysis/panel_insider_density.py` and
  `scripts/run_vs1_insider_density.py`. Where this text and the code differ, this
  text governs, and the run is invalid until the code is fixed.
- Nothing produced under this pre-registration is a trading signal.
  `promotion_allowed` is false in every artifact.

## 1. Question and directional hypothesis

Within the Technology sector, does the number of distinct insiders who recently
bought their company's stock on the open market (Form 4, transaction code P),
dated by when the filing became public, predict the stock's forward return
relative to XLK?

- H1 (pre-registered direction, +1): more recent distinct insider buyers →
  higher forward return relative to XLK.
- H0: the mean cross-sectional rank correlation between the feature and the
  forward relative return is 0.

## 2. Data

### 2.1 Insider events (off-DB file)

- Source: SEC DERA "Insider Transactions Data Sets" (structured Forms 3/4/5),
  2006Q1–2026Q2, derived file
  `/data/sec/form345/derived/nonderiv_transactions.parquet` on grid-svr (one row
  per non-derivative transaction line × named reporting owner). Its exact sha256
  is recorded in every run receipt; a different file is a different input.
- Columns used: `accession_number`, `filing_date`, `issuer_cik`,
  `document_type`, `amended`, `owner_cik`, `nonderiv_trans_sk`,
  `transaction_date`, `transaction_code`, `shares`, `price_per_share`,
  `acquired_disposed_code`.
- A **purchase** is a row with all of:
  1. `transaction_code` = `P`;
  2. `document_type` = `4` exactly (4/A, 5, 5/A and 3 excluded) and `amended` not
     true;
  3. `acquired_disposed_code` = `A`;
  4. 0 ≤ `filing_date` − `transaction_date` ≤ 365 days;
  5. `shares` ≥ 100, `price_per_share` > 0 and `shares` × `price_per_share` ≥
     $10,000 (tiny and unpriced trades excluded);
  6. an equity-swap flag or a 10b5-1 flag, **if** the file carries one, is not
     true. The derived file carries neither, so **10b5-1 purchases cannot be
     identified and are not excluded** (declared dilution; plan purchases are
     rare among open-market buys). Footnote text is not parsed.
- **Actor:** the smallest reporting-owner CIK on the accession (joint filers of
  one accession are one actor).
- **De-duplication:**
  (a) the owner fan-out of one line collapses on (`accession_number`,
  `nonderiv_trans_sk`); (b) one economic purchase reported in several
  accessions (joint filers filing separately) collapses on (issuer CIK,
  transaction date, shares rounded to 1, price rounded to $0.01), keeping the
  earliest filing date and the smallest actor CIK. Two different insiders buying
  the identical share count at the identical price on the same day would be
  merged; this is accepted.
- **known_at** = `filing_date` at 22:00 America/New_York. The data set carries
  no acceptance time; under Reg S-T Rule 13(a)(4) a Section 16 form submitted by
  22:00 ET receives that day's filing date, so 22:00 ET is the latest moment the
  filing can have become public on its filing date.
- **Section 16 activity:** every accession (any code, any form type, amended or
  not) of the issuer, known_at as above.
- Direction comes only from `transaction_code` and `acquired_disposed_code` of
  the SEC data. No vendor buy/sell label (e.g. QuiverQuant `signal_type`, which
  tags thousands of code-P rows as sales) and no GRID `insider_trades` row
  (no filing date, no owner CIK) is used anywhere.

### 2.2 Universe (Technology membership) and its bias

An issuer is in the VS1 universe at decision t iff all hold:

1. **Sector-map primary sector = Technology.** From
   `analysis/sector_map_data.yaml` with LF sha256
   `2d262fe1a8ab4fbfe7abbde86c947c3ff35c12c49f00f4217b24e0dd3af3cdba` (origin/main
   1bb2f61b); the harness refuses any other file. For each ticker with
   `type: company` entries, the score of a sector is the largest actor `weight`
   among the ticker's entries in that sector. The primary sector is the unique
   argmax. **A tie between sectors excludes the ticker from every sector**
   (Technology ties excluded: ALB, LAC, MP, SQM, UUUU). Multi-sector tickers that
   carry a Technology entry but whose primary sector is another sector: AMZN,
   APD, BABA, BYDDF, GOOGL, LIN, META.
   Resulting 88 Technology tickers: AAPL ACLS ADBE ADI ADSK AMAT AMD ANET ARM
   ASAN ASML AVGO BIDU CAMT CDNS CFLT CHKP CIEN CRM CRWD CSCO CVLT CYBR DDOG DELL
   DOCU ENTG ESI ESTC FORM FTNT GEN GFS GPRO GRMN HPE HPQ HUBS IBM ICHR INTC INTU
   JNPR KLAC LITE LOGI LRCX MCHP MDB MKSI MNDY MRVL MSFT MU NBIS NET NOW NTAP NVDA
   NXPI OKTA ON ONTO ORCL PANW PLTR PSTG QCOM QLYS RPD S SAP SHOP SMCI SNOW SNPS
   SONY TEAM TENB TSLA TSM TXN UCTT VEEV WDAY WOLF ZM ZS.
2. **CIK:** the ticker resolves to an issuer CIK through SEC
   `company_tickers.json` (the issuer map; its sha256 is recorded per run).
   Tickers without a CIK are dropped and listed. When two member tickers share a
   CIK, the alphabetically first one represents the issuer.
3. **Section 16 filer at t:** the issuer has at least one accession with known_at
   in (t − 730 days, t]. This is point-in-time and outcome-free; it removes
   foreign private issuers (which do not file Forms 3/4/5) and issuers before
   their first filing or long after their last.
4. **Price-admitted and trading:** the ticker is on the admitted-price manifest
   (§2.3) and has a close on the decision session (else the feature abstains) and
   on the label-end session (else the label is missing).

**Bias (declared):** the sector map is hand-curated today and undated, so
membership is today's list applied to 2012–2026. It contains firms that became
large or relevant by 2026 and omits Technology firms that failed, were acquired
or never became prominent; four members (CFLT, CYBR, JNPR, PSTG) no longer
resolve to a live CIK (GD0 §2) and drop out at rule 2. Current tickers are used
for prices, so pre-change histories depend on the price source's ticker
continuity. If insider-bought firms that later collapsed are missing, the
insider-buy association is biased upward; the universe is also tilted to large
capitalisations, where the published effect is weakest. Results are statements
about this universe only.

### 2.3 Prices (not read yet)

- Admission comes from the GD4 basis probe after the #671 quarantine: per ticker
  a single source, zero multi-valued dates, split-consistent against TIINGO or
  TWELVEDATA splits within tolerance, no April-2026 bulk-batch rows. The admitted
  list (XLK included), its source, series-id template and basis are frozen in a
  price manifest (with the probe report's sha256) **before** the discovery run;
  tickers not admitted are excluded before any label is computed and listed in
  the run manifest.
- Refused sources: `yfinance` and the Kaggle bulk load, whatever the series id.
- Reads go only through `store.observations.read_window(conn, series_id,
  source=<manifest source>, start, as_of, as_of_ts)` on a read-only session
  (SUCCESS rows, one row per date, pulled no later than the run's `as_of_ts`).
  Discovery reads stop at 2019-12-31; holdout reads require the holdout key
  (§8).
- Basis: every series (constituents and XLK) on the manifest's single declared
  basis (split- and dividend-adjusted closes if the admitted source provides them
  for all series, else split-adjusted closes for all series).
- **No silent drops.** Delisting returns are not available, so a label whose end
  close is missing is excluded from the statistic, but it is counted and
  reported per trial and window: issuer-dates with a feature but no label, and
  the same among buyer issuer-dates (A > 0). If more than 5% of the primary
  trial's buyer issuer-dates in either window lack a label, the verdict carries
  a `SURVIVORSHIP_WARNING`. Tickers dropped for no CIK, no admitted price or a
  tie are listed in the run manifest.

## 3. Timing

- **Sessions:** the dates on which XLK has an admitted close.
- **Decision instant:** 16:00 America/New_York on a session date. An event
  counts at decision t iff its known_at ≤ t. Because known_at is 22:00 ET on the
  filing date, a filing first counts at the close of the first session after its
  filing date.
- **Entry and label:** entry at the decision session's close; label = issuer
  close-to-close return from session t to session t + h minus XLK's
  close-to-close return over the same sessions, both legs on identical session
  bars. (Entry at the next open is not possible with close-only prices; the
  close is later and therefore conservative.) No entry ever uses a close printed
  before the filing was public: the trade_edge tracker audit measured that such
  entries inflated its v1 result.
- **Horizons:** h ∈ {5, 20} sessions.
- **Horizon spacing:** decisions every h sessions from the first session of the
  window, so outcome windows never overlap.
- **Windows:** discovery decisions from 2012-01-01 with label end before
  2020-01-01; holdout decisions from 2020-01-01 with label end before
  2026-07-01. Every close outside the window is blanked before labelling (split
  first, then label); a label that would need an out-of-window close does not
  exist. Data after 2026-06-30 is reserved for forward evaluation.

## 4. Features

For issuer e at decision t, over purchases with known_at in (t − W, t]:

`A(e, t; W, τ) = Σ over distinct actors a of exp(−(t − k_a) / τ)`, where k_a is
actor a's latest qualifying known_at for e and ages are in fractional days. Each
actor counts once. A = 0 with no qualifying purchase; the feature abstains (NaN)
when e is not a Section 16 filer at t or has no close at t.

- `A90`: W = 90 days, τ = 45 days.
- `A30`: W = 30 days, τ = 15 days.

`D_peer` (the within-sector percentile of A) is a monotone transform of A on
each date, so its rank correlation equals A's; it is not a separate trial.

## 5. Declared trials (4)

| Trial | Feature | Horizon |
|---|---|---|
| `A90|fwd5` | A90 | 5 sessions |
| `A90|fwd20` (**primary**) | A90 | 20 sessions |
| `A30|fwd5` | A30 | 5 sessions |
| `A30|fwd20` | A30 | 20 sessions |

No other feature, horizon, window, universe or rule is tested under this
version.

## 6. Statistic and null

- **Per-date statistic:** Spearman rank correlation (average ranks for ties)
  across issuers with both a feature and a label on the date. The date abstains
  when fewer than 20 issuers qualify or when the feature or the label is constant
  across them (e.g. no buyer anywhere).
- **Trial statistic:** the mean of the per-date ICs over non-abstaining dates.
  The rank IC is invariant to subtracting a return common to all issuers, so the
  test does not depend on the benchmark; XLK enters the reported magnitudes.
- **Minimum:** 30 non-abstaining dates, else the trial is `insufficient_data`
  with p = 1.
- **Null (selects):** block sign-flip of the per-date IC series. Blocks are
  contiguous in decision order; the block length comes from the repository rule
  `autocorrelation_block` applied to the discovery IC series (overlap depth 0;
  capped so at least 8 blocks remain; caveats recorded). Two-sided p =
  (1 + #{|null mean| ≥ |observed mean|}) / (perms + 1), 20,000 sign-flip draws,
  seed 20260927, seeded by (seed, n, block, perms) only. Valid under H0 when the
  IC series is sign-symmetric within blocks.
- The one-sided p in the pre-registered direction (+1) is computed with the same
  draws.

## 7. Selection and multiplicity

- VS1 is **run k = 1** of a new S11-style panel ledger `grid-granular-panel`
  with global level q = 0.10. It is issued α₁ = q / (1 · 2) = **0.05**.
- **Selection:** Holm's step-down at α₁ over all 4 declared trials, untestable
  trials included at p = 1 (repository `holm_adjusted`). Holm is valid under any
  dependence between the 4 trials.
- BH-adjusted p-values (q = 0.10, the plan's rule) are computed over the same 4
  trials and **reported only**; they select nothing.
- The other 10 sectors (§13) are run k = 2 of the same ledger, α₂ = 0.10/6.
  Because α_k sums to less than q, the probability of any false discovery-stage
  selection across every run this ledger ever records stays below 0.10 (nominal:
  it assumes valid null p-values).
- The ledger's windows are single-use: no trial identity (sector, feature,
  horizon, target) may be re-tested on any part of 2012-01-01 → 2026-07-01 under
  another version.

## 8. Holdout

- The holdout is opened only by an explicit `allow_holdout=True` together with
  this body's sha256; the harness also requires the repository copy of this text
  to re-hash to the same value and the frozen discovery manifest to carry it.
  Holdout prices cannot be read without that key.
- Evaluated once: every trial selected in discovery, and the primary trial (for
  the §13 generalization gate) whether selected or not.
- Block: the frozen discovery block, capped so at least 8 blocks remain.
- **Retrospective survivor:** a selected trial with Bonferroni-adjusted holdout
  p (two-sided p × number of selected trials) ≤ 0.05 and the same sign as in
  discovery.
- The plan's secondary entity-split holdout (70/30 by CIK hash) is **not used**:
  with at most 88 issuers it would cut discovery power by 30% for a check this
  small a universe cannot power.

## 9. Reported only (never select)

For each trial on each window: IC standard deviation, share of positive dates,
median issuers per date; a time-alignment block-permutation p (outcome
cross-sections permuted over dates in blocks, feature cross-sections fixed, the
direct analogue of the time-series loop's null); a within-date issuer-shuffle
p; the buyer magnitude (mean over dates of the mean relative return of issuers
with A > 0, of issuers with A = 0, and their difference, plus buyer issuer-date
counts); and a momentum baseline (mean IC of the past-20-session relative return,
and the mean cross-sectional rank correlation between A and that momentum,
because insiders tend to buy after declines and short-term reversal could
masquerade as an insider effect). Momentum is not a trial and is not in the Holm
denominator. A size stratum is also reported: the buyer relative return split by
whether the issuer's largest qualifying purchase line in the window is at least
$500,000 (the trade_edge tracker's construct) or smaller. The stratum never
selects and is not a trial.

## 10. Stage-0 power gate (before any price read)

- Computed from the Form 4 feature panel only, on proxy sessions (weekdays that
  are not US federal holidays) over the discovery window, with synthetic outcomes
  planted at mean rank IC 0.01, 0.02 and 0.03 (per date: planted signal on the
  standardised feature rank plus independent Gaussian noise; 200 simulations,
  999 sign-flip draws, success = two-sided p ≤ α₁/4 = 0.0125, the smallest Holm
  threshold). It ignores common factors and price-availability losses, so it is
  optimistic.
- **Gate:** power of the primary trial at IC 0.01 ≥ 0.50.
- If the gate fails, the run **halts before any price read**. The owner then
  chooses either (a) a v2 pre-registration with an expanded Technology universe
  (sector-map members plus issuers whose SEC SIC code is in 3570–3579, 3660–3679
  or 7370–7379, which needs a CIK→SIC source), written before any VS1 price is
  read; or (b) running this v1 as a declared underpowered calibration run
  (`--accept-underpowered`, recorded in the manifest). Deciding on power is
  allowed because power uses no outcome.
- Expectation, stated in advance: with at most ~70 US Section 16 filers in the
  universe during 2012–2019, few of them large-cap issuers with open-market
  insider buying, the gate is **more likely to fail than pass**.

## 11. Calibration expectation and verdict

**Published prior (paper names only):** Seyhun, "Insiders' profits, costs of
trading, and market efficiency" (1986); Lakonishok and Lee, "Are insider trades
informative?" (2001); Jeng, Metrick and Zeckhauser, "Estimating the returns to
insider trading: a performance-evaluation perspective" (2003); Cohen, Malloy and
Pomorski, "Decoding inside information" (2012). Together they report that
insider purchases, unlike sales, are followed by positive abnormal returns, that
the effect is concentrated in small firms and in non-routine ("opportunistic")
purchases, and that it is weak among large firms.

**Expected effect in this universe (stated before looking):**
- Direction: positive.
- Buyer-minus-non-buyer relative return: between 0 and +1.0% per 20 sessions
  (roughly 0 to +0.5% per 5 sessions). The lower half of that range is the most
  likely because of the large-capitalisation tilt, post-2010 decay and no
  routine/opportunistic split.
- Mean rank IC: 0.00 to 0.03 at 20 sessions, 0.00 to 0.02 at 5 sessions.
- In-house reference (not evidence for VS1): the trade_edge tracker, after its
  audit's corrections (point-in-time entry, one position per event), shows a
  mean 30-session excess of about +3.4% net of costs, a median of −0.35% and a
  clustered t of about 1.0, driven by micro-caps; its issuers of $2B and above
  show about +1.3% with a clustered t of 0.09. That is consistent with the
  expectation above that a large-cap Technology universe carries at most a weak
  effect.

**Calibration state (discovery only, written into the frozen manifest):**
- `CONTRARY`: any trial with mean IC < 0 and two-sided p ≤ 0.05.
- `CONSISTENT`: the primary trial has mean IC > 0 with one-sided p ≤ 0.10, or a
  selected trial has mean IC > 0.
- `WEAK_POSITIVE`: the primary mean IC is > 0 but neither of the above.
- `ABSENT`: otherwise.

**Verdict (after the holdout):**
- `MACHINERY_SUSPECT` if the calibration is `CONTRARY`, or any holdout survivor
  has a negative IC, or the calibration is `ABSENT` while the Stage-0 gate passed.
  Meaning: before any other result of this program is trusted, audit the event
  parse, dates, universe and prices. It is not a claim that the published effect
  is false.
- `HOLDOUT_SURVIVOR_FORWARD_PENDING` if at least one retrospective survivor has
  a positive IC.
- `NO_SURVIVOR` otherwise, marked `UNDERPOWERED` when the Stage-0 gate did not
  pass (then a null is not evidence against the published effect).

## 12. After the verdict: the forward tracker

- A survivor gets a forward-admission file. The S10 forward log (v1) is
  time-series only, so VS1's forward log is a separate panel tracker ("tracker
  v2" of the trade_edge audit). It is not built under this version; its own
  pre-registration must adopt these rules unchanged:
  - the event rules of §2.1 and the universe rule of §2.2;
  - known_at = the later of the filing's public time (EDGAR acceptance time
    when available, else filing date 22:00 ET) and GRID's first ingest of the
    filing;
  - one position per (issuer, entry session), the entry session being the first
    session whose close is strictly after known_at, carrying the distinct
    actors, purchase count, total and largest line (harness function
    `entry_positions`); never one position per insider-day line;
  - both legs (issuer and XLK) on the same admitted, adjusted source and the same
    bars; every candidate recorded as opened, no_price, unresolved_ticker or
    late_filing, and delisted or unpriceable positions closed by a rule declared
    in that pre-registration, never left open;
  - the survivor's feature, horizon and direction; decisions every h sessions
    from the first session after 2026-06-30; one look after 30 non-abstaining
    forward decision dates; one-sided block sign-flip test at 0.05 divided by the
    number of survivors;
  - append-only, hash-chained records (the `research_forward_log` mechanism).
- No survivor is a trading signal, weight or promotion.

## 13. Pre-registration of the other 10 equity sectors

Frozen now, before any of them is looked at. Nothing in §§1–12 may be retuned
for them, whatever VS1 shows.

- **Sectors and benchmarks:** Energy XLE, Financials XLF, Healthcare XLV,
  Industrials XLI, Consumer Discretionary XLY, Consumer Staples XLP, Real Estate
  XLRE, Utilities XLU, Communication Services XLC, Materials XLB.
- **Everything else as VS1:** event rules (§2.1), universe rule with sector S in
  place of Technology (§2.2; membership counts under the tie rule: Energy 97,
  Financials 118, Healthcare 133, Industrials 99, Consumer Discretionary 154,
  Consumer Staples 124, Real Estate 97, Utilities 67, Communication Services 56,
  Materials 75), price admission (§2.3), timing (§3), features (§4), the same 4
  trials per sector with primary trial `A90|fwd20` and direction +1 (§5),
  statistic and null (§6), holdout (§8), reported-only items (§9), Stage-0 power
  recorded per sector (§10).
- **Session calendar and benchmark gaps:** sessions are the sector ETF's close
  dates. XLRE starts 2015-10 and XLC 2018-06: before an ETF's first admitted
  close, that sector uses XLK's session calendar and the equal-weighted mean
  return of the sector's admitted issuers as its benchmark. The rank IC is
  unchanged by this; only reported magnitudes differ.
- **Multiplicity:** all 40 trials form one run, k = 2 of `grid-granular-panel`,
  Holm at α₂ = 0.10/6 ≈ 0.016667 over the 40 trials, untestable ones at p = 1.
  Sectors failing their power gate still run and stay in the denominator; they
  cannot count as evidence against an effect.
- **When:** only after VS1's holdout verdict file exists, in one run.
- **Generalization gate (plan §2.5):** a cross-sector claim needs the primary
  trial's one-sided holdout p < 0.10 with a positive sign in at least 4 of the 11
  sectors (VS1 included), with the count's null calibrated by a sector-block
  permutation and the stricter of that and the independence bound (0.0185) used,
  plus the plan's leave-one-sector-out and single-issuer (< 25% of the IC sum)
  checks and forward confirmation in at least 2 sectors. Until then, results are
  per sector only.

## 14. Deviations from the plan's §5 (declared)

1. Selection is Holm at the ledger-issued α₁ = 0.05, not BH at q = 0.10 (BH is
   reported). Reason: S11 alpha spending keeps the error rate bounded across the
   11-sector program; BH across runs is not valid.
2. `D_peer` is not a separate trial (rank-invariant); the two features are W = 90
   and W = 30.
3. Entry is the first session close after known_at, not the next open
   (close-only prices).
4. Momentum is reported as a baseline, outside the trial ledger.
5. 10b5-1 purchases are not excluded (no flag in the derived file).
6. The entity-split holdout is not used (§8).
7. Primary-sector ties are excluded; GD0 left the tie-break as an owner
   decision. If the owner fixes a different rule before the first price read,
   that is v2.
8. A Stage-0 power gate is added (§10).
9. Reporting rules from the trade_edge tracker audit are added: missing-label
   counts and the survivorship warning (§2.3), the $500,000 largest-line stratum
   (§9) and the forward-tracker rules (§12).

## 15. Amendments

Any change to this body is a new version with a new hash, registered before any
price read of the changed scope. A change after a price read of VS1 is not an
amendment: it is a new study whose windows the ledger already marks as used.

<!-- PREREG-BODY-END -->

## Trailer (not part of the hashed body)

- Body sha256: recorded in `analysis/panel_insider_density.py`
  (`PREREG_BODY_SHA256`) and in the local registry entry; recompute with
  `python -m scripts.run_vs1_insider_density hash-prereg`.
