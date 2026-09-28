# VS1 pre-registration v2: SIC-expanded Technology insider open-market-buy density vs XLK

The body between the two markers below is the pre-registration. Its sha256 (the
UTF-8 bytes strictly between the markers, line endings normalised to LF) is
pinned in `analysis/panel_insider_density_v2.py` as `PREREG_BODY_SHA256` and is
recorded in the trailer after the end marker. It supersedes VS1 v1
(`vs1-insider-density-v1-preregistration.md`, body sha256 `85078eee...`), which
was registered but never read a price. Any change to the body is a new version
(v3) with a new hash; this version then stays in its registry as superseded.

<!-- PREREG-BODY-START -->

## 0. Status and what was seen before writing this

- Written 2026-09-27, after VS1 v1 failed its Stage-0 power gate and before any
  VS1 price, return or outcome was read. No `raw_series` or `resolved_series`
  row has been read for VS1 under v1 or v2, and no discovery, holdout or
  `open-*` stage of either version has run.
- **Owner decision (2026-09-27):** v1 §10 option (a): a v2 pre-registration with
  an SIC-expanded Technology universe.
- Read before writing:
  - the v1 pre-registration (body sha256
    `85078eeeb08fe292f4a01a295261c6594d865423cdfd505621949ba43dea7c5a`);
  - v1's Stage-0 power report (`GRID-VS1-STAGE0-POWER-20260927.md`). It counted
    Form 4 purchases of the v1 universe: 563 purchases in 2012–2019 across 49
    issuers, and primary power 0.02 at IC 0.01;
  - the v2 candidate amendments (`docs/paper_log/vs1-v2-candidate-amendments.md`,
    items C1–C3);
  - the trade_edge tracker audit and the GD0 security-master audit;
  - the SEC submissions JSON of every issuer CIK in the Form 3/4/5 SUBMISSION
    table. Only the issuer metadata was read: SIC, name, former names, current
    tickers;
  - a profile of the SUBMISSION table's `ISSUERTRADINGSYMBOL` field, pooled over
    all issuers. It covered how symbols are written: lists, exchange prefixes and
    `NONE`.
- The universe sizes in §2.2 come from those files alone. They were computed
  without any Form 4 transaction, feature or event count of a v2 issuer.
- The v2 Stage-0 power is computed only after this text is registered.
- Harness: `analysis/panel_insider_density_v2.py` and
  `scripts/run_vs1_v2_insider_density.py`.
  - It reuses the v1 harness's own functions unchanged for the event rules,
    feature, timing, labels, statistic, null, holdout rule and Stage-0
    simulation.
  - VS1 v1's module, text, registry and witness are unchanged and remain pinned.
  - Where this text and the code differ, this text governs, and the run is
    invalid until the code is fixed.
- Nothing produced under this pre-registration is a trading signal.
  `promotion_allowed` is false in every artifact.

## 1. Question and directional hypothesis

The question is unchanged from v1 §1. Within an SIC-expanded Technology
universe, does the number of distinct insiders who recently bought their
company's stock on the open market predict the stock's forward return relative
to XLK? Buying means Form 4, transaction code P, dated by when the filing became
public.

- H1 (pre-registered direction, +1): more recent distinct insider buyers →
  higher forward return relative to XLK.
- H0: the mean cross-sectional rank correlation between the feature and the
  forward relative return is 0.

## 2. Data

### 2.1 Insider events

The event rules are v1 §2.1, unchanged:

- the source file and its columns;
- the purchase definition (code P, Form 4 only and not amended, A, a filing lag
  of 0–365 days, at least 100 shares and at least $10,000, price above 0);
- the actor, the de-duplication and `known_at` (the filing date at 22:00
  America/New_York);
- Section 16 activity (every accession of the SUBMISSION table);
- the 10b5-1 caveat.

The SUBMISSION table (`derived/submissions.parquet`) also supplies each
accession's `document_type` and its `issuer_ticker` (ISSUERTRADINGSYMBOL as
filed) for the admission rules of §2.2.

### 2.2 Universe and admission

**Candidate issuers.** An issuer CIK is a candidate if (a) or (b) holds:

- **(a)** It is a v1 sector-map Technology member with a CIK: v1 §2.2 rules 1–2,
  the same sector map and tie rule. That gives 84 of the 88 pre-registered
  tickers; CFLT, CYBR, JNPR and PSTG have no live CIK.
- **(b)** It has at least one current ticker in the issuer map and a current SEC
  SIC code in **3570–3579, 3660–3679 or 7370–7379**.
  - These are the three ranges of v1 §10 option (a), followed exactly.
  - The owner's instruction also names 3672 (printed circuit boards). It lies
    inside 3660–3679 and adds no issuer.
  - Sector-map members are included whatever their SIC (e.g. TSLA 3711, AMAT
    3559). Members inside the ranges count once, with source `both`.

**Pinned input files.** The harness refuses any other file.

- Sector map: `analysis/sector_map_data.yaml`, LF sha256
  `2d262fe1a8ab4fbfe7abbde86c947c3ff35c12c49f00f4217b24e0dd3af3cdba`.
- Issuer map: SEC `company_tickers.json`, fetched 2026-09-27T21:05:28Z, sha256
  `016ae8ffe06c0f8f8bed5aff9af1bb69ae12b197a3441851c712f88a5d7f64f1`. This is
  the file v1's Stage-0 used.
- SIC map: `issuer_sic_map.jsonl`, sha256
  `4200acd05c9fbf563dd681acfedefd8ccd3a3ff46330fe742edf7159dd883f08`.
  - It was built by `scripts/fetch_sec_issuer_sic.py` from
    `https://data.sec.gov/submissions/CIK##########.json`.
  - It covers every issuer CIK of the SUBMISSION table: 20,760 CIKs, all
    HTTP 200, fetched 2026-09-27T21:38Z to 2026-09-27T22:33Z, with the User-Agent
    `GRID Intelligence ops@stepdad.finance` at no more than 8 requests/s.
  - Each body's sha256 and fetch time are in the fetch log.

**Price ticker.**

- For (a): the member ticker, as in v1.
- For (b): one of the CIK's current tickers, chosen in this order:
  letters-only first, then the shortest, then alphabetical.
- Every current ticker of the CIK counts for the ticker rule below.
- If two candidate CIKs would share one price ticker, the lower CIK keeps it and
  the other is dropped and listed.

**Resulting candidates: 782 issuers.**

- By source: sector map only 15, both 69, SIC only 698.
- By SIC group: 3570–3579 55, 3660–3679 189, 7370–7379 523, other (sector-map members outside the ranges) 15.
- Of the 2,304 SIC-map CIKs with an in-range SIC, 1,537 (67%) have no
  current ticker. They are excluded; see Bias.

**Admission at decision t.** A candidate is admitted at decision t only if all of
the following hold. Rules 1–3 are point-in-time and use filings only; rules
4–5 use the frozen price manifest and closes.

1. **Section 16 filer at t.** v1 rule 3, unchanged: an accession with known_at in
   (t − 730 d, t].
2. **Form 4 history.** At least 2 distinct Form 4 accessions (`4` or `4/A`) of
   the issuer with known_at in (t − 730 d, t].
   - Joint-owner fan-out of one accession counts once.
   - This implies rule 1. It admits only issuers that are demonstrably reporting
     insider transactions around t. That limits the use of today's SIC for
     periods when the CIK was a shell, a blank-check company or a
     holdings-only filer.
3. **Ticker rule** (candidate amendment C1, review item 5 of #697).
   - Take the issuer's latest filing instant with known_at ≤ t at which one of
     its own Section 16 accessions names a ticker. That instant must name one of
     the issuer's current tickers.
   - Symbols are compared in canonical form: upper case, alphanumerics only, so
     `BRK-B` = `BRK.B` = `BRKB`.
   - Lists (`ISCA, ISCB`, `MOGA/MOGB`), exchange prefixes (`NYSE: KRC`) and
     quoting are split. Each part, and the whole string, is a candidate symbol.
   - Candidate symbols that are null tokens (`NONE`, `N/A`), blank, all-digit
     or longer than 10 characters are discarded. An accession left with no
     candidate names no ticker and is ignored.
   - When several accessions share the latest instant, the rule passes if any of
     them names a current ticker.
   - Before the issuer's first ticker-naming filing, and while its latest filings
     name another ticker, the issuer abstains. That covers a pre-change or reused
     ticker, a de-SPAC or a reverse merger.
   - This targets the cases flagged in review item 5, ESI and GEN. Any span in
     which the issuer's own filings named a different ticker abstains, because
     prices under today's ticker can belong to another company there. The
     exact spans come from the filings, not from this text.
4. **Listing cross-check** (C1, N = 0 sessions). The price manifest may carry a
   per-ticker listing start from the admitted source's own metadata
   (`listed_from`). An issuer-date before it abstains. The manifest is frozen
   before any price read.
5. **Price-admitted and trading.** v1 rule 4, unchanged. The ticker must be on
   the admitted-price manifest (§2.3) with a close on the decision session (else
   the feature abstains) and on the label-end session (else the label is
   missing).

**Bias (declared):**

- **SIC is current, not point-in-time.** The SEC submissions JSON carries only
  the issuer's classification at fetch time (2026-09-27). A firm that moved into
  or out of these codes is placed by today's code for all of 2012–2026.
  - Rules 2–3 limit the damage from identity changes that come with a ticker
    change or a dormant period: de-SPACs, reverse mergers, shells and reused
    tickers.
  - They do not catch a business pivot under an unchanged ticker. Such a firm is
    counted as Technology before its pivot.
  - Former names (`formerNames`) are recorded in the SIC map but are not used as
    a rule: most renames are not industry changes.
- **Survivorship.** A candidate needs a ticker in the 2026-09-27 issuer map.
  Firms delisted, acquired or deregistered by then are excluded, as in v1.
  - If insider-bought firms that later failed are missing, the association is
    biased upward.
  - The expansion reduces v1's hand-curation and large-cap tilt, but not this
    survivorship. Two thirds of the in-range CIKs that ever filed a Section 16
    form since 2006 are excluded by it.
  - Delisting bounds (§9) report how large the known missing-label hole could
    be. They cannot see firms that never entered the universe.
- **SIC codes are assigned coarsely.** 7370–7379 includes IT services and data
  processing, and 3660–3679 includes electronic components that XLK does not
  hold. Results are statements about this universe only.

### 2.3 Prices (not read yet)

v1 §2.3 applies unchanged: admission by the GD4 basis probe after #671, refused
sources, reads only through `store.observations.read_window` on a read-only
session, the single declared basis, and no silent drops with the
`SURVIVORSHIP_WARNING`. Two additions:

- The admitted-price manifest may carry `listed_from` (§2.2 rule 4). It is part
  of the manifest's digest.
- Many (b) issuers are small and may have no admitted price series.
  - Tickers without an admitted price are excluded before any label is computed
    and are listed in the run manifest.
  - Stage-0 (§10) is computed before price admission, on the filings-admitted
    panel. Price admission can only shrink it.

## 3. Timing

v1 §3, unchanged: sessions are XLK's admitted closes; the decision is at 16:00
America/New_York; entry is at the decision close; the label is the issuer
close-to-close return minus XLK's; h ∈ {5, 20}; decisions are spaced h sessions
apart; the discovery window is 2012-01-01 to 2019-12-31 (label end before
2020-01-01), the holdout window 2020-01-01 to 2026-06-30, and later data is
reserved.

## 4. Features

v1 §4, unchanged: `A90` (W = 90 d, τ = 45 d) and `A30` (W = 30 d, τ = 15 d),
each actor counted once. The feature abstains (NaN) when the issuer is not
admitted at t (§2.2 rules 1–5).

## 5. Declared trials (4)

Unchanged: `A90|fwd5`, `A90|fwd20` (**primary**), `A30|fwd5`, `A30|fwd20`. No
other feature, horizon, window, universe or rule is tested under this version.

## 6. Statistic and null

v1 §6, unchanged:

- the per-date Spearman IC (abstaining below 20 issuers or on a constant cross
  section);
- the mean over dates as the trial statistic, with a minimum of 30 dates;
- a block sign-flip null with the `autocorrelation_block` rule and at least 8
  blocks, 20,000 draws, seed 20260927, two-sided p.

The one-sided p in the pre-registered direction (+1) is computed from the same
draws. v2 adds the one-sided p in the negative direction from those same draws,
for §11 only.

## 7. Selection and multiplicity

- **Ledger slot.** VS1 v2 is **run k = 1** of the panel ledger
  `grid-granular-panel`, at q = 0.10 and α₁ = 0.05.
  - It takes the slot v1 was registered for.
  - v1 never read a price and is superseded. Its registration stays in its own
    registry and in its off-host witness, with 2 records.
  - Enforcement: v2's harness refuses `open-discovery` and `discover` unless
    v1's pinned witness (`05-GRID/Paper-Log/vs1/granular_panel_prereg_v1.anchors.jsonl`
    on vault `main`) still holds only v1's registration anchor, which covers 2
    records.
  - If v1 is ever opened, v2 cannot run under α₁, and the owner decides.
- **Selection** is v1 §7, unchanged: Holm's step-down at α₁ over all 4 trials,
  with untestable trials at p = 1. BH-adjusted p (q = 0.10) is reported only.
- The other 10 sectors remain run k = 2 at α₂ = 0.10/6 (§13).
- The windows are single-use, as in v1 §7. VS1's discovery and holdout windows
  are consumed by v2's runs.

## 8. Holdout

v1 §8, unchanged:

- The holdout needs an explicit flag and **this** body's sha256.
- It is evaluated once: the selected trials plus the primary trial.
- The block is frozen from discovery.
- A retrospective survivor needs a Bonferroni-adjusted p of at most 0.05 and the
  same sign as in discovery.
- The entity-split holdout is not used.

## 9. Reported only (never select)

Everything in v1 §9 is reported, unchanged:

- IC sd, share of positive dates and median issuers;
- the time-alignment and issuer-shuffle p;
- buyer magnitudes;
- the momentum baseline;
- the $500,000 largest-line stratum.

v2 adds the following, none of which selects:

- **Delisting-return bounds** (C2), for every trial and window: the mean IC and
  the buyer-minus-non-buyer return, with every missing label imputed.
  - *Pessimistic:* a missing buyer label (A > 0) takes the date's worst observed
    relative return, and a missing non-buyer label the date's best.
  - *Neutral:* 0.
  - *Optimistic:* the mirror of pessimistic.
  - Also reported: whether the observed IC keeps its sign under the pessimistic
    bound. If the primary trial's IC does not, the verdict carries the note
    `DELISTING_SENSITIVE`.
- **SIC-group-neutral IC:** the mean rank IC with labels demeaned within SIC
  group (3570–3579, 3660–3679, 7370–7379, other) on each date. Because (b)
  issuers are not XLK constituents, this is the sector-neutral check.
  - A **size-neutral check is not possible** with these inputs: they carry no
    point-in-time shares outstanding or market capitalisation. The v1
    largest-line stratum stays the only size-related report.
- **The v1 rule's outcome** under the v1 §11 calibration and verdict rules,
  reported next to the v2 outcome.
- **Admission counts:** issuer-dates on the discovery proxy sessions that fail
  each of §2.2 rules 1–3, filing instants that name a non-current ticker, and
  issuers that never name a current ticker. These go in the Stage-0 report and
  the admission receipt.

The benchmark stays XLK. The rank IC does not depend on it; XLK enters only the
reported magnitudes.

## 10. Stage-0 power gate (before any price read)

- The settings are v1 §10's, unchanged:
  - the v2 Form 4 feature panel on proxy sessions (weekdays that are not US
    federal holidays) over the discovery window;
  - synthetic outcomes planted at mean rank IC 0.01, 0.02 and 0.03;
  - 200 simulations and 999 sign-flip draws;
  - success at a two-sided p of at most α₁/4 = 0.0125;
  - the model ignores common factors and price-admission losses, so it is
    optimistic.
- **Gate:** power of the primary trial at IC 0.01 ≥ 0.50.
- The Stage-0 report also states the admitted issuers and the 2012–2019
  purchase events per year. Both are computed from filings only.
- **If the gate fails, the run halts before any price read.** The owner then
  chooses one of:
  - (i) run v2 as a declared underpowered calibration run
    (`--accept-underpowered`, recorded in the frozen inputs). A null result then
    carries `UNDERPOWERED`;
  - (ii) do not run VS1.

  Any other change is a v3 pre-registration, registered before any VS1 price
  read. That includes making `fwd5` primary, which power alone may justify
  because power uses no outcome, or a different universe.
- **Expectation, stated before computing it:**
  - A rough bound says 50% power at IC 0.01 needs about 620 admitted issuers per
    date at 101 horizon-spaced dates. It uses a per-date rank-IC sd of about
    1/√n, a z of 2.5 and n·D ≈ 62,500.
  - I expect roughly 250–450 admitted issuers per date in 2012–2019.
  - So the primary trial's power at IC 0.01 should land between about 0.10 and
    0.45. **The gate is more likely to fail than pass.**
  - The `fwd5` trials, with 4× the dates, are expected to exceed 0.50 at IC
    0.01.

## 11. Calibration expectation and verdict

**Published prior and expected effect:** v1 §11, unchanged.

- The direction is positive.
- Buyer-minus-non-buyer is 0 to +1.0% per 20 sessions.
- The mean rank IC is 0.00 to 0.03 at 20 sessions and 0.00 to 0.02 at 5
  sessions.
- The wider universe adds smaller issuers, where the published effect is
  stronger. The upper halves of those ranges are therefore somewhat more likely
  than under v1.

**Calibration state** (candidate amendment C3). It is computed on discovery only
and written into the frozen manifest.

- `CONTRARY`: a trial with a negative mean IC whose **negative one-sided p
  survives Holm at α₁ over the 4 trials**. The negative direction gets the same
  multiplicity control as the positive one.
- `CONSISTENT`: the primary trial has mean IC > 0 with one-sided p ≤ 0.10, or a
  selected trial has mean IC > 0.
- `WEAK_POSITIVE`: the primary mean IC is > 0 but neither of the above.
- `ABSENT`: otherwise.
- `primary_against_expectation` (a flag): the primary trial's mean IC is < 0
  with a negative one-sided p of at most 0.05. This is evidence against the
  expected sign. It replaces v1's alarm "`ABSENT` while the Stage-0 gate passed",
  which fires on mere absence of evidence.

**Verdict (after the holdout):**

- `MACHINERY_SUSPECT` if any of the following holds. Its meaning is v1's:
  audit the event parse, dates, universe and prices before trusting anything
  else.
  - the calibration is `CONTRARY`;
  - `primary_against_expectation` is set;
  - a holdout survivor has a negative IC.
- `HOLDOUT_SURVIVOR_FORWARD_PENDING` if at least one retrospective survivor has
  a positive IC.
- `NO_SURVIVOR` otherwise. It is marked `UNDERPOWERED` when the Stage-0 gate did
  not pass.
- Notes: `SURVIVORSHIP_WARNING` (v1's rule) and `DELISTING_SENSITIVE` (§9).
- The v1 rule's verdict is reported alongside.

**False-alarm rates, stated.** These come from the synthetic global null of the
harness tests: four correlated trials, labels loading on a persistent common
factor whose exposure correlates with the feature, 120 dates, 40 issuers. The
run used 2,000 replications and 999 sign-flips, and covers the discovery rules
only.

| Rule | Fires under the global null |
|---|---|
| v1 `CONTRARY` | 0.129 |
| v1 `ABSENT` (→ `MACHINERY_SUSPECT` if powered) | 0.409 |
| v1 alarm total when powered | 0.537 |
| v2 `CONTRARY` (Holm, negative) | 0.066 |
| v2 `primary_against_expectation` | 0.060 |
| v2 alarm total (either) | 0.109 |

- The union bound for v2 is 0.10 with valid p-values.
- The small excess (and `CONTRARY` at 0.066 against a Holm level of 0.05)
  reflects the block sign-flip null's slight anti-conservatism under a
  persistent common factor at 120 dates. v2's alarm should therefore be read as
  firing about 1 time in 9 under the null, not 1 in 20.

## 12. After the verdict: the forward tracker

v1 §12 applies unchanged. A survivor's forward tracker must also adopt the v2
universe and admission rules of §2.2, with the ticker rule evaluated on filings
known at each forward decision.

## 13. The other 10 equity sectors

- **Carried over unchanged from v1 §13.**
  - The same sectors and benchmarks.
  - The v1 rules: the sector-map universe of v1 §2.2 with v1's tie rule and
    membership counts, and v1 §§2–11 including v1's calibration and verdict
    rules.
  - One run, k = 2, with Holm at α₂ = 0.10/6 over the 40 trials.
  - It runs only after VS1's holdout verdict file exists.
  - The generalization gate is unchanged, with "VS1" now meaning this v2 run.
- The v2 changes (SIC expansion, the §2.2 admission rules, the §9 additions and
  the §11 alarm) are **not** applied to them. Doing so would be a new
  registration of those sectors, and it is an owner decision to be made before
  any of them is looked at.

## 14. Deviations from VS1 v1 (declared)

1. **Universe** (§2.2): sector-map members plus current-SIC issuers in
   3570–3579, 3660–3679 and 7370–7379 that have a current ticker. The issuer map
   and SIC map are pinned by hash.
2. **Admission** (§2.2):
   - Form 4 history of at least 2 Form 4 accessions in 730 days (new);
   - the ticker rule (C1, new);
   - the listing cross-check (C1, new);
   - v1 rules 3–4 are kept.
3. **Reported only** (§9): delisting bounds (C2), the SIC-group-neutral IC, the
   v1 rule's outcome and admission counts.
4. **Calibration and verdict** (§11, C3):
   - `CONTRARY` uses Holm-adjusted negative one-sided p.
   - "`ABSENT` while powered" is replaced by `primary_against_expectation`.
   - False-alarm rates are stated.
5. **Ledger** (§7): v2 takes v1's run slot k = 1. The harness refuses v2 if v1
   was opened.
6. **Registry** (§15): a separate pinned registry and a separate off-host witness
   file.
7. **Unchanged:** event rules, features, horizons, trials, statistic, null,
   selection, holdout, timing, entry, exclusions, windows, the Stage-0 settings
   and gate, and the other-10-sector plan.

## 15. Registration and amendments

- **Registration:** the header and `preregistration` records are appended to a
  hash-chained registry `granular_panel_prereg_v2.jsonl`, using the
  `research_forward_log` mechanism, separate from v1's.
  - The anchor lines are witnessed off-host at
    `05-GRID/Paper-Log/vs1/granular_panel_prereg_v2.anchors.jsonl` on `main` of
    `https://github.com/3pacs/obsidian-vault.git`.
  - That file is append-only. It is byte-exact (`-text`), and its history must
    stay a strict line-prefix chain.
  - The v2 `preregistration` record carries:
    - the ledger runs (VS1 k = 1, α = 0.05; the other sectors k = 2, α = 0.10/6,
      under v1 rules);
    - the SIC ranges and the pinned input hashes;
    - a `supersedes` entry naming v1's body sha256 and registry head
      `5b10ff57c48c68164fbef9100c174f62d26be3c87e45928cd122549c9bcf7508`.
- **Amendments:** any change to this body is a new version with a new hash,
  registered before any price read of the changed scope. A change after a VS1
  price read is not an amendment: it is a new study on windows the ledger
  already marks as used.

<!-- PREREG-BODY-END -->

## Trailer (not part of the hashed body)

- Body sha256: recorded in `analysis/panel_insider_density_v2.py`
  (`PREREG_BODY_SHA256`) and in the v2 registry's header and `preregistration`
  records; recompute with `python -m scripts.run_vs1_v2_insider_density hash-prereg`.
