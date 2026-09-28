# VS1 other-10-sector joint run, pre-registration "sectors v3": SIC-expanded universes, 5-session primary

The body between the two markers below is the pre-registration. Its sha256 (the
UTF-8 bytes strictly between the markers, line endings normalised to LF) is
pinned in `analysis/panel_insider_density_sectors_v3.py` as `PREREG_BODY_SHA256`
and is recorded in the trailer after the end marker. It supersedes "sectors v2"
(`vs1-sectors-v2-preregistration.md`, body sha256 `ed7cacb9...`, never opened)
and, through it, the sector plan of VS1 v1 §13. Any change to the body is a new
version with a new hash.

<!-- PREREG-BODY-START -->

## 0. Status and what was seen before writing this

- Written 2026-09-28, before any price, return or outcome of any of the 10
  sectors was read, and before any sector feature, Form 4 event count or Stage-0
  power was computed.
- **Supersedes "sectors v2"**, which was never opened. Its body sha256 is
  `ed7cacb99cd010963dedfa842677784d0e7ffa95ec4534a2a005238a03a6815e` and its
  registry head `bcfc31b0f355dc04bfbd252b1705a5bd441701649bcd2b9bb4e136adbff23a04`.
  - That text (§10) required Stage-0 success at p ≤ α₂/40 ≈ 0.000417 with 999
    sign-flip draws.
  - The smallest attainable p with 999 draws is 1/1000 = 0.001, so its Stage-0
    power was 0 in every sector by construction (review round 2 of #701, item
    R4).
  - This version fixes that (§10), folds in the future harness's opening and
    contamination rules (§7), and corrects the survivorship figure (§2.2).
  - Everything else is sectors v2 unchanged. It includes the owner decision of
    2026-09-28, 02:05Z (all sectors on 5 sessions), which replaces the VS1 v1
    §13 plan.
- **Read before writing:**
  - VS1 v1 (body `85078eeeb08fe292f4a01a295261c6594d865423cdfd505621949ba43dea7c5a`),
    v3 (body `fa7eda1c70906720b36dd84d0bb8b65a53f7badc35cd05055e08d7a9b40c2e42`,
    registry head `c110b193660d5ce073d7badcddf360c739811fd86799874f3c786a16c2babbc9`)
    and sectors v2;
  - the pinned issuer metadata and the sector map.
- **No harness exists that can open these sectors.**
  - `analysis/panel_insider_density_sectors_v3.py` holds this registration only.
  - The joint-run harness must implement this text exactly (§7) and must be
    reviewed before any sector run.
- Nothing produced under this pre-registration is a trading signal.
  `promotion_allowed` is false in every artifact.

## 1. Question and directional hypothesis

Asked for each of the 10 sectors, with the sector's benchmark ETF in place of
XLK: does the number of distinct insiders who recently bought their company's
stock on the open market (Form 4, code P, dated by public filing) predict the
stock's forward return relative to the sector ETF?

- **H1** (pre-registered direction +1, every sector): more recent distinct
  insider buyers lead to a higher forward relative return.
- **H0** (per trial): the mean cross-sectional rank correlation is 0.

## 2. Data

### 2.1 Insider events

VS1 v1 §2.1, unchanged:

- the source file;
- the purchase definition;
- the actor;
- de-duplication;
- `known_at` at 22:00 America/New_York on the filing date;
- Section 16 activity;
- the 10b5-1 caveat.

### 2.2 Universe per sector and admission

**Pinned inputs**, the same files as VS1 v3:

- the sector map `analysis/sector_map_data.yaml`, LF sha256
  `2d262fe1a8ab4fbfe7abbde86c947c3ff35c12c49f00f4217b24e0dd3af3cdba`;
- the issuer map `company_tickers.json`, sha256
  `016ae8ffe06c0f8f8bed5aff9af1bb69ae12b197a3441851c712f88a5d7f64f1`;
- the SIC map `issuer_sic_map.jsonl`, sha256
  `4200acd05c9fbf563dd681acfedefd8ccd3a3ff46330fe742edf7159dd883f08`. The SIC
  in it is current, not point-in-time; the bias is as declared in v3 §2.2.

**Sector SIC ranges** (current SEC SIC, inclusive). They are disjoint from each
other and from the Technology ranges (3570–3579, 3660–3679, 7370–7379); the code
checks this.

| Sector (ETF) | SIC ranges |
|---|---|
| Energy (XLE) | 1220–1229, 1300–1389, 2910–2912, 2990–2999, 4610–4619, 4922, 5171–5172 |
| Materials (XLB) | 1000–1099, 1400–1499, 2410–2429, 2600–2659, 2800–2829, 2850–2899, 3210–3299, 3310–3399, 3410–3412 |
| Healthcare (XLV) | 2830–2836, 3841–3845, 3851, 5047, 5122, 6324, 8000–8099 |
| Financials (XLF) | 6000–6299, 6300–6323, 6325–6411, 6700–6769, 6771–6791, 6793–6794, 6796–6797, 6799 |
| Real Estate (XLRE) | 6500–6553, 6798 |
| Utilities (XLU) | 4900–4921, 4923–4949, 4960–4991 |
| Communication Services (XLC) | 2710–2749, 4800–4899, 7310–7319, 7810–7849 |
| Consumer Staples (XLP) | 0100–0299, 2000–2199, 2840–2844, 5140–5149, 5180–5182, 5400–5499, 5912 |
| Consumer Discretionary (XLY) | 1520–1531, 2300–2399, 2510–2599, 3140–3149, 3630–3639, 3651–3652, 3710–3716, 3750–3751, 3942–3949, 5200–5299, 5300–5330, 5332–5399, 5500–5540, 5542–5599, 5600–5699, 5700–5799, 5810–5813, 5900–5911, 5913–5999, 7000–7099, 7900–7999, 8200–8299 |
| Industrials (XLI) | 1540–1799, 3400–3409, 3413–3499, 3500–3569, 3580–3599, 3600–3629, 3640–3649, 3690–3699, 3720–3749, 3760–3769, 3812, 4000–4599, 4700–4723, 4725–4799, 4950–4959, 5000–5044, 5046, 5048–5064, 5066–5099, 7320–7369, 7380–7389, 8700–8730, 8732–8748 |

**Assignments where the mapping is ambiguous** (chosen conservatively,
documented):

- **Assigned against the SIC division, following the ETF's own sector:**
  - 4922 (natural-gas transmission, i.e. midstream) → Energy, not Utilities.
  - 6324 (hospital and medical service plans, i.e. managed care) → Healthcare,
    not Financials.
  - 6798 (REITs) → Real Estate.
  - 7310–7319 (advertising) and 7810–7849 (motion pictures) → Communication
    Services.
  - 5912 (drug stores) → Consumer Staples.
  - 1520–1531 (homebuilders) → Consumer Discretionary.
  - 4950–4959 (refuse and sanitary services) → Industrials.
- **Left out of every sector** because they split across sectors:
  - 5331 (variety stores);
  - 5541 (gasoline stations);
  - 4724 (travel agencies);
  - 5045 and 5065 (computer and electronic-parts wholesale);
  - 3820–3829 (measuring, analytical and control instruments);
  - 8731 (commercial physical and biological research);
  - 2670–2699 (converted paper);
  - 2750–2799 (printing);
  - 6770 (blank checks);
  - 6792 and 6795 (oil and mineral royalty traders);
  - 3790–3799 (miscellaneous transportation equipment);
  - 7200–7299 (personal services);
  - 9995 and every other code not listed above.

**Candidates per sector.** Every CIK belongs to at most one sector. The rules are
applied in this order:

1. A CIK in the VS1 v3 Technology universe (sector-map Technology members and
   the Technology SIC ranges) belongs to no other sector.
2. A v1 sector-map member belongs to its primary sector under v1's
   unique-max-weight rule, with ties excluded as in v1 §2.2. Its ticker is the
   member ticker. This holds even when its SIC points elsewhere.
3. Any other CIK with a current ticker in the issuer map belongs to the sector
   of its current SIC. Its price ticker is chosen by the v2 rule.
4. Share-class and price-ticker collisions keep the lowest CIK.

**Resulting candidates** (source: sector map only / both / SIC only):

| Sector | Total | Sector map only | Both | SIC only |
|---|---|---|---|---|
| Energy | 224 | 35 | 52 | 137 |
| Materials | 316 | 28 | 43 | 245 |
| Healthcare | 1,110 | 25 | 97 | 988 |
| Financials | 832 | 23 | 88 | 721 |
| Real Estate | 286 | 8 | 77 | 201 |
| Utilities | 128 | 11 | 51 | 66 |
| Communication Services | 170 | 8 | 26 | 136 |
| Consumer Staples | 246 | 50 | 58 | 138 |
| Consumer Discretionary | 585 | 34 | 102 | 449 |
| Industrials | 832 | 15 | 73 | 744 |

- 32 sector-map members of these sectors fall in the Technology universe and are
  excluded by rule 1.
- 119 sector-map members have a SIC in another sector's ranges and stay in their
  sector-map sector by rule 2.

**Admission at decision t.** VS1 v3 §2.2 rules 1–5 apply unchanged, per issuer:

1. a Section 16 filer at t;
2. Form 4 history of at least 2 Form 4 accessions in the 730 days before t;
3. the ticker rule: the issuer's own latest ticker-naming filing names a current
   ticker, which covers ticker reuse, de-SPACs and reverse mergers;
4. the `listed_from` cross-check;
5. price-admitted and trading.

**Bias (declared).** Current SIC applies to the past, and only survivors are
included, as declared in v3 §2.2. SIC divisions do not map cleanly onto the
ETFs' GICS sectors. Results are statements about these universes only.

**Survivorship figure.** Among the SIC-map CIKs whose current SIC is in a
sector's ranges, these have no current ticker and are excluded:

| Sector | In-range CIKs | No current ticker | Share |
|---|---|---|---|
| Energy | 967 | 777 | 80% |
| Materials | 1,061 | 757 | 71% |
| Healthcare | 2,640 | 1,553 | 59% |
| Financials | 2,431 | 1,616 | 66% |
| Real Estate | 918 | 636 | 69% |
| Utilities | 294 | 171 | 58% |
| Communication Services | 749 | 582 | 78% |
| Consumer Staples | 623 | 423 | 68% |
| Consumer Discretionary | 1,589 | 1,017 | 64% |
| Industrials | 2,518 | 1,641 | 65% |
| **All 10** | **13,790** | **9,173** | **67%** |

These are CIKs that filed any Section 16 form since 2006. The figures come from
the pinned SIC map and issuer map alone.

### 2.3 Prices (not read yet)

VS1 v3 §2.3 applies unchanged, per sector:

- the GD4 basis probe after the #671 quarantine;
- a single source, with yfinance and the Kaggle bulk load refused;
- zero multi-valued dates;
- split-consistent;
- no April-2026 bulk rows;
- an admitted list frozen before any read, including each sector ETF;
- reads only through `store.observations.read_window`;
- a single declared basis;
- no silent drops, with the `SURVIVORSHIP_WARNING` per sector.

## 3. Timing

VS1 v1 §3 applies unchanged per sector, except for which sessions and which
benchmark are used:

- **Sessions** are the sector ETF's admitted close dates.
- **XLRE** (first close 2015-10) **and XLC** (first close 2018-06): before the
  ETF's first admitted close, the sector uses XLK's session calendar. Its
  benchmark there is the equal-weighted mean close-to-close return of the
  sector's admitted issuers over the same sessions, as v1 §13 required.
  - The rank IC is unchanged by the benchmark; only the reported magnitudes
    differ.
- Decision at 16:00 New York.
- Entry at the decision close.
- h ∈ {5, 20} sessions, with decisions spaced h sessions apart.
- Discovery window: 2012-01-01 to 2019-12-31. Holdout window: 2020-01-01 to
  2026-06-30.

## 4. Features

VS1 v1 §4, unchanged: `A90` (W = 90 d, τ = 45 d) and `A30` (W = 30 d, τ = 15 d).
The feature abstains when the issuer is not admitted at t.

## 5. Declared trials (40)

The same 4 trials in each of the 10 sectors. The 5-session A90 trial is primary
in every sector:

| Trial | Horizon | Role |
|---|---|---|
| `A90\|fwd5` | 5 sessions | **primary** |
| `A30\|fwd5` | 5 sessions | secondary |
| `A90\|fwd20` | 20 sessions | secondary |
| `A30\|fwd20` | 20 sessions | secondary |

**Why this primary:**

- It is the owner's decision (all sectors on 5 sessions).
- It is the same primary as VS1 v3, whose choice rested on power alone.
- No sector outcome, feature or power has been computed.

No other feature, horizon, window, universe or rule is tested under this version.

## 6. Statistic and null

VS1 v1 §6, unchanged, per sector and trial:

- the per-date Spearman IC, abstaining below 20 issuers or when the cross
  section is constant;
- the trial statistic is the mean over dates, with a minimum of 30 dates;
- a block sign-flip null with the `autocorrelation_block` rule and at least 8
  blocks, 20,000 draws, seed 20260927, two-sided p;
- the one-sided p in each direction comes from the same draws.

## 7. Selection, multiplicity and opening rule

- **Ledger:** `grid-granular-panel` at q = 0.10. This joint run is **run k = 2**
  with **α₂ = 0.10/6 ≈ 0.016667**. The level is unchanged from VS1 v1 §13 and
  needed no change.
- **Selection:** Holm's step-down at α₂ over **all 40 trials** jointly, with
  untestable trials at p = 1. BH-adjusted p (q = 0.10) over the 40 is reported
  only.
- **Registry identity:** the joint-run harness uses the registry id
  **`sectors-v3`**. Its witness is the canonical path
  `05-GRID/Paper-Log/vs1/granular_panel_prereg_sectors_v3.anchors.jsonl`. The
  census entry for `sectors-v3` on main must be exactly that path.
  - It must not reuse any `vs1-v<n>` id.
  - It must not be hosted by the Technology `Harness` as a numbered version,
    which would give it a `vs1-v<n>` id and apply the Technology rule that every
    other witness covers only 2 records.
- **Opening check** (at every discovery and holdout opening and every price-key
  issue). The census must be keyed by exact canonical path, with no unknown VS1
  witness file on main. Then:
  - **v1, v2 and sectors-v2** witnesses cover exactly their 2 registration
    records.
  - **VS1 v3** may cover more than 2 records **only once v3's holdout is
    sealed.** The harness reads v3's registry and verifies its hash chain
    against v3's witness on main (`verify_chain(external_anchors=...)`). Then:
    - the chain's last record must be `holdout_result`, and it must be covered
      by the witness;
    - the witness must not extend past that record;
    - the holdout verdict file must exist.

    Before that point, v3's witness must cover exactly 2 records. Any other
    state refuses.
  - **No other VS1 registry** is witnessed, apart from this one and the
    registries named above.
- **Contamination:**
  - Each opening and each price read records the full census, keyed by exact
    path.
  - Contamination is measured as **growth against the census recorded at this
    run's own discovery opening**: any other registry whose covered records
    later exceed its value in that census, or any witness path that appears
    after it.
  - It is not measured as "≠ 2". v3 is legitimately past 2 records when this run
    opens.
  - A contaminated run carries `contaminated: true` in its verdict and
    holdout-result record.
- **Single use:** no trial identity (sector, feature, horizon, target) may be
  re-tested on any part of 2012-01-01 → 2026-07-01 under another version.

## 8. Holdout

VS1 v1 §8 applies per sector, with a joint family:

- The holdout needs an explicit flag and **this** body's sha256.
- It is evaluated once, on every trial selected in discovery plus every
  sector's primary trial `A90|fwd5`.
- Each sector's block is frozen from its discovery IC series.
- A retrospective survivor needs a Bonferroni-adjusted p (two-sided p × the
  number of selected trials across the run) of at most 0.05 and the same sign as
  in discovery.

## 9. Reported only (never select)

Everything VS1 v3 §9 reports, per sector and trial:

- IC sd, share of positive dates and median issuers;
- the time-alignment and issuer-shuffle p;
- buyer magnitudes;
- the momentum baseline;
- the $500,000 largest-line stratum;
- delisting-return bounds (C2), including sign survival under the pessimistic
  bound;
- the v1-rule outcome;
- admission counts.

The SIC-group-neutral IC of v3 becomes a **sub-industry-neutral IC**: labels are
demeaned within the issuer's SIC major group (first two digits) on each date. A
size-neutral check is not possible with these inputs.

## 10. Stage-0 power (per sector, before any price read)

- **Settings** per sector, on the sector's feature panel with proxy sessions:
  - the 4 trials;
  - planted mean rank IC of 0.01, 0.02 and 0.03;
  - 200 simulations;
  - **9,999 sign-flip draws per simulation**, seed 20260927 as in v1;
  - **success at a two-sided p ≤ α₂/40 ≈ 0.000417**, the smallest Holm
    threshold of the joint run.
- **Why 9,999 draws.** It keeps the success criterion equal to the real
  selection threshold, only raising the resolution.
  - The smallest attainable p is 1/10,000 = 0.0001, below 0.000417.
  - The alternative, a looser success level, would overstate power against the
    threshold the run must actually beat, so it was rejected.
  - The discovery statistic itself uses 20,000 draws (v1 §6). There the smallest
    attainable p is about 0.00005, so selection at α₂/40 is attainable too.
- **Recorded, not a gate** (v1 §13):
  - Sectors whose primary power at IC 0.01 is below 0.50 still run and stay in
    the Holm denominator.
  - Their null results cannot count as evidence against an effect; they carry
    `UNDERPOWERED`.
- Not computed yet. It must be computed and recorded before any sector price
  read.

## 11. Calibration and verdict

The rules are VS1 v3 §11, adapted to one joint family of 40 trials. The
adaptation is declared: v3's per-run rules would fire about 10 times as often
across 10 sectors.

- **Per sector, calibration state:**
  - `CONSISTENT`: the sector's primary has mean IC > 0 with one-sided p ≤ 0.10,
    or a selected trial of the sector has mean IC > 0.
  - `WEAK_POSITIVE`: the primary mean IC is > 0 but neither of the above.
  - `ABSENT`: otherwise.
- **Run level, `CONTRARY`:** a trial with a negative mean IC whose negative
  one-sided p survives Holm at α₂ over all 40 trials.
- **Run level, `primary_against_expectation`:** a sector's primary has a
  negative mean IC and a negative one-sided p that survives Holm at 0.05 over the
  10 sector primaries.
- **Verdict:**
  - `MACHINERY_SUSPECT` (run level) if `CONTRARY`, any
    `primary_against_expectation`, or a holdout survivor with a negative IC.
  - Otherwise, per sector: `HOLDOUT_SURVIVOR_FORWARD_PENDING` for a positive
    retrospective survivor, else `NO_SURVIVOR`, marked `UNDERPOWERED` when the
    sector's Stage-0 power at IC 0.01 is below 0.50.
  - Notes `SURVIVORSHIP_WARNING` and `DELISTING_SENSITIVE` apply per sector, on
    the primary.
  - `contaminated` applies as in §7.
- **False-alarm bound** under the global null, for the whole run: the union
  bound of α₂ + 0.05 ≈ 0.067 for the two alarms together, assuming valid
  p-values. It has not been simulated for the joint geometry. v3's simulation
  showed the block sign-flip null to be slightly anti-conservative under a
  persistent common factor, so read the bound as roughly 0.07–0.08.
- **Expected effect** (v1 §11 prior): positive, and small at 5 sessions (mean
  rank IC 0.00–0.02). It is likely larger in sectors with more small issuers.

## 12. After the verdict

VS1 v1 §12's forward-tracker rules apply to any survivor, with this text's
universe and admission rules.

**Generalization gate** (v1 §13 and plan §2.5), now on a single horizon. A
cross-sector claim needs all of the following:

- The primary trial `A90|fwd5` has a one-sided holdout p < 0.10 with a positive
  sign in at least 4 of the 11 sectors. The 11 are VS1 v3 Technology plus these
  10, all at 5 sessions.
- The count's null is calibrated by a sector-block permutation, using the
  stricter of that and the independence bound (0.0185).
- A leave-one-sector-out check passes.
- No single issuer contributes 25% or more of the IC sum.
- Forward confirmation holds in at least 2 sectors.

This resolves the mixed-horizon issue the v3 text left open.

## 13. Supersession

- **Superseded, and never run:**
  - the VS1 v1 §13 sector plan, carried into v2 §13 and v3 §13;
  - sectors v2.
- **Enforcement in code:**
  - VS1 v1 pins `SECTOR_PLAN_SUPERSEDED_BY` naming this registration, and its
    run spec refuses every non-Technology sector.
  - The sectors-v2 module pins `SUPERSEDED_BY` and its opening check refuses.
  - The v2/v3 harnesses have no sector stage.
- **Within the VS1 witness census:** this registry is the known entry
  `sectors-v3` at its canonical path.
  - Every VS1 Technology version refuses to open while this witness, or
    sectors-v2's, covers more than 2 records.
  - This run's own opening rule is §7.

## 14. Deviations (declared)

**From sectors v2:**

1. Stage-0 uses 9,999 sign-flip draws, so the success threshold α₂/40 is
   attainable (§10).
2. The opening rule, registry identity and contamination measure for the future
   harness are stated (§7).
3. The survivorship figure of §2.2 is computed for these sectors.
4. sectors v2 is superseded (§13).

**From VS1 v1 §13:**

1. Universes use the v3 method (sector map plus SIC ranges, a current ticker,
   v3 admission).
2. The primary trial is `A90|fwd5`, with the 20-session trials secondary in the
   same Holm family.
3. v3's admission rules, reported items and alarms apply. The alarms are
   adapted to the joint family (§11).
4. The generalization gate uses 5-session primaries in all 11 sectors (§12).
5. Everything else in v1 §13 is kept: the sector ETFs, the late-ETF fallback,
   one joint run at k = 2 with α₂ = 0.10/6 over 40 trials, Stage-0 recorded but
   not gating, and the run happening only after VS1's holdout verdict exists.

## 15. Registration and amendments

- **Registration:** the header and `preregistration` records go into the
  hash-chained registry `granular_panel_prereg_sectors_v3.jsonl`, separate from
  every other VS1 registry.
  - The anchor lines are witnessed off-host at the canonical path
    `05-GRID/Paper-Log/vs1/granular_panel_prereg_sectors_v3.anchors.jsonl` on
    `main` of `https://github.com/3pacs/obsidian-vault.git`. That file is
    append-only and byte-exact.
  - The `preregistration` record carries:
    - the sector SIC ranges and the pinned input hashes;
    - the run (sectors, ETFs, k = 2, α₂, the trials, primary `A90|fwd5`, one
      40-trial Holm family);
    - the Stage-0 settings (9,999 draws, threshold α₂/40);
    - what it supersedes (sectors v2, and v1's and v3's §13 plans).
- **Amendments:** any change to this body is a new version with a new hash,
  registered before any sector price read. After a sector price read, a change
  is a new study on windows the ledger already marks as used.

<!-- PREREG-BODY-END -->

## Trailer (not part of the hashed body)

- Body sha256: recorded in `analysis/panel_insider_density_sectors_v3.py`
  (`PREREG_BODY_SHA256`) and in the registry's header and `preregistration`
  records; recompute with `python -m analysis.panel_insider_density_sectors_v3 hash-prereg`.
