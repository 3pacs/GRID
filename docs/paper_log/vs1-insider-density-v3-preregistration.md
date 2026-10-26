# VS1 pre-registration v3: SIC-expanded Technology insider open-market-buy density vs XLK, 5-session primary

The body between the two markers below is the pre-registration. Its sha256 (the
UTF-8 bytes strictly between the markers, line endings normalised to LF) is
pinned in `analysis/panel_insider_density_v3.py` as `PREREG_BODY_SHA256` and is
recorded in the trailer after the end marker. It supersedes VS1 v2
(`vs1-insider-density-v2-preregistration.md`, body sha256 `159f69da...`) and v1
(body sha256 `85078eee...`), both registered and never opened. Any change to the
body is a new version (v4) with a new hash.

<!-- PREREG-BODY-START -->

## 0. Status and what was seen before writing this

- Written 2026-09-27, before any VS1 price, return or outcome was read.
  - No `raw_series` or `resolved_series` row has been read for VS1 under v1, v2
    or v3.
  - No discovery, holdout or `open-*` stage of any version has run.
- **History:**
  - **v1** (body sha256
    `85078eeeb08fe292f4a01a295261c6594d865423cdfd505621949ba43dea7c5a`,
    registry head `5b10ff57c48c68164fbef9100c174f62d26be3c87e45928cd122549c9bcf7508`)
    used 84 hand-curated sector-map Technology tickers. It failed its Stage-0
    power gate: primary `A90|fwd20` power 0.02 at IC 0.01 against the 0.50 gate.
    The owner chose option (a), an SIC-expanded universe.
  - **v2** (body sha256
    `159f69da278cd23a3f55e8249e795c59a23235b9d5b1aacacce118de067e2d2b`,
    registry head `05b20c31273a926d1be56d2afe4fce4e3cf7a8ca2769f394cad9e2fb773ceff8`)
    widened the universe to 782 candidates, with a median of 218 admitted issuers
    per proxy session in 2012–2019. It also failed the gate, with primary
    `A90|fwd20` power 0.16 at IC 0.01.
  - v2's Stage-0 table at IC 0.01, from the Form 4 feature panel with synthetic
    outcomes only:

    | Trial | Usable dates | Power |
    |---|---|---|
    | `A90\|fwd20` | 101 | 0.160 |
    | `A30\|fwd20` | 100 | 0.120 |
    | `A90\|fwd5` | 402 | 0.665 |
    | `A30\|fwd5` | 400 | 0.675 |

    At IC 0.02 the powers were 0.635, 0.685, 1.000 and 1.000. The binding
    constraint is the number of horizon-spaced dates at 20 sessions.
- **Owner decision (2026-09-27, 23:40Z):** v2 §10 option (iii). The v3
  pre-registration makes the 5-session horizon the primary test.
- **Why the horizon changed.** Power, and nothing else. The decision rests only on
  v2's Stage-0 power table.
  - That table uses no price, return or outcome.
  - v1 §10 and v2 §10 both allow a decision on power for exactly this reason.
  - No VS1 discovery or holdout statistic exists under any version.
- Everything in v2 except the primary trial is carried over unchanged:
  - the universe, SIC map, pinned inputs and admission rules;
  - the features, the four trials and their Holm family;
  - the statistic, null, holdout rule and reported items;
  - the alarms and the other-sector plan.
  Sections marked "as v2" repeat v2's text.
- Harness: `analysis/panel_insider_density_v3.py` and
  `scripts/run_vs1_v3_insider_density.py`, on the v2 harness code.
  - VS1 v1's and v2's texts and registrations are unchanged.
  - Their code now refuses to open any discovery (§7).
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

The same four trials as v1 and v2, in one Holm family. Only the primary changes:

| Trial | Feature | Horizon | Role |
|---|---|---|---|
| `A90\|fwd5` | A90 | 5 sessions | **primary** |
| `A90\|fwd20` | A90 | 20 sessions | secondary |
| `A30\|fwd5` | A30 | 5 sessions | secondary |
| `A30\|fwd20` | A30 | 20 sessions | secondary |

**Choice of primary: `A90|fwd5`.** It is chosen on the power table alone.

- Its IC-0.01 power (0.665) and `A30|fwd5`'s (0.675) are indistinguishable. The
  difference, 0.010, is about 0.3 Monte-Carlo standard errors
  (√(0.67 × 0.33 / 200) ≈ 0.033).
- `A90|fwd5` has slightly more usable dates (402 against 400).
- It keeps v1's and v2's primary feature (W = 90 days, τ = 45 days), so exactly
  one design element changes from v2: the horizon.

**Secondary trials:**

- The 20-session trials, and `A30|fwd5`, are secondary.
- They stay in the same Holm family at α₁ (§7), so selecting them costs the
  primary nothing beyond what v1 and v2 already declared.
- "Secondary" affects only the trials the holdout evaluates (§8), the calibration
  state (§11) and the Stage-0 gate (§10). It never changes selection.

No other feature, horizon, window, universe or rule is tested under this version.

## 6. Statistic and null

v1 §6, unchanged:

- the per-date Spearman IC (abstaining below 20 issuers or on a constant cross
  section);
- the mean over dates as the trial statistic, with a minimum of 30 dates;
- a block sign-flip null with the `autocorrelation_block` rule and at least 8
  blocks, 20,000 draws, seed 20260927, two-sided p.

The one-sided p in the pre-registered direction (+1) is computed from the same
draws. The one-sided p in the negative direction (added in v2) comes from those
same draws, for §11 only.

## 7. Selection and multiplicity

- **Ledger slot.** VS1 v3 is **run k = 1** of the panel ledger
  `grid-granular-panel`, at q = 0.10 and α₁ = 0.05.
  - It takes the slot v1 was registered for and v2 re-registered.
  - Neither v1 nor v2 ever read a price. Both are superseded and stay in their
    own registries and off-host witnesses with 2 records each.
- **Enforcement** (in code):
  - v1's and v2's harnesses pin `SUPERSEDED_BY`, so their `open_discovery` and
    `resume_discovery` refuse unconditionally.
  - Every version's `resume_discovery` also refuses when a witness file of a
    later version (`granular_panel_prereg_v{n}.anchors.jsonl` with n greater than
    its own) is on vault `main`. This catches older checkouts once they fetch the
    witness a price key needs.
  - v3's `open-discovery` and `discover` refuse unless the v1 and v2 witnesses on
    vault `main` still cover only their 2 registration records.
  - Only v3 can obtain a price key.
  - If v1 or v2 is ever found opened, v3 cannot run under α₁, and the owner
    decides.
- **Selection:** as v1 and v2. Holm's step-down at α₁ over all 4 trials, with
  untestable trials at p = 1. BH-adjusted p (q = 0.10) is reported only.
- The other 10 sectors remain run k = 2 at α₂ = 0.10/6 (§13).
- The windows are single-use, as in v1 §7. VS1's discovery and holdout windows
  are consumed by v3's runs.

## 8. Holdout

v1 §8, unchanged, with this version's primary:

- The holdout needs an explicit flag and **this** body's sha256.
- It is evaluated once, on the selected trials plus the primary trial
  `A90|fwd5`.
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

v2 added the following, and v3 keeps them. None of them selects:

- **Delisting-return bounds** (C2), for every trial and window: the mean IC and
  the buyer-minus-non-buyer return, with every missing label imputed.
  - *Pessimistic:* a missing buyer label (A > 0) takes the date's worst observed
    relative return, and a missing non-buyer label the date's best.
  - *Neutral:* 0.
  - *Optimistic:* the mirror of pessimistic.
  - Also reported: whether the observed IC keeps its sign under the pessimistic
    bound. If the primary trial's IC (`A90|fwd5`) does not, the verdict carries the note
    `DELISTING_SENSITIVE`.
- **SIC-group-neutral IC:** the mean rank IC with labels demeaned within SIC
  group (3570–3579, 3660–3679, 7370–7379, other) on each date. Because (b)
  issuers are not XLK constituents, this is the sector-neutral check.
  - A **size-neutral check is not possible** with these inputs: they carry no
    point-in-time shares outstanding or market capitalisation. The v1
    largest-line stratum stays the only size-related report.
- **The v1 rule's outcome** under the v1 §11 calibration and verdict rules,
  reported next to the v3 outcome.
- **Admission counts:** issuer-dates on the discovery proxy sessions that fail
  each of §2.2 rules 1–3, filing instants that name a non-current ticker, and
  issuers that never name a current ticker. These go in the Stage-0 report and
  the admission receipt.

The benchmark stays XLK. The rank IC does not depend on it; XLK enters only the
reported magnitudes.

## 10. Stage-0 power gate (before any price read)

- **Settings:** v1 §10's, unchanged.
  - The v3 Form 4 feature panel, which is v2's, on proxy sessions over the
    discovery window.
  - Planted mean rank IC of 0.01, 0.02 and 0.03.
  - 200 simulations and 999 sign-flip draws.
  - Success at a two-sided p of at most α₁/4 = 0.0125.
  - The model is optimistic: it ignores common factors and price-admission
    losses.
- **Gate:** power of the primary trial `A90|fwd5` at IC 0.01 ≥ 0.50.
- **What is known in advance.**
  - The v3 panel, seeds and settings are identical to v2's. The v3 Stage-0 is
    therefore expected to reproduce v2's `A90|fwd5` power of 0.665 exactly, and to
    pass.
  - It is re-run from the v3 code as a check of the pinned code path, not as new
    evidence.
- **The gate is optimistic.**
  - Power at IC 0.01 is 0.665 before any price-admission loss.
  - The published effect at 5 sessions is expected to be small (§11).
  - A null result is weak evidence against an effect near IC 0.01.
- **If the gate fails** (a code or input discrepancy), the run halts before any
  price read, and the owner decides.
- **If it passes,** the next step is still gated on the price-admission work of
  §2.3: the GD4 basis probe and the frozen price manifest, before
  `freeze-inputs`.

## 11. Calibration expectation and verdict

**Published prior:** v1 §11, unchanged.

**Expected effect at the primary horizon (5 sessions):**

- The direction is positive.
- Buyer-minus-non-buyer is 0 to +0.5%.
- The mean rank IC is 0.00 to 0.02, the lower half most likely.
- The wider universe adds smaller issuers, where the published effect is
  stronger.
- A 5-session horizon captures less of a drift that the literature reports over
  weeks to months. The primary is therefore expected to be weaker per trial than
  the 20-session trials, even though it is better powered.

**Calibration state:** v2 §11, computed with this version's primary.

- `CONTRARY`: a trial with a negative mean IC whose negative one-sided p survives
  Holm at α₁ over the 4 trials.
- `CONSISTENT`: the primary `A90|fwd5` has mean IC > 0 with one-sided p ≤ 0.10,
  or a selected trial has mean IC > 0.
- `WEAK_POSITIVE`: the primary mean IC is > 0 but neither of the above.
- `ABSENT`: otherwise.
- `primary_against_expectation`: the primary `A90|fwd5` has mean IC < 0 with a
  negative one-sided p of at most 0.05.

**Verdict:** v2 §11, unchanged.

- `MACHINERY_SUSPECT` if the calibration is `CONTRARY`, if
  `primary_against_expectation` is set, or if a holdout survivor has a negative
  IC.
- Otherwise `HOLDOUT_SURVIVOR_FORWARD_PENDING` if at least one retrospective
  survivor has a positive IC.
- Otherwise `NO_SURVIVOR`, marked `UNDERPOWERED` when the Stage-0 gate did not
  pass.
- Notes: `SURVIVORSHIP_WARNING` and `DELISTING_SENSITIVE`, both on the primary
  trial.
- The v1 rule's verdict is reported alongside. It uses v1's primary,
  `A90|fwd20`.

**False-alarm rates**, recomputed with the v3 primary. The null is the same as in
v2 §11: four correlated trials with a persistent common factor, 120 dates, 40
issuers. The run used 2,000 replications and 999 sign-flips, and covers the
discovery rules only.

| Rule | Fires under the global null |
|---|---|
| v1 `CONTRARY` | 0.122 |
| v1 `ABSENT` (→ `MACHINERY_SUSPECT` if powered) | 0.392 |
| v1 alarm total when powered | 0.513 |
| v3 `CONTRARY` (Holm, negative) | 0.063 |
| v3 `primary_against_expectation` (`A90\|fwd5`) | 0.059 |
| v3 alarm total (either) | 0.105 |

With valid p-values the union bound is 0.10. As in v2, the alarm should be read as
firing about 1 time in 10 under the null.

## 12. After the verdict: the forward tracker

v1 §12 applies unchanged. A survivor's forward tracker must also adopt the v2/v3
universe and admission rules of §2.2, with the ticker rule evaluated on filings
known at each forward decision.

## 13. The other 10 equity sectors

- **Carried over unchanged from v1 §13.**
  - The same sectors and benchmarks.
  - The v1 rules: the sector-map universe, v1 §§2–11, v1's primary trial
    `A90|fwd20`, and v1's calibration and verdict rules.
  - One run, k = 2, with Holm at α₂ = 0.10/6 over 40 trials.
  - It runs only after VS1's holdout verdict file exists.
- **Generalization gate (v1 §13):** each sector contributes its own
  pre-registered primary trial's one-sided holdout p.
  - For VS1 that is v3's `A90|fwd5`; for the other 10 sectors it is `A90|fwd20`.
  - The count therefore mixes horizons. Aligning the other sectors to a
    5-session primary would be a new registration of those sectors, and it is an
    owner decision to be made before any of them is looked at.
- The v2/v3 changes are **not** applied to the other sectors: SIC expansion,
  admission rules, reported items, alarms and the primary horizon.

## 14. Deviations (declared)

**From v2:**

1. The primary trial is `A90|fwd5` instead of `A90|fwd20` (§5). The 20-session
   trials are secondary in the same Holm family.
2. Consequences of that change: the Stage-0 gate, the holdout's always-evaluated
   trial, `CONSISTENT`, `primary_against_expectation` and the primary-trial notes
   all refer to `A90|fwd5` (§8, §10, §11).
3. False-alarm rates were recomputed for the new primary (§11).
4. The ledger slot k = 1 now belongs to v3. v1 and v2 refuse to open in code, and
   v3 refuses if either was opened (§7).
5. v3 has its own registry and witness (§15).

**From v1:** v2 §14 items 1–6, unchanged. Also unchanged: event rules, features,
horizons, trials, statistic, null, selection, timing, entry, exclusions, windows,
the Stage-0 settings and the other-10-sector plan.

## 15. Registration and amendments

- **Registration:** the header and `preregistration` records are appended to a
  hash-chained registry `granular_panel_prereg_v3.jsonl`, using the
  `research_forward_log` mechanism, separate from v1's and v2's.
  - The anchor lines are witnessed off-host at
    `05-GRID/Paper-Log/vs1/granular_panel_prereg_v3.anchors.jsonl` on `main` of
    `https://github.com/3pacs/obsidian-vault.git`.
  - That file is append-only. It is byte-exact (`-text`), and its history must
    stay a strict line-prefix chain.
  - The `preregistration` record carries:
    - the ledger runs: VS1 k = 1, α = 0.05, primary `A90|fwd5`, with the
      secondary trials listed; the other sectors k = 2, α = 0.10/6, under v1
      rules;
    - the SIC ranges and the pinned input hashes (as v2);
    - a `supersedes` list naming v1's and v2's body sha256 and registry heads.
- **Amendments:** any change to this body is a new version with a new hash,
  registered before any price read of the changed scope. A change after a VS1
  price read is not an amendment: it is a new study on windows the ledger
  already marks as used.

<!-- PREREG-BODY-END -->

## Trailer (not part of the hashed body)

- Body sha256: recorded in `analysis/panel_insider_density_v3.py`
  (`PREREG_BODY_SHA256`) and in the v3 registry's header and `preregistration`
  records; recompute with `python -m scripts.run_vs1_v3_insider_density hash-prereg`.
