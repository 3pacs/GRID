# VS1 pre-registration v5: v4 with source filtering, a pull-batch splice check and a ticker-reuse price bound

The body between the two markers below is the pre-registration. Its sha256 (the
UTF-8 bytes strictly between the markers, line endings normalised to LF) is
pinned in `analysis/panel_insider_density_v5.py` as `PREREG_BODY_SHA256` and is
recorded in the trailer after the end marker. It supersedes VS1 v4
(`vs1-insider-density-v4-preregistration.md`, body sha256 `0b5e8c55...`) and,
through it, v3, v2 and v1, all registered and never opened. Any change to the
body is a new version (v6) with a new hash.

<!-- PREREG-BODY-START -->

## 0. Status and what was seen before writing this

- **VS1 v5 = v4 plus three price-admission rules** from the review of the #706
  Tiingo backfill: source filtering, a pull-batch splice check and a
  ticker-reuse price bound (§2.3).
  - VS1 v4 (body sha256
    `0b5e8c559743da83affe82549069994b0146b9185d1e968ae7961bc8b6107425`, registry
    head `425047c26e57eff55928272a88f6c3490da8c431aaaf4e4911d147986c51cac8`) was
    registered and never opened.
  - No cross-check, post-admission Stage-0 or price read ran under it.
  - Every tolerance below was fixed before any cross-check, splice or metadata
    result was seen.
  - The review findings are basis and coverage metadata: sources under a series
    id, pull-batch dates, and the absence of any entity check. No returns or
    outcomes were involved.
  - Everything else in this text is v4's, kept verbatim, including its history
    below.
- Written 2026-09-28, before any VS1 price return, label or outcome was read.
  - No discovery, holdout or `open-*` stage of any VS1 version has run.
  - No price has been aligned to any Form 4 event.
- **History:**
  - **v1:** 84 hand-curated tickers. It failed Stage-0 (power 0.02).
  - **v2:** the SIC-expanded universe. It failed Stage-0 (0.16 on `A90|fwd20`).
  - **v3:** v2 with the 5-session primary `A90|fwd5`.
    - Body sha256 `fa7eda1c70906720b36dd84d0bb8b65a53f7badc35cd05055e08d7a9b40c2e42`.
    - Registry head `c110b193660d5ce073d7badcddf360c739811fd86799874f3c786a16c2babbc9`.
    - It passed Stage-0 at 0.665 on the filings-admitted panel of about 218 issuers per session.
- **The v3 price-admission basis probe** (GD4, PR #705, report
  `GRID-VS1-V3-PRICE-PROBE-20260928.md`, `probe_report.json` sha256
  `5a70e37896f2bd234c32586588081033f515e8e7c6386bd9687e427964ea9e0b`) read
  **basis metadata and coverage only**: dates, pull vintages, adjustment factors
  and sources. It computed no return and did no event alignment.
  - Of 306 filings-admitted issuers, 78 were price-admitted, plus XLK.
  - 224 have no TIINGO series in the window.
  - 2 have multi-valued dates and 2 are split-inconsistent.
  - The admitted issuers carry 394 of 2,859 admitted purchase events, with a
    median of 56 issuers per session.
  - Every TIINGO row in the window was written by whole-history pulls between
    2026-04-07 and 05-07. A literal "no April-2026 bulk-batch rows" rule would
    therefore refuse every series.
- **Owner decisions (2026-09-28, 03:40Z)**, based on that coverage and basis
  metadata only, with no outcomes seen:
  - **(a)** A TIINGO whole-history backfill for the 224 uncovered tickers is
    approved. A separate agent runs it after 08:30Z.
  - **(b)** The rule "no April-2026 bulk-batch rows" is replaced by two things:
    exclude QUARANTINED rows and refused sources, **and** cross-check every
    ticker against a second provider, TwelveData (§2.3).
  - **(c)** A binding Stage-0 power gate on the price-admitted panel, before
    `freeze-inputs` (§10).
  - **(d)** The holdout-period basis check runs only at `open-holdout` (§8).
- Everything else is VS1 v3, unchanged: the universe, SIC map, admission rules,
  features, the four trials with primary `A90|fwd5`, the statistic, null,
  selection, reported items and alarms. Sections not rewritten here repeat v3's
  text.
- Harness: `analysis/panel_insider_density_v5.py` and
  `scripts/run_vs1_v5_insider_density.py`, on the pinned `Harness`. VS1 v1, v2,
  v3 and v4 are superseded, and their code refuses to open (§7).
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

### 2.3 Prices: admission rule (exact)

**Source.**
- The single admitted price source is **TIINGO** (`source_catalog` id 524, name
  `TIINGO`), series `YF:{ticker}:adj_close`. It holds split- and
  dividend-adjusted closes, read only through `store.observations.read_window`
  with `source="TIINGO"`, SUCCESS rows, one row per date, and the frozen
  `as_of_ts`.
- **Refused and never read:**
  - `yfinance`, under every series id and every status, QUARANTINED included;
  - `KAGGLE_BULK`;
  - every source other than TIINGO.
- The stored raw close `YF:{ticker}:close` (TIINGO) is read only to derive the
  adjustment factor, for basis check 2 and for the cross-check's adjustment-date
  exclusion.

**Basis checks** (from the GD4 probe, unchanged). They run per ticker over
2011-11-02 → 2019-12-31, the discovery read window including the 60-day warm-up:

1. zero multi-valued dates (one value per date across TIINGO pulls);
2. every step in the adjustment factor `adj_close / close` is either a split
   ratio within the probe's declared tolerance or a distribution step of at most
   25%;
3. no gaps: no benchmark session inside the ticker's span is missing a close;
4. no QUARANTINED row in the series.

**Cross-check against TwelveData.** A ticker is admitted only if this check also
passes:

- **Data.**
  - TwelveData REST `time_series`, `interval=1day`, `start_date=2011-11-02`,
    `end_date=2019-12-31`.
  - Fetched twice per ticker: `adjust=all` (split- and dividend-adjusted) and
    `adjust=none`.
  - The key is read from the environment variable `TWELVEDATA_API_KEY`.
  - Each response body is saved to a file (gzip) with its sha256 and fetch time
    in a fetch log. **Nothing is written to the database.**
  - The repository documents the plan as 800 requests per day and 8 per minute.
    About 2 × 303 = 606 requests fit in one day at no more than 8 per minute.
  - If the plan does not return `adjust=all` and `adjust=none` for the full
    window, the check cannot run as specified. The run stops and the owner
    decides.
- **Common dates and returns.**
  - Take the dates in the window with a close in both TIINGO `adj_close` and
    TwelveData `adjust=all`.
  - For each pair of consecutive common dates (s, t), compute each vendor's
    close-to-close return `r = P_t / P_s − 1`.
  - Pairs where s and t are not consecutive benchmark sessions are dropped and
    counted.
  - Returns only: no Form 4 date, event, label or event return is used or
    computed.
- **Adjustment dates are excluded and counted.** Pair (s, t) is excluded when
  either vendor's adjustment factor changes between s and t by more than 1e-4
  relative (1 bp). That is an ex-date of a split or distribution.
  - The 1e-4 is above the factor jitter that price rounding creates (quotes to 4–5
    decimals move a factor by less than 1e-4 for any price of $0.10 or more).
  - It is below any real distribution, since a payment of less than 1 bp of price
    is practically nonexistent.
  - TIINGO's factor is `adj_close / close`.
  - TwelveData's factor is `adjust=all close / adjust=none close`.
  - A pair where either vendor's factor cannot be formed is excluded the same
    way.
  - The count of excluded pairs is reported.
  - A ticker with more than 10% of its consecutive-session pairs excluded is
    **not admitted**, because its agreement cannot be verified on at least 90% of
    its days.
- **Pass rule.** Let n be the number of remaining pairs. The ticker passes iff
  **n ≥ N = 250** and the share of those pairs with
  **|r_TIINGO − r_TwelveData| ≤ Y = 10 basis points (0.0010)** is
  **≥ X = 99%**.
- **Why these values** (chosen from first principles and literature norms before
  any cross-check was run):
  - **Y = 10 bp.**
    - On a day without an adjustment, both vendors' adjusted returns equal the
      raw close-to-close return of the same consolidated-tape official close, so
      they should agree up to price rounding.
    - With prices quoted to at least 4–5 decimals, rounding moves a return by at
      most about 1 bp for any adjusted price of $0.10 or more.
    - 10 bp is about 10 times that bound and about 1/20 of a typical daily return
      sd for these stocks (about 2%).
    - A wrong instrument, a date shift or a mis-scaled factor therefore misses Y
      on most days, while a correct series misses it only on isolated bad prints.
  - **X = 99%.**
    - Vendor-comparison work, for example Ince and Porter (2006) on Datastream
      against CRSP, finds disagreements on correctly matched securities
      concentrated in a small fraction of observations, well under 1% once
      coverage issues are removed.
    - A mismatched series disagrees on the large majority of days.
    - 99% lies far from both.
  - **N = 250**, one trading year.
    - At X = 99%, n = 250 allows 2 mismatched days, so a correct series survives
      one or two isolated bad prints.
    - A series that disagrees on half its days passes with essentially zero
      probability under a binomial model.
    - With fewer pairs, a single bad print would fail a correct series unless X
      were loosened.
    - The cost is that a ticker with fewer than 250 usable pairs in the window
      is not admitted, for example a listing after late 2018. The count is
      reported.
- **Reference implementation:** `crosscheck_statistics` and `CROSSCHECK` in
  `analysis/panel_insider_density_v4.py`, and `splice_check`, `name_match` and
  `listing_bound` in `analysis/panel_insider_density_v5.py`, pinned with this
  text. Where the two
  differ, this text governs.
- **The check computes agreement statistics only:** n, the within-Y share, the
  number of excluded adjustment pairs and dropped pairs, and the date range.
  - It never aligns anything to Form 4 events.
  - It never computes an event, label or forward return.
  - Its per-ticker results are written to a report file whose sha256 goes into
    the price manifest as `crosscheck_report_sha256`.

**Source filtering (v5).**
- Every study read of prices is source-filtered to TIINGO: `store.observations.read_window(conn, series_id, source="TIINGO", ...)`.
  - That read fails closed when a series id carries rows from more than one source and no source is given.
- Rows of any other source stored under the same series id, e.g. refused `KAGGLE_BULK` or `yfinance` rows under `YF:AAOI:close`:
  - are ignored by every read and every basis check;
  - are counted per ticker and reported;
  - are **not** disqualifying.
- Only TIINGO rows count for basis check 1: more than one distinct TIINGO SUCCESS value for one date disqualifies, the GD4 semantics unchanged.

**Pull-batch splice check (v5).**
- **Definitions.**
  - The *selected row* of a series on a date is the row the frozen read returns: TIINGO, SUCCESS, the latest `pull_timestamp` no later than the frozen `as_of_ts`.
  - Its *pull batch* is the UTC calendar date of that `pull_timestamp`.
  - A *batch boundary* is a pair of consecutive benchmark sessions (s, t) inside the read window where the selected rows of `YF:{ticker}:adj_close` or `YF:{ticker}:close` come from different pull batches.
- **Rule.** At every batch boundary, let f = `adj_close / close` (TIINGO). The boundary passes iff:
  - **|f_t / f_s − 1| ≤ 1e-4**; or
  - TwelveData shows the same step at the same pair (a corroborated ex-date): with g = TwelveData `adjust=all / adjust=none`, **|(f_t / f_s) / (g_t / g_s) − 1| ≤ 1e-4**.
- A ticker with any failing boundary is **not admitted**. The number of boundaries per ticker is reported.
- **Why 1e-4:** the same tolerance, for the same reason, as the cross-check's adjustment-factor tolerance. It is above rounding jitter and below any real distribution. A re-adjusted vintage spliced to an older one shows up as a factor step of the size of the dividends paid between the two pulls, typically 0.2–1% for a dividend payer, which is 20–100 times the tolerance.
- The return cross-check also covers splices: a splice step changes the adjusted return on the boundary pair.
- **The 2020-01-01 boundary.** The discovery read window ends 2019-12-31, so a boundary at 2020-01-01 between pre-2020 backfill rows and 2020+ rows falls outside it. That boundary, and every boundary inside 2020-01-01 → 2026-06-30, falls under the holdout-period basis check at `open-holdout` (§8).

**Ticker-reuse price bound (v5).**
- Price series are keyed by symbol only, so a symbol's history can belong to an earlier company.
- **Earliest usable date.** For each admitted ticker T of issuer e, closes of T are used only on dates ≥ **L_T = max(S_T, F_{e,T})**:
  - S_T is the `startDate` in Tiingo's metadata for the current security (`/tiingo/daily/{T}`);
  - F_{e,T} is the filing date of the first Section 16 filing of issuer e whose `ISSUERTRADINGSYMBOL` names T (canonical symbol match, as in §2.2 rule 3).
- **Blanking.** Closes of T before L_T are blanked **before** any feature mask, label, momentum baseline or other statistic is computed.
  - L_T is carried in the price manifest as `listed_from[T]`.
  - Issuer-dates before L_T abstain (§2.2 rule 4).
- **Tiingo metadata.**
  - Fetched per ticker from `https://api.tiingo.com/tiingo/daily/{T}`, with the key in environment variable `TIINGO_API_KEY`.
  - Saved to files (gzip) with sha256 and fetch time. **Nothing is written to the database.**
  - A metadata report with the per-ticker `startDate`, `endDate`, `name` and `exchangeCode` is pinned in the manifest as `tiingo_meta_report_sha256`.
- **Entity check.** The ticker is **excluded** (reason `entity_mismatch`) when Tiingo's `name` does not match the issuer's SEC name (the pinned SIC map's `name`). The ticker is also excluded when the metadata is missing (reason `no_meta`).
- **Name match rule**, pinned:
  1. Upper-case both names.
  2. Replace every non-alphanumeric character with a space.
  3. Split into tokens.
  4. Drop the legal-form tokens {INC, INCORPORATED, CORP, CORPORATION, CO, COMPANY, LTD, LIMITED, PLC, LLC, LP, LLP, NV, SA, AG, SE, THE, HOLDINGS, HOLDING, GROUP, CLASS, A, B, C, DE, NEW}.
  5. Match iff the Jaccard similarity of the remaining token sets is ≥ **0.5**, or one remaining token string (joined with spaces) is a prefix of the other.
- **Why 0.5:** legal forms and share-class words are removed. The same company's two current names then share most tokens, e.g. "Advanced Micro Devices Inc" and "Advanced Micro Devices". Two different companies holding a reused symbol typically share at most one token.

**Manifest (v5).** It carries everything v4's manifest carries, and also:
- `listed_from` = {T: L_T} for **every** admitted issuer ticker;
- `tiingo_meta_report_sha256`.

**Manifest and freeze.**
- The price manifest carries:
  - `source=TIINGO` and `series_template=YF:{ticker}:adj_close`;
  - `basis="split+dividend adjusted"` and `benchmark=XLK`;
  - `admitted`: tickers passing the basis checks, the cross-check, the splice
    check and the entity check;
  - `probe_report_sha256` and `crosscheck_report_sha256`;
  - `listed_from` for every admitted issuer ticker, and `tiingo_meta_report_sha256` (v5).
- Tickers not admitted are excluded before any label is computed and are listed
  with their reason.
- `freeze-inputs` uses `--as-of-ts` equal to the snapshot instant of the
  post-backfill probe, so the harness reads exactly the vintages the probe and
  cross-check examined. The probe records a per-series `rows_sha256`.
- VS1 v1 §2.3's other rules are unchanged: a single basis and no silent drops,
  with the `SURVIVORSHIP_WARNING`.

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

- **Ledger slot.** VS1 v5 is **run k = 1** of `grid-granular-panel`, at q = 0.10
  and α₁ = 0.05.
  - It takes the slot registered for v1 and re-registered by v2 and v3.
  - None of those ever read a price. All are superseded and keep their 2
    registration records in their own registries and witnesses.
- **Enforcement** (in code):
  - v1, v2, v3 and v4 pin `SUPERSEDED_BY` to v5. Their discovery openings and holdout
    steps refuse.
  - Every version refuses a discovery key when:
    - another version's witness shows more than its 2 registration records;
    - a later version or an unknown or non-canonical VS1 witness file is on main;
    - its own witness is not its canonical path.
  - v5 refuses unless the v1, v2, v3 and v4 witnesses still cover only their 2
    registration records.
  - Every census is recorded, and an overlapping opening flags the run
    `contaminated`.
- **Selection:** as v3. Holm at α₁ over all 4 trials; BH reported only.
- The other 10 sectors are governed by the sectors-v3 registration (run k = 2).
- **Single use:** the windows are single-use as in v1 §7.

## 8. Holdout

- **As v3:**
  - explicit flag and this body's sha256;
  - evaluated once, on the selected trials plus the primary `A90|fwd5`;
  - frozen block;
  - Bonferroni-adjusted p of at most 0.05 with the same sign.
- **Holdout-period basis check.** The §2.3 rule applies to the holdout read
  window 2020-01-01 → 2026-06-30: basis checks 1–4, the TwelveData cross-check
  with the same X, Y and N, and the pull-batch splice check. The splice check
  includes the 2020-01-01 boundary. The source filtering and the price bound
  L_T apply unchanged.
  - It runs **only at `open-holdout`**, after the discovery is sealed, and never
    earlier.
  - Its report must state `window = "holdout"` and a snapshot time later than
    the chain's `discovery_frozen` record.
  - Its sha256 is recorded in `holdout_opened`.
  - A ticker failing it abstains in the holdout: no holdout label. It is listed.
  - The rest of the manifest is unchanged.
  - No holdout price row is read before `holdout_opened`.

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

## 10. Stage-0 power gates (before any price read)

- **Filings Stage-0** (v3 §10, unchanged; it already ran). The settings are 200
  simulations, 999 sign-flips, success at p ≤ α₁/4 = 0.0125, and the gate is
  primary `A90|fwd5` power at IC 0.01 ≥ 0.50. Recorded result: **0.665**.
  Because the panel and settings are v3's, v4's filings Stage-0 is identical.
- **Post-admission Stage-0 (binding, new):**
  - The same simulation and the same settings, on the filings-admitted panel
    restricted to the **price-admitted** issuers of the frozen manifest.
  - It uses proxy sessions and synthetic outcomes, feature data only. No price
    value enters it; only the list of admitted tickers does.
  - It must pass, with primary `A90|fwd5` power at IC 0.01 ≥ 0.50, **before
    `freeze-inputs`**.
  - The power file records the manifest's sha256.
- **If the post-admission gate fails: stop, and the owner decides.** The options
  are:
  - (i) a declared underpowered calibration run (`--accept-underpowered`,
    recorded in the frozen inputs, with a null marked `UNDERPOWERED`);
  - (ii) not running VS1;
  - any other change, which is a v5 registered before any VS1 price read.
- **Expectation, stated in advance:**
  - Before the backfill, the admitted panel is about 56 issuers per session with
    394 events, and the gate is expected to fail.
  - After the backfill it depends on TIINGO's coverage of the 224 and on the
    cross-check.

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

- **Not governed by this text.** The joint 10-sector run is pre-registered
  separately as "sectors v3".
  - Body sha256 `7b6eecae453cc71a0af021d96c259c65de5ab3d03f835746c7d413dcda7e4103`.
  - Registry head `a9a349823cba1dd92b23d9716222c6f3924a759fd99803d4a2a591dcbcdf7120`.
  - It is run k = 2 at α₂ = 0.10/6 over 40 trials, with a 5-session primary in
    every sector.
  - It replaces the v1 §13 plan, which v2 and v3 had carried.
  - It opens only after this run's holdout result is sealed (its §7).
- **Generalization gate:** as sectors-v3 §12. The primary trial is `A90|fwd5`
  in all 11 sectors, with VS1 represented by this v5 run.
- **Not inherited:** this text's price-admission rule (§2.3) does not apply to the other
  sectors. It is not part of the sectors-v3 text, and applying it there would be
  a new sectors registration before any sector price read.

## 14. Deviations (declared)

**From VS1 v4** (v5; these come from the review of the #706 backfill, i.e. from
basis and coverage metadata only, with no outcome seen):

1. **Source filtering (§2.3).** Reads are filtered to TIINGO. Other-source rows
   under the same series id are ignored, counted and reported, not
   disqualifying. Only multi-valued TIINGO dates disqualify.
2. **Pull-batch splice check (§2.3).** The tolerance is 1e-4, or a
   TwelveData-corroborated step. The 2020-01-01 boundary belongs to the
   holdout-period check (§8).
3. **Ticker-reuse price bound (§2.3).** Closes are used only from
   L_T = max(Tiingo `startDate`, the issuer's first filing naming T). The Tiingo
   metadata is fetched to files. A pinned name-match rule (Jaccard ≥ 0.5 or a
   prefix) excludes entity mismatches.
4. **Registry (§15).** A new pinned registry and the canonical witness
   `05-GRID/Paper-Log/vs1/granular_panel_prereg_v5.anchors.jsonl`. v1–v4
   refuse (§7).

**From VS1 v3** (v4, kept):

All of these are owner decisions of 2026-09-28, 03:40Z. They rest on price
**coverage and basis metadata** from the GD4 probe only; no outcome was seen.

1. **Price source and admission (§2.3).**
   - TIINGO `adj_close` is the only source.
   - yfinance (every id and status), Kaggle bulk and every other source are
     refused.
   - The "no April-2026 bulk-batch rows" rule is replaced by the QUARANTINED and
     refused-source exclusions plus the TwelveData return cross-check
     (X = 99%, Y = 10 bp, N = 250, adjustment dates excluded and counted, at
     most 10% excluded).
   - The GD4 basis checks are kept.
   - Why: every candidate row was written by April–May 2026 whole-history pulls,
     so the literal rule would refuse everything. A second provider tests the
     rows directly instead of their pull date.
2. **Coverage (§0).** The approved TIINGO backfill for the 224 uncovered tickers
   runs before the probe and cross-check are re-run.
3. **Post-admission Stage-0 (§10).** It is binding before `freeze-inputs`; on
   failure the run stops and the owner decides. Why: v3's gate was computed on
   the filings panel, while price admission shrank the panel to about a quarter.
4. **Holdout-period basis check (§8)**, at `open-holdout` only.
5. **Ledger and supersession (§7).** v4 takes k = 1; v1, v2 and v3 refuse.
   §13 now points to the sectors-v3 registration, which replaced v3's carried
   v1 §13 plan.
6. **Registry (§15).** A new pinned registry, and the canonical witness
   `05-GRID/Paper-Log/vs1/granular_panel_prereg_v5.anchors.jsonl`.
7. **Unchanged from v3:** the universe, SIC map, admission rules 1–5, features,
   horizons, trials and primary `A90|fwd5`, the statistic, null, selection,
   holdout rule, reported items, alarms and their false-alarm rates, timing,
   entry, exclusions and windows.

## 15. Registration and amendments

- **Registration:** the header and `preregistration` records go into
  `granular_panel_prereg_v5.jsonl`, using the `research_forward_log` mechanism.
  - The anchor lines are witnessed at the canonical path
    `05-GRID/Paper-Log/vs1/granular_panel_prereg_v5.anchors.jsonl` on `main` of
    `https://github.com/3pacs/obsidian-vault.git`. That file is append-only and
    byte-exact.
  - The `preregistration` record carries:
    - the ledger run (VS1 k = 1, α = 0.05, primary `A90|fwd5`);
    - the price-admission rule (the source, the refused sources, and the
      cross-check's X, Y and N);
    - the post-admission gate;
    - the pinned input hashes;
    - what it supersedes (v1, v2, v3 and v4 bodies and registry heads);
    - the v5 splice tolerance and name-match rule.
- **Amendments:** any change to this body is a new version with a new hash,
  registered before any VS1 price read. After a VS1 price read, a change is a
  new study on windows the ledger already marks as used.

<!-- PREREG-BODY-END -->

## Trailer (not part of the hashed body)

- Body sha256: recorded in `analysis/panel_insider_density_v5.py`
  (`PREREG_BODY_SHA256`) and in the v5 registry's header and `preregistration`
  records; recompute with `python -m scripts.run_vs1_v5_insider_density hash-prereg`.
