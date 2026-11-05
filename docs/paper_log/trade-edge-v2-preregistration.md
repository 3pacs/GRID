# Pre-registration: trade_edge tracker v2 (large insider open-market buys, forward paper log)

Status: registered 2026-10-07 (the git commit that adds this file is the registration record).
Owner: Anik. Author: Claude (Opus 5.5).
The sha256 of this file's LF bytes is pinned in `paper_log/trade_edge/config.py`
(`PREREG_SHA256`) and written into the log's header record. Nothing below may change after the
header record is written on grid-svr. Any change is a new version (v3) with a new log file; the v2
log is kept as-is.

Every signal, report and scoreboard this job produces carries the banner
**"UNPROVEN — not investment advice, research paper log"** until the label rule in §10 says
otherwise, and even then it stays "not investment advice, research paper log". Nothing here places
orders, calls a broker or writes to the GRID database.

## 0. What was seen before writing this

- The v1 tracker (`grid-svr:~/trade_edge/`, table `trade_edge_paper`, cron disabled 2026-09-28)
  and its last log line (n = 153 closed, mean excess +7.2% vs SPY). v1 is contaminated and is not
  evidence: the 2026-09-27 audit (`GRID-TRADE-EDGE-TRACKER-AUDIT-20260927`, summarised in
  `vs1-insider-density-v1-preregistration.md` §3, §11, §12, §14; the audit file itself was not
  found on precision5520 or grid-svr when this was written) found that v1 entered at closes printed
  before the filing was public and counted one event several times. After point-in-time entry and
  one position per event the effect was a mean 30-session excess of about +3.4% net of costs, a
  median of -0.35% and a clustered t of about 1.0, driven by micro-caps; issuers of $2B and above
  showed about +1.3% with a clustered t of 0.09. That is weak and unproven.
- Read-only probes of griddb on 2026-10-07 (schemas, row counts, freshness, price coverage): the
  `SEC_INSIDER` puller writes one `raw_series` row per (ticker, insider, BUY/SELL, transaction
  date) with the accession, filing URL and filing date in `raw_payload` (filing date present since
  2026-09-25); `insider_trades.filing_date` is NULL on every row and `insider_trades` carries no
  accession, so it is not used. GRID's own price store (`raw_series` `YF:{ticker}:close`) held a
  recent close for 2 of the 10 tickers with a large insider buy since 2026-09-28; SPY is covered.
- No return, price path or outcome of any v2 position was looked at; none exists yet.
- Rules adopted from `vs1-insider-density-v1-preregistration.md` §2.1, §2.3, §9 and §12 (the
  "tracker v2" rules). Deviations are declared in §13.

## 1. Question

After a corporate insider's open-market purchase becomes public, does the issuer's stock beat SPY
over the next 30 sessions, net of a stated trading cost, when entered at the first close after
GRID could have known about it? Primary population: events whose largest qualifying purchase line
is at least $500,000 (the v1 construct). The smaller events are tracked under identical rules as a
reported comparison.

## 2. Events

### 2.1 Candidate filings

- Candidates are the accessions behind `raw_series` rows of source `SEC_INSIDER` whose `series_id`
  is `INSIDER:{ticker}:{insider}:BUY` (Form 4 transaction code P as parsed by
  `ingestion/altdata/insider_filings.py`), `pull_status = 'SUCCESS'`, with a non-empty accession.
  The tracker sees what GRID's live ingest saw; filings GRID never ingested are not in the log.
- **First ingest** of an accession = the earliest `pull_timestamp` of its rows plus 15 minutes.
  `pull_timestamp` is the inserting transaction's start time, so a row can become visible to
  other sessions after its stamp; the margin keeps known_at from preceding visibility.
- Each candidate accession is re-read once from EDGAR (the full submission text,
  `https://www.sec.gov/Archives/edgar/data/{cik}/{accession without dashes}/{accession}.txt`, free,
  with the repository's `SEC_USER_AGENT`). GRID's ingest keeps only the first line per
  (ticker, insider, BUY/SELL, transaction date) and only the first reporting owner, so the
  submission is the record of every line, every reporting owner, the submission type and the
  EDGAR acceptance time. From it the tracker takes: `<ACCEPTANCE-DATETIME>` (America/New_York),
  `CONFORMED SUBMISSION TYPE`, `FILED AS OF DATE`, issuer CIK, name and trading symbol, every
  reporting owner CIK and name, the filing-level and line-level Rule 10b5-1 flags (footnote text
  as in `insider_filings.py`), and every non-derivative transaction line with code P.
- If the submission cannot be fetched or parsed (after one retry), the accession is recorded from
  GRID's rows alone (`source = grid_db`): lines from `raw_payload`, actor = the normalised insider
  name, submission type taken as `4`, acquired/disposed taken as `A`, acceptance time unknown. The
  count of such accessions is reported every run.
- Every candidate accession is written once to the log as a `filing` record with all its code-P
  lines and, per line, the exclusion reason or none.

### 2.2 Qualifying purchase lines (VS1 §2.1)

A line qualifies if all hold:
1. transaction code `P`, non-derivative table;
2. submission type exactly `4` (4/A and every other type excluded);
3. acquired/disposed code `A`;
4. 0 <= filing date - transaction date <= 365 days; otherwise the line is `late_filing`
   (recorded, counted, never opened);
5. shares >= 100, price > 0, shares x price >= $10,000;
6. not flagged as an equity swap, and not flagged Rule 10b5-1 (filing-level `aff10b5One`,
   line-level `aff10b5One`, or a referenced footnote naming Rule 10b5-1).

### 2.3 Actor and de-duplication (VS1 §2.1)

- Actor = the smallest reporting-owner CIK on the accession (joint filers are one actor).
- One economic purchase reported in several accessions collapses on (issuer, transaction date,
  shares rounded to 1, price rounded to $0.01), keeping the earliest known_at and the smallest
  actor.

### 2.4 known_at (VS1 §12)

known_at = the later of
- the filing's public time: the EDGAR acceptance time when available, else the filing date at
  22:00 America/New_York (Reg S-T 13(a)(4)), and
- GRID's first ingest of the accession.

### 2.5 Universe

All issuers with a qualifying line (not VS1's Technology universe). The issuer is keyed by its
CIK (by trading symbol for `grid_db` accessions whose symbol no CIK-keyed accession carries). A
symbol that is empty, `NONE`, `N/A` or not a plausible exchange symbol makes the position
`unresolved_ticker` (recorded, counted, never priced).

## 3. Timing

- Sessions: NYSE regular sessions (`ingestion.market_calendar`). A session's close instant is
  16:00 America/New_York, or 13:00 on the recurring early-close days (the day after Thanksgiving,
  July 3, December 24 when they are sessions; `paper_log/gex_levels/sessions.py`).
- **Entry session** of a purchase = the first session whose close instant is strictly after its
  known_at. No entry ever uses a close printed before the filing was public or before GRID had it.
- **Position** = one per (issuer, entry session) (VS1 §12, harness `entry_positions`), carrying the
  distinct actors, the purchase count, the total value and the largest line. Never one position
  per insider-day line.
- **Stratum**: `large` if the largest qualifying line is >= $500,000 (primary), else `small`.
- **Forward admission**: a position enters the log only if its entry close instant is after the
  header record's `run_at`. Nothing that closed before the log started is ever in it.
- **Horizons**: 30 sessions (primary, as v1), 5 and 20 sessions (reported). Exit session = the
  h-th session after the entry session. Overlapping positions in one issuer are allowed (one per
  entry session).
- A session's data are treated as available 30 minutes after its close instant.

## 4. Statuses and the delisting rule

- `opened`: a close exists on the entry session.
- `no_price`: no close on the entry session by 5 sessions after it.
- `unresolved_ticker`: see §2.5.
- `late_filing`: line-level, §2.2 rule 4.
- `closed`: closes exist on the entry and exit sessions.
- `closed_delisted`: the exit-session close is still missing 5 sessions after the exit session
  (delisted, halted, renamed or otherwise unpriceable). The position is closed at the last close
  available between entry and exit (from the exit fetch, else from the tracker's own daily
  `marks`, else the entry close, i.e. 0%), with SPY over the same sessions. These positions are
  counted in the scoreboard at that return and reported separately, with a sensitivity mean that
  books them at an extra -30% (Shumway 1997's performance-delisting return). No position is ever
  left open or silently dropped.

## 5. Prices

- Source: yfinance (no key, no cost), both legs (issuer and SPY) from one fetch on identical
  session dates. GRID's price store is not used because it does not cover this universe (§0); the
  basis contamination that made VS1 refuse yfinance concerns stored historical vintages, not a
  single fresh fetch scored at exit.
- Returns: split- and dividend-adjusted closes (`auto_adjust=True`) from one fetch made when the
  exit is scored, entry and exit taken from the same series. The unadjusted entry close is also
  recorded at entry, for audit and for the delisting fallback.
- `marks`: once per completed session, the latest unadjusted close of every open position.

## 6. Market capitalisation bucket

Recorded once per position when it is first logged: GRID's `ticker_metrics_daily.market_cap_usd`
(latest row within 10 calendar days before the log time), else yfinance `fast_info["marketCap"]`
at the log time, else unknown. Buckets: `<300M`, `300M-2B`, `>=2B`, `unknown`.

## 7. Costs

Round-trip cost subtracted from every excess return, by bucket:

| bucket | round trip |
|---|---|
| `>=2B` | 10 bps |
| `300M-2B` | 30 bps |
| `<300M` | 100 bps |
| `unknown` | 100 bps |

Why: v1 and `realized_alpha` used a flat 5 bps, which is about right for liquid large caps but
not for the micro-caps that drove v1's mean, whose quoted half-spreads are commonly 0.5% or more
and whose closing-auction depth is thin. A small account entering at the close pays roughly the
half-spread twice. The benchmark leg carries no cost. The scoreboard also reports gross excess and
excess net of a flat 5 bps (v1 comparability).

## 8. Scoreboard (every run)

For each horizon, stratum, and cap bucket (plus all buckets together), over `closed` and
`closed_delisted` positions:
- n open, n closed, n closed_delisted;
- mean and median excess net of cost; mean gross excess; mean net at flat 5 bps; mean net with the
  delisting penalty;
- win rate = share of positions with net excess > 0;
- entry-date-clustered t of the mean net excess: CR1, clusters = entry sessions,
  SE^2 = G/(G-1) x sum_g (sum_{i in g} (x_i - mean))^2 / n^2, t = mean / SE; reported with G. It
  does not correct for overlap between adjacent 30-session windows, so it is optimistic.

Reporting rules (VS1 §2.3, §9): counts of `no_price`, `unresolved_ticker`, `closed_delisted`,
`late_filing` lines and `grid_db` (unenriched) accessions; a `SURVIVORSHIP_WARNING` when
(`no_price` + `unresolved_ticker` + `closed_delisted`) exceeds 5% of the primary stratum's
positions; the $500,000 largest-line stratum split; and the number of positions first logged after
their entry close (`late_logged`: mechanically valid, not actionable in time).

## 9. Daily output

Each run writes a JSON file and a short Markdown file: the banner and label, insider-feed
freshness (latest `SEC_INSIDER` pull and latest `insider_trades.created_at`), the new signals of
this run (ticker, issuer, insiders, total, largest line, filing date, acceptance time, known_at,
entry session, cap bucket, stratum), positions entered and closed this run, and the scoreboard.

## 10. Label rule

The label is computed only for the primary scoreboard (stratum `large`, all buckets, 30 sessions,
net of cost) and only at three pre-specified looks: when 100, 200 and 300 positions have closed
(ordered by exit session, then position id; the look uses exactly the first n).

- `SUPPORTED_FORWARD` at a look if clustered t >= 2.3 **and** median net excess > 0 **and** at
  least 30 distinct entry sessions. Terminal.
- `CONTRARY` at a look if clustered t <= -2.3. Terminal.
- Otherwise `UNPROVEN` until the next look; after the third look without a crossing,
  `NOT_SUPPORTED`. Terminal.

Why: 2.3 is the Pocock boundary for three equally spaced looks at two-sided 0.05 (2.29), so
reading the scoreboard daily cannot by itself manufacture a pass; the median condition guards
against the v1 pattern (positive mean from a few micro-cap outliers, negative median); 100 closed
positions is the earliest point at which a clustered t over dozens of entry dates means anything.
Cap-bucket splits, the small stratum and the 5/20-session horizons never change the label. Between
looks the scoreboard shows interim numbers marked interim. Even `SUPPORTED_FORWARD` is a research
result, not a trading recommendation.

## 11. Schedule

- Runs at 08:30 and 15:30 America/New_York, Monday to Friday (the 08:30 run scores the previous
  close and lists filings ingested overnight, whose entry close is that day; the 15:30 run catches
  intraday ingests before their entry close).
- Each run: read new candidates (read-only DB), enrich them (EDGAR), log new `filing` and
  `signal` records, write `entry` records for positions whose entry session has completed, a
  `marks` record when a new session has completed, `exit` records for due horizons, and one `run`
  record. Then the reports.

## 12. Integrity

- Append-only JSONL on grid-svr under `/data/grid/paper_log/trade_edge_v2/`, through the
  `analysis.research_forward_log.ForwardLog` chain: every record carries the sha256 of the previous
  line, every append is anchored in `trade_edge_v2.anchors.jsonl`, the first record is a header
  carrying this file's sha256 and the pinned code commit.
- DB sessions are opened read-only (`default_transaction_read_only = on`, verified with `SHOW`).
  The job writes no table. The v1 table `trade_edge_paper` and the v1 files are never touched.
- No orders, no broker calls, no paid APIs, no LLM.

## 13. Deviations from VS1 §12 (declared)

1. Universe: all issuers, not the Technology sector; benchmark SPY, not XLK.
2. Events come from GRID's live `SEC_INSIDER` ingest re-read from EDGAR, not from the quarterly
   DERA data set (not available daily). The DERA rules (§2.2, §2.3) are applied to the submission.
3. 10b5-1 lines are excluded: the submission carries the flag (VS1's file did not).
4. Prices from yfinance, not an admitted price manifest (§5).
5. This is an event study (one position per event, fixed horizons), not the VS1 density feature;
   VS1 has no survivor to inherit. Horizons are 30 sessions (primary, v1 comparability), 5 and 20.
6. Inference is a running scoreboard with three pre-specified looks and a clustered t, not one
   look and a block sign-flip test.
7. Early-close sessions use their 13:00 close instant.

## 14. What a result means

- `SUPPORTED_FORWARD`: the forward record, net of costs and in time, beat SPY at a level that
  survives three looks. Next step would be a second, independent forward test with real
  size-dependent costs before any real money, still not advice.
- `UNPROVEN` / `NOT_SUPPORTED` / `CONTRARY`: the large-insider-buy signal stays out of trading
  decisions.
