# Edge stream: Hermes registers and runs looks continuously (design + first slice)

## Context

Anik wants Hermes (grid-hermes on grid-svr) to keep searching for "anything reasonably sane" in the style of the
hand-run edge searches (`~/dev/gex/agentic-20261105/edge2..edge9`, edge6 follow-ups, fly-shadow, bounce-shadow).
The hand-run searches are honest because of their rules: registration before data, one look on held-out data,
Bonferroni within a family, insert-only records, expectancy after costs, both sides must earn. Fourteen-odd looks so
far; one pass (edge6 fly), which only pays when held to settlement. The stream has to automate those rules so
endless looks cannot manufacture a false pass, and so it never re-uses data a family has already seen.

What is already spent (prior consumption, imported at genesis, never re-used):

| group | horizon | period | by |
|---|---|---|---|
| us_equity_index (SPY QQQ IWM DIA RSP XSP SPX ES NQ, sectors XL*) | intraday | 2018-05-01 .. 2026-11-12 | edge9 (holdout + dev), edges 2, 4-8, climax replay, panel trials |
| us_equity_index | overnight | 2022-11-14 .. 2026-11-12 | edge2 N1 |
| us_equity_index, factors | multiday | 1926-07 .. 2026-11-10 | edge3 (SPY daily 1993-2026), high-Sharpe v1-v6 (French 1926-2026) |
| rates (TLT IEF SHY), credit (HYG LQD), vol (VIXY VIX) | intraday | 2018-05-01 .. 2022-11-11, 2024-11-11 .. 2026-11-09 | edge9 dials |
| every high-Sharpe universe ticker (30 names + EFA EEM DBC XLC XLRE) | multiday | 2014-01 .. 2026-10-31 | high-Sharpe v1-v6 |
| us_equity_index options (SPY/XSP 0DTE) | intraday | 2022-11-14 .. 2026-11-10 | edges 2, 4, 6, 7 |

What is still fresh and free on grid-svr (measured 2026-11-12/13): Polygon (GRID's key, bearer header) serves
next-day 1-minute and daily bars for any ETF or stock over a rolling window that now starts 2024-11-12 (same-day =
403; 429s do occur, so a slow fetch). Untouched groups intraday in that window: metals (GLD SLV), energy (USO UNG),
international equity (EFA EEM EWJ FXI EWZ), FX (FXE FXY UUP; thin), crypto ETFs (IBIT ETHA), US single stocks
(AAPL MSFT NVDA AMZN META GOOGL TSLA). Forward data (any group, from a registration onward) is fresh by construction.
Buying data (Databento) costs money and is never done by the stream.

## Decisions (roast, Anik 2026-11-13)

1. Budget: **per calendar year, q 0.05 over at most 40 registrations** (0.00125 per look, Bonferroni within), abandoned
   count, stage-1 PASS → forward confirmation at 0.05/k before anything reaches Anik.
2. Fresh data: consumption keyed **per (group, horizon class, period)**; disclosed in every report.
3. Execution: **Schwab can short shares** → capability table schwab = long/short shares, spreads, hold to expiry;
   shorts judged at bid/ask on shares. Robinhood agentic = long options/shares only.
4. My calls: one pooled trial per group look (k = 1); history looks run automatically (that is the request) but
   nothing promotes; costs from a per-instrument floor (Polygon bars carry no bid/ask): per side max($0.02, 1.5 bp) for
   liquid names, max($0.03, 3 bp) for thin ones, no fees on shares, no borrow cost intraday.

## Reuse (no new ledger code)

- `analysis/ledger_steered_exploration.py`: `Ledger` + `Anchor` (canonical JSON lines, `prev_sha256` chain, external
  anchor that refuses truncation/restart, `xb` create, `promotion_allowed: false` enforced). Unknown record kinds
  are accepted by `verify_chain`, so the stream adds its own kinds on a **dedicated** ledger `grid-edge-looks`
  (placeholder catalog at genesis, as `grid-hypothesis-trials` does in `docs/reference/HYPOTHESIS_SCORING_V2.md`).
- Polygon access pattern: `ingestion/altdata/polygon_puller.py` (key from config/env, bearer header, 429 handling).
- Hermes: a time-gated block in `scripts/hermes_operator.py::run_cycle()` per the `hermes-operations` skill;
  env-gated like `HYPOTHESIS_TRIAL_LEDGER_PATH` (no env = writes nothing).
- Pass statistics follow edge9's look (session-resampled percentile bootstrap, seed 20261107).

## Design

Package `alpha_research/edge_stream/` (GRID repo, worktree `~/dev/grid-edge-stream`, branch
`claude/edge-stream-20261113`; the shared checkout is never switched).

**Records (insert-only, on the `grid-edge-looks` ledger):**
- `edge_policy` (seq 1, once): budget rule, groups map, cost floors, account capabilities, gate thresholds, prior
  consumption table above, prior look count (disclosed, not re-controlled). Amendments are new records and may only
  tighten (lower alpha, add consumption, add groups/instruments).
- `edge_registration`: family + code sha256 of the rule module, mechanism (text + citation), lane (history |
  forward), instruments, group, horizon class, window (history `[start, end)`; forward `from` = next session after
  registration, `n_target`, `max_sessions`), decision rule + params, direction rule + its non-clock inputs, sides,
  entry/exit spec and account, cost model per instrument, trials and k, alpha per trial, n_min / n per side, the pass
  test, the sanity report, proposer (template | inbox:<agent>). Its line sha256 is the seal. Written **before** the
  look's data is fetched; its window is consumed from that moment (abandoned included).
- `edge_look`: registration sha, data manifest sha256 (exact bars used), per-trial verdict PASS / FAIL /
  UNDERPOWERED with every number, report path + sha. The registration is SPENT.
- `edge_abandoned`: registration sha + reason (fetch failed 5 times or 14 days, nondeterminism). Alpha stays spent.
- Gate rejections go to `rejections.jsonl` (append-only, no alpha spent: nothing was read).

**Sanity gate (all machine checks; failure = rejection):**
G1 mechanism text + citation present · G2 direction is a function of at least one declared market-state input that
is not clock/calendar (template metadata + a property test per family: same clock, different state, different
direction) · G3 directional → both sides traded and both judged (`non_directional` needs a stated reason) · G4 costs ≥
the policy floor per instrument (round trip half-spread x2 + fees; options at bid/ask + per-contract fee) · G5 the
judged exit is one the named account can do (capability table: robinhood_agentic long options/shares only, SPY
expiry-day closeout 15:45, no shorts, no spreads; schwab shares + spreads + hold to expiry); the result is computed at
that exit only (edge6 lesson) · G6 power: window can supply n_min (sessions x max fires) else reject; forward lanes
carry `max_sessions` · G7 fresh: history window overlaps no consumption of (group, horizon class); forward starts after
registration · G8 budget left this epoch and alpha resolvable (bootstrap B ≤ 2,000,000) · G9 rule harness hands the
rule bars truncated at the decision minute and asserts no bar after it (lookahead guard; Polygon bars are not DB rows,
so `store/pit.py` does not apply, this guard replaces it) · G10 instrument on the policy's liquid list.

**Multiple testing (decision 1):** per-year budget: q_year 0.05 over at most 40 registrations per
calendar year → alpha 0.00125 per look, Bonferroni over its k trials; abandoned looks count; at 40 the stream stops
registering until January. Stage 2: a stage-1 PASS auto-registers a forward confirmation of the same rule (fresh
forward data, alpha 0.05/k, outside the 40); only a confirmed PASS goes to Anik as a candidate. False candidates per
year ≤ 0.05 x 0.05.

**Pass test (per trial):** n ≥ 100 and ≥ 50 long and ≥ 50 short; mean net > 0, long mean > 0, short mean > 0;
session-resampled percentile bootstrap lower bound at 1 − alpha/k > 0 (B = max(10 000, ceil(100/(alpha/k))), seed
20261107); mean net above the always-long mean on the same sessions; mean net > 0 in at least 2 of 3 equal time
thirds. UNDERPOWERED when counts fall short. Win rate descriptive only.

**Cycle (`python -m alpha_research.edge_stream cycle`, flock, idempotent):**
1. open + verify ledger/anchor; 2. open history registrations → fetch (cache sqlite in the data dir) → evaluate →
write `looks/<sha>.json` (0444) + digest line → append `edge_look`; a crash before the append recomputes the same
result from the cached bars (identical bytes required, else abandon); 3. forward registrations: only count available
sessions — **no outcome statistic is computed before the look**; look when n_target sessions or max_sessions reached;
4. register at most one new proposal per weekday from the queue (templates, then `inbox/*.json` proposals that
parameterise an implemented family; a new family is a reviewed code PR); 5. write `status.json` (machine-readable:
head, epoch budget, open registrations, consumption map, verdicts) and `DIGEST.md` (short human view).

**Hermes step:** once per UTC day after 11:00 UTC (Polygon has yesterday by then), if `EDGE_STREAM_LEDGER_PATH` is
set, spawn the cycle as a detached subprocess (`start_new_session`, own wall-clock limit, its own log) so the Hermes
cycle never waits and no orphan thread holds the ledger lock. Never places orders; never touches gex-trader config.

**Dry run:** `cycle --dry-run` uses an in-memory ledger and stops before any evaluation (prints sessions available,
never an outcome), so a demo never spends fresh data.

## First searches (queue, in order)

Family IM1, market intraday momentum (Gao, Han, Li & Zhou 2018 JFE; hedging-demand channel Baltussen, Da, Lammers &
Martens 2021 JFE): direction = sign of the prior-close → 10:00 ET return; enter at the 15:30 bar close, exit at the
15:59 close; trade every eligible session; costs per instrument floor; account schwab (long and short shares). One pooled trial per
group (k = 1, session-clustered), per-instrument numbers descriptive.
1. IM1 history metals (GLD SLV) 2024-11-13 .. 2026-11-12 · 2. energy (USO UNG) · 3. international equity (EFA EEM EWJ
FXI EWZ) · 4. crypto ETFs (IBIT ETHA) · 5. single stocks (7 mega caps) · 6. FX (FXE FXY UUP) if the liquid list keeps
them · 7. IM1 forward on us_equity_index (SPY QQQ IWM) from registration, n_target 100 sessions (~April 2027).

## Deliverables / files

- `alpha_research/edge_stream/{__init__,policy,ledger,sanity,windows,stats,data,families,runner,report,__main__}.py`
- `scripts/hermes_operator.py`: one gated block (step 7x) that spawns the cycle
- `tests/test_edge_stream.py` (ledger round trip + tamper refusal, gate rejections per rule, consumption overlap,
  budget exhaustion, lookahead guard, bootstrap pass/fail on synthetic data, crash-replay idempotency, forward no-peek,
  Hermes gating off without env)
- `docs/specs/2026-11-13-edge-stream-design.md` (this design), `docs/reference/EDGE_STREAM.md` runbook
- `install-edge-stream-grid-svr.sh` (repo root, untracked, for Anik): dirs `/data/grid_ledgers/edge-looks/{ledger,anchor}`,
  genesis + policy record, Hermes drop-in `zz-edge-stream.conf`, restart
- report + hand-over note in the shared notes folder; memory note; agent-report

PR opened, not merged (merges deploy; Anik's word). The code is inert until Anik runs the installer.

## Verification

- `python -m pytest tests/test_edge_stream.py tests/test_ledger_steered_exploration.py -q` (threads capped at 2)
- Synthetic end-to-end: in-memory ledger, synthetic bars with a planted effect → PASS; null bars → FAIL; second
  look refused; tampered ledger refused
- grid-svr `cycle --dry-run` from a staged copy (release-tree PYTHONPATH): Polygon fetch of the first queued window,
  registration printed, no evaluation
- After Anik's install: first real cycle writes registration + look for queue item 1; `status.json` verified

## Changes after the independent review (2026-11-13, before any real look)

- `code_sha256` = AST digests (docstrings dropped) of the whole evaluation path (`families`, `stats`, `runner`,
  `data`, `policy`) + numpy major.minor + policy sha: any real change to rule, trade selection, readiness, data
  filter, costs or test abandons open registrations; comments and docs do not.
- Empty days: a weekday without bars is cached as empty only if it is an NYSE holiday or was fetched ≥ 3 days after
  it; a history look requires every non-holiday weekday served or settled; requests older than 725 days are refused
  (history windows now start 700 days back so warm-up and a two-week-late look stay inside Polygon's window).
- A `|move| > 25 %` between the bars a trade uses (split / adjustment basis) skips the session.
- Pass test adds a session-clustered t lower bound (ratio estimator, S − 1 df) at 1 − alpha/k beside the bootstrap.
- Forward registrations may run side by side only if their families differ (one rule is never judged twice on the
  same sessions).
- Robustness: per-item try/except (a bad inbox file or a refused registration is a rejection row, never a dead
  cycle), `G0_wellformed` (lane, horizon, distinct ticker strings, stage), the request cap is never counted against a
  registration and failures count per distinct day (history 5, forward 10), stale read-only report tmp is removed and
  the report is fsynced, ledger + anchor appends run with SIGTERM/SIGINT/SIGHUP blocked, the CLI opens the ledger
  inside the cycle lock, `verify` re-hashes every look report.
- Known and accepted: the S11 `_append` does not fsync (a power cut can lose the last record; the anchor check then
  refuses to open until repaired by hand, fail-closed); serial dependence across adjacent sessions is not modelled.
