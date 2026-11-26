# Edge stream — Hermes registers and runs edge-search looks

Code: `alpha_research/edge_stream/`. Design and owner decisions: `docs/specs/2026-11-13-edge-stream-design.md`.
Tests: `tests/test_edge_stream.py`. It never places, reviews or cancels an order, and nothing it writes promotes
anything: a confirmed PASS is a candidate report for Anik's decision.

## What one cycle does

Hermes (`run_intelligence_tasks`, first block) spawns `python -m alpha_research.edge_stream cycle` once per UTC day
at or after 11:00 UTC, detached (`start_new_session`, `timeout 5400`, log `$EDGE_STREAM_DATA_DIR/cycle.log`).
Without the three `EDGE_STREAM_*` variables it does nothing. The cycle, under `cycle.lock`:

1. runs the one look of every open history registration (Polygon minute bars, cached in `bars.sqlite3`); forward
   registrations only count sessions whose bars exist until `n_target_sessions` (100) or `max_sessions` (250);
2. a stage-1 PASS registers its forward confirmation (stage 2, alpha 0.05/k);
3. registers at most one new stage-1 proposal per weekday (queue in `template_queue.QUEUE`, then `inbox/*.json`),
   each after the sanity gate (`sanity.check`, G1–G11); rejections go to `rejections.jsonl` and a refused
   queue item is checked again the next weekday;
4. rewrites `status.json` (machine-readable, schema `edge-stream-status/1`) and `DIGEST.md` (a view of it).

Look reports: `looks/<registration sha[:16]>.json` (0444, schema `edge-look-report/1`, every trade included).

## Rules it enforces (refused, not just checked)

| rule | where |
|---|---|
| registration sealed (hash chain + anchor) before its data is read; looks happen in a later cycle | `ledger.append_registration`, `runner.run_cycle` step order |
| one look per registration; an abandoned one keeps its alpha spent | `ledger.load_state` / `append_look` / `append_abandoned` |
| two-stage evidence bar (policy 3): at most 40 stage-1 registrations per calendar year (UTC), each a discovery at alpha 0.025, Bonferroni over trials; one confirmation look at 0.05/k; `40 x 0.025 x 0.05 <= q_year` | `policy`, `ledger._check_registration` |
| a registration is checked against the numbers of the policy it was sealed under (a 0.00125 registration from policy 1 or 2 stays valid) | `ledger._check_registration` (`state.policy`) |
| a discovery is refitted without its largest calendar year; an effect that disappears is `EPISODE` and is not confirmed | `stats.largest_year_out`, `stats.judge(discovery=True)` |
| a confirmation look that does not PASS closes the family in that group, on every lane | `ledger.StreamState.closed_families`, `sanity` G8 |
| noise is measured per day or ISO week; a finer unit in a trade's `session` is refused | `stats.judge` |
| no reuse of (group, horizon class, period): prior consumption table + every registration | `ledger.conflicts` |
| forward windows start after registration; no outcome computed before the look | `runner.forward_progress` (presence only) |
| look runs only on the registered family code (`code_sha256`): the live package when it reproduces the hash, else the sealed tree that does (below), else abandoned; the check runs first in every cycle, on every lane, before any data is read (a forward registration is not left to collect to its look) | `runner._evaluator`, `sealed.evaluator` |
| the hash is per family: its own class, the shared code of `families.py` and the other evaluation modules (`stats`, `runner`, `data`, `policy`); other families' classes, the `FAMILIES` registry and the queue (`template_queue.py`) are left out, so adding or changing one family abandons no other family's registration | `families.code_sha256` |
| unread status is a positive receipt in the ledger: before the first read of each kind of an open registration's window (`progress`, `look`) the runner appends an `edge_read` record to the hash-chained, anchored ledger, so a lost or restored data dir cannot hide one; a dry run reads no open window. Under policy 4 only a registration (either lane) sealed under policy 4+ with no `edge_read` record at all, abandoned at the code check with no failed attempt or staged report, frees its window and may register again. A forward presence count is a read: that window stays consumed and that (family, group, lane) closed. Registrations sealed before policy 4 free and reopen nothing; contrary or partial evidence keeps everything closed. Alpha stays spent | `ledger.append_read` / `StreamState.unread_abandoned` / `reregistrable`, `runner.read_evidence`, `policy.UNREAD_ABANDONMENT` |
| bars after the decision minute never reach the rule | `families.decision_bars` / `assert_no_lookahead` |
| a stage-1 trial of a session family must be able to pay its costs: pooled break-even hit rate (1 + round-trip cost floor / mean \|move\|) / 2 ≤ 0.60, measured on the ~13–16 sessions **before** the window (unsigned sizes only); event families (EC1) are not priced by it | `sanity` G11 (bar source set by the CLI; `sanity` is not hashed, so the gate never abandons an open registration) |
| expectancy test: n ≥ 100, ≥ 50 a side, both sides > 0, session bootstrap LB and clustered-t LB at 1−alpha/k > 0, beats always-long, 2 of 3 thirds | `stats.judge` |

## Families

| id | what it trades | data | lane it starts on |
|---|---|---|---|
| IM1 | last half hour in the direction of the prior close → 10:00 move | Polygon minute bars | history per group, then forward on SPY/QQQ/IWM |
| EC1 | an earnings reaction the market's own move hid: long when the stock beat SPY by ≥ 2 % on a day SPY fell ≥ 0.75 %, short on the mirror; in at the next open, out at the close 15 sessions later, SPY-hedged at equal notional | SEC EDGAR 8-K Item 2.02 (acceptance time) + Polygon daily bars | history 2026-01-02 → registration day, 490 S&P 500 names (`universe.py`) |

EC1 notes: trades are resampled by ISO week of entry (holds overlap); costs are four per-side floors plus a 2 %/yr
carry on the short leg; the years before 2026 are reserved for the D0 research contract's own earnings test
(`policy.PRIOR_CONSUMPTION`), so the gate refuses them; a forward EC1 look waits for 250 sessions. It needs
`SEC_USER_AGENT` in GRID's settings: without it the cycle lists EC1 under `deferred` and seals nothing. Preview
without sealing or forming a trade:
`python -m alpha_research.edge_stream cycle --dry-run --family EC1 --data-dir <scratch>` (prints sessions,
filings found, companies left out, and the hour of day filings were accepted).

## Unread abandonments (policy 4, owner decision 2026-11-21)

The policy 3 merge (#1238) changed IM1's code while `IM1-v1:us_single_stocks:history:2024-12-19` waited for its
look: the 2026-11-20 cycle abandoned it unread, and under policy 3 its window stayed consumed and its group counted
as done, so single stocks could never be tested. Policy 4 adds `unread_abandonment` and the owner decision behind it (Codex review 6010600452 asked that unread status be evidenced across prior cycles, not inferred from when the code check runs: review 6010864065 that the receipt be positive and independent of the data dir: hence `edge_read` ledger records. Single stocks is not freed: it was sealed before read records existed, and its evidence (cycle log, bar cache) cannot be reconciled from the source; bringing it back needs its own reviewed receipt); the ledger's consumption and the runner's queue read it from the policy in force, so it applies only to records
after the amendment. Before merging a change to an evaluation module, check `status.json` for an open history
registration due a look and merge after it.

## The evidence bar (policy 3, owner decision GRID#1213)

Recorded by the owner on 2026-11-16
([comment](https://github.com/3pacs/GRID/issues/1213#issuecomment-6932309442)); the policy carries the decision's
id, link and the sha256 of its text in `owner_decisions`.

| stage | what | level |
|---|---|---|
| 1, discovery | the pass test on the registered window, then the refit without the largest calendar year | 0.025 a look, Bonferroni over its trials |
| 2, confirmation | one look on data the search never read (the stream's forward lane), registered before the data exists | 0.05 one-sided, Bonferroni over its trials |

- **Why 0.025.** The yearly budget `q_year` 0.05 is unchanged and now covers both stages: if all 40 registrations
  of a year were false, the expected number of false *confirmed* candidates is 40 x 0.025 x 0.05 = 0.05. The
  retired 0.00125 bar put the whole budget on stage 1 and needed an annualised Sharpe near 2.6 on two years.
- **Best of the shuffled searches.** A look's trials are fixed in its registration before any data is read, so
  there is no search inside a look for a shuffle to price, and Bonferroni over the trials bounds that bar from
  above. A family that searches cells before choosing one (as `analysis.factory_stats` max-T does) has to bring
  its best-of-shuffled bar from its own discovery.
- **Largest year left out.** The calendar year with the largest summed net is dropped; if the mean net of the
  rest is not above zero the verdict is `EPISODE` (no confirmation is registered). With fewer than 50 trades
  outside that year, which includes every one-year window such as EC1's 2026 history, the refit is recorded as
  `not_possible` and the verdict stands; the forward confirmation is then the second period.
- **A miss closes the family.** A confirmation look that is not `PASS` closes that family in that group for
  good. An abandoned confirmation has no look: it closes nothing, its window stays consumed and the discovery
  stays unconfirmed (a discovery is confirmed at most once).
- **Before anything is acted on.** A confirmed `PASS` is a paper result until its cost has been measured against
  real fills (GRID#1214); the stream still never trades.
- **Not built.** Confirmation on unread *history* years, and confirmation-only entries for discoveries made
  outside the stream. Stage 2 is forward only.

To check the decision's hash: `gh api repos/3pacs/GRID/issues/comments/6932309442 --jq .body`, LF line ends,
exactly one trailing newline, sha256 = `a4284cd5…68228f4` (`policy.OWNER_DECISIONS`).

## Changing the policy

`policy.py` is the policy the code runs; the ledger holds the policy in force. When they differ the cycle appends
an `edge_policy_amendment` record before it seals anything new, and only if the code's policy passes
`policy.amendment_violations`: the tightening rule (budget numbers may fall, sample and cost floors may rise,
groups, instruments, cost tiers, pass-test conditions and prior consumption may be added; nothing else may
change), except for the fields a new entry in `policy.OWNER_DECISIONS` waives. A decision can waive only
`budget.alpha_per_look`, `budget.rule`, `promotion` and `evidence_bar`, must carry its link and text hash, is never
edited or removed afterwards, and never lifts the yearly budget. Anything else that loosens is refused: the cycle
reports it under `errors` and registers nothing until the code is put right.

**Deploying any change to `families`, `stats`, `runner`, `data` or `policy` abandons the registrations open at that
moment** (their looks and their windows are spent): their code is hashed into every registration. The policy
payload, numpy's major.minor and Python's `ast.dump` (so the interpreter's minor version, 3.14 on grid-hermes)
are in the hash too. **Unless the same PR adds a sealed tree:**

- **What it is.** `alpha_research/edge_stream/sealed/<name>/` is a byte copy of the edge-stream package as
  deployed (every module but `__main__` and `hermes`, never edited). Its provenance is in `sealed.TREES`: the
  release's git tree id, the file sha256s, and the open registrations it seals. `tests/test_edge_stream_sealed.py`
  pins the bytes and, on Python 3.14, that the tree reproduces each sealed hash.
- **How it is used.** On every cycle a registration whose hash the live code no longer reproduces is matched to a
  tree, failing closed at each step (`sealed.evaluator`):
  1. every file is byte-equal to the pinned manifest, with no file missing, unreadable or extra. This covers
     `ledger`, `sanity`, `universe` and `__init__`, which the hash leaves out but the sealed runner imports;
  2. only then is the tree imported (an import error excludes it);
  3. each imported module must come from the tree's own directory;
  4. the tree's own `code_sha256`, recomputed from its files, must equal the registration's. No hash-to-tree map
     is trusted.

  Its code check, presence count, look and report then run in that tree (the runner's ledger records, read
  receipts and report file stay the live ones). A tree failing any step judges nothing, and the registration is
  abandoned as before.
- **Trust boundary.** The bytes are checked on disk just before import, and re-checked on every call. The code in
  memory is what was imported, so this assumes the deployed release root is immutable while a cycle runs: written
  only by the deploy, owned by `grid`, never edited in place. Stale bytecode is trusted as Python trusts it.
- **What a tree does not freeze.** A sealed look reads bars through the *live* sources and `extras`. Its failed
  attempts are counted by the *live* `runner._attempt_failed` under live policy thresholds.
  `tests/test_edge_stream_sealed.py` holds them to the tree:
  - live `data.py` is AST-equal to the tree's;
  - `_attempt_failed` and the three retry thresholds are equal;
  - a cap is never counted;
  - source errors abandon on the tree's day;
  - readiness and date coverage (before the window, a gap, a holiday) match.

  While a sealed registration is open, a change to any of these fails CI. It then needs a frozen adapter, or the
  registration's loss accepted in the PR.
- **When a tree goes.** Remove one only when no open registration in the live ledger reproduces under it.
- **The first tree.** `tree_a1d9bd4c` (#1337, release tree `a1d9bd4c`, deployed from `ca04a2d5` on 2026-11-18)
  keeps the two forward registrations open on 2026-11-25: the SPY/QQQ/IWM test `69a84820` and EC1's confirmation
  `60b49abd`, both sealed under policy 3.

Without a tree, land such a change when the open registrations are the ones you can best afford to lose, and say
so in the PR.

Adding a family = a reviewed PR to `families.py` (its source is hashed into registrations). An inbox proposal
(`{"family": "IM1", "group": "...", "instruments": [...], "lane": "history"|"forward", "proposer": "codex"}`) may only
parameterise an existing family. Policy numbers change only by a new policy version that tightens, or by a
recorded owner decision.

## Install on grid-svr (Anik; after the PR is merged and deployed)

`install-edge-stream-grid-svr.sh` (handed over in the working folder) does, idempotently:

```bash
install -d -m 0750 /data/grid_ledgers/edge-looks/ledger /data/grid_ledgers/edge-looks/anchor /data/grid_ledgers/edge-looks/data
```

```bash
cd /data/grid_v4/grid_release && umask 0027 && PYTHONPATH=. /data/grid_v4/grid_venv-314/bin/python -m alpha_research.edge_stream genesis --ledger /data/grid_ledgers/edge-looks/ledger/grid-edge-looks.jsonl --anchor /data/grid_ledgers/edge-looks/anchor/grid-edge-looks.anchors.jsonl
```

then the drop-in `/etc/systemd/system/grid-hermes.service.d/zz-edge-stream.conf` with
`EDGE_STREAM_LEDGER_PATH`, `EDGE_STREAM_ANCHOR_PATH`, `EDGE_STREAM_DATA_DIR`, `daemon-reload` and a grid-hermes
restart. Genesis refuses an existing ledger or anchor, so a second run cannot reset the yearly count.
Keep an off-host copy of the anchor; optional `chattr +a` on both `.jsonl` files.

## Read it

```bash
cd /data/grid_v4/grid_release && PYTHONPATH=. /data/grid_v4/grid_venv-314/bin/python -m alpha_research.edge_stream verify --ledger /data/grid_ledgers/edge-looks/ledger/grid-edge-looks.jsonl --anchor /data/grid_ledgers/edge-looks/anchor/grid-edge-looks.anchors.jsonl
```

```bash
jq '{epoch, open, verdicts, candidates}' /data/grid_ledgers/edge-looks/data/status.json
```

A dry run (in-memory ledger, no outcome, presence-only preflight of the next window):
`python -m alpha_research.edge_stream cycle --dry-run --data-dir <scratch>`.

## If the ledger refuses to open

"ledger has N records but its anchor has N−1" means a crash (power loss: appends are not fsynced) between the
ledger line and its anchor line. Nothing is lost silently: stop (`--remove`), compare the last ledger line with the
anchor's last `ledger_head_sha256`, and decide by hand with Anik; never delete or re-genesis the ledger.
