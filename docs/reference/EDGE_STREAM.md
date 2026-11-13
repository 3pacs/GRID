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
3. registers at most one new stage-1 proposal per weekday (queue in `families.QUEUE`, then `inbox/*.json`),
   each after the sanity gate (`sanity.check`, G1–G10); rejections go to `rejections.jsonl`;
4. rewrites `status.json` (machine-readable, schema `edge-stream-status/1`) and `DIGEST.md` (a view of it).

Look reports: `looks/<registration sha[:16]>.json` (0444, schema `edge-look-report/1`, every trade included).

## Rules it enforces (refused, not just checked)

| rule | where |
|---|---|
| registration sealed (hash chain + anchor) before its data is read; looks happen in a later cycle | `ledger.append_registration`, `runner.run_cycle` step order |
| one look per registration; an abandoned one keeps its alpha spent | `ledger.load_state` / `append_look` / `append_abandoned` |
| 40 stage-1 registrations per calendar year (UTC), alpha 0.00125 each, Bonferroni over trials | `policy`, `ledger._check_registration` |
| no reuse of (group, horizon class, period): prior consumption table + every registration | `ledger.conflicts` |
| forward windows start after registration; no outcome computed before the look | `runner.forward_progress` (presence only) |
| look runs only on the registered family code (`code_sha256`), else abandoned | `runner.compute_look` |
| bars after the decision minute never reach the rule | `families.decision_bars` / `assert_no_lookahead` |
| expectancy test: n ≥ 100, ≥ 50 a side, both sides > 0, session bootstrap LB at 1−alpha/k > 0, beats always-long, 2 of 3 thirds | `stats.judge` |

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

## Changing the policy

`policy.py` is the policy the code runs; the ledger holds the policy in force. When they differ the cycle appends
an `edge_policy_amendment` record before it seals anything new, and only if the code's policy passes
`policy.tightening_violations` (budget numbers may fall, sample and cost floors may rise, groups, instruments, cost
tiers and prior consumption may be added; nothing else may change). A loosening policy is refused: the cycle
reports it under `errors` and registers nothing until the code is put right.

**Deploying any change to `families`, `stats`, `runner`, `data` or `policy` abandons the registration that is open
at that moment** (its look and its window are spent): their code is hashed into every registration. Land such a
change when the open registration is the one you can best afford to lose, and say so in the PR.

Adding a family = a reviewed PR to `families.py` (its source is hashed into registrations). An inbox proposal
(`{"family": "IM1", "group": "...", "instruments": [...], "lane": "history"|"forward", "proposer": "codex"}`) may only
parameterise an existing family. Policy numbers change only by a new policy version that tightens.

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
