# Desk brief (desk-brief/1)

Two private briefs per XNYS session, built from records the desk already keeps (3pacs/GRID#1164, milestone #15).
Code: `analysis/desk_brief.py` (clock, labels, rendering, the wording guard), `analysis/desk_brief_sources.py`
(read-only adapters), `config/desk_brief_sources.json` (the sources and each registration's look count),
`scripts/desk_brief.py` (CLI), `scripts/desk_brief_run.sh` with `deploy/systemd-user/grid-desk-brief-*` (timers,
not installed).

## What it is not

It computes no metric, ranks nothing and promotes nothing. It places, stages and simulates no order. A forward
trial's line is a count toward its one look ("unproven, n of N", from `analysis/desk_evidence.py`); the brief never
shows an interim hit rate, mean or P&L, whatever the trial's registration would allow. Outcomes in R appear only
from the paper record (D4, 3pacs/GRID#14), whose summary has already applied each registration's disclosure rule:
its `stats` block is null unless the registration allows interim display or the look is SPENT. The brief copies
`stats` only when present, never derives a mean or sign from counts, and does not show gross-only (pre-cost) stats.
Until the summary file exists, the post-close brief says it does not grade any call in R.

## Clock

| Brief | as-of (registered) | Timer |
|---|---|---|
| pre_open | 30 min before the session's open (09:00 ET on a regular day) | weekdays 09:01 America/New_York |
| post_close | 22:20 America/New_York on the session's day | weekdays 22:21 America/New_York |

22:20 ET is after the shadow graders (21:10-21:40 UTC), the evening text (22:05/23:35 UTC) and the 01:50 UTC
overnight plays and forecast grades. It is 03:20 UTC in winter and 02:20 UTC in summer, outside the Hermes window
(02:00-02:15 UTC).

Rules, each tested (`tests/test_desk_brief.py`):

- Every source states its own clock. A source whose clock is later than the as-of is `after_clock` and its figures
  are left out. A source with no readable clock is `unknown`. A naive timestamp is refused, never assumed UTC.
  Exception: `market_state_daily.computed_at`, a naive TIMESTAMP, is read as UTC only when the session's
  TimeZone is UTC.
- Ledger rows are counted only when their clock is at or before the as-of. A row with an unreadable clock is not
  counted, and the trial says so.
- Live endpoints (the Records page, the GEX site state) can only be read when the brief runs. A read up to 10
  minutes after the as-of is used, and the section states how late it was. A later read is `after_clock`, so a
  brief made by hand long after its clock loses them. That is why the timers use `Persistent=false`.
- A brief is written once: `<out>/<session>-<kind>.{json,md}`, folder 0700, files 0600. A rerun writes nothing.
  A run before the as-of refuses (exit 2). On a holiday morning the next session's clock is still ahead.
- A brief given `--as-of` by hand says "clock given by hand" at its top.

## Sources

| Section | Source | Read |
|---|---|---|
| Market state | `griddb.market_state_daily` (market-state/1, #1191) | latest row computed before the as-of, read-only transaction. The writer computes a session's record the next evening, so post-close normally shows the previous session and pre-open the one before. Only an older record is a data problem. |
| Event calendar | pinned XNYS and factory-events parts (`analysis/factory_session_calendar.py`, forward parts #1293) | `FactoryEvents.known(day, as_of)` for 5 sessions. A day no part answers is printed as unknown, never quiet. Beside it: the market-state record's FRED release list (GDP, PCE, ...), which has no known-at. |
| Session in prices | desk-state `desk-evening/1` files | the evening text for the session (post-close) or the one before (pre-open) |
| GEX lane records | gex-watch `/api/records` (gex-records/1) | every gex-signals trial, the factory slots' feature history, the shadow trader, GEX levels v1: counts and look status as published |
| Shadow ledgers | `fly-shadow` (X2 and amendments 1-3), `bounce-shadow`, `forecast-record`, `overnight-shadow` sqlite, opened `mode=ro` | the session's row (status, reason) and the rows that count toward the registered look |
| Selector, plays, trader | `selector-shadow`, `desk-state plays.db`, `gex-trader` | choices only (names, on/off, intents): no expected values, dollars or P&L |
| Source receipt | `/data/grid_ledgers/desk-receipts` (#1272) | the session's receipt: factory ledger and Sleuth leads row counts (leads are not findings), thesis log status |
| GEX site state | gex-watch `/api/state` | current GEX served or withheld, and why; a withheld reason other than the scheduled stop is a data problem |
| Desk health | desk-state `desk-health/1` files | the latest health line and its problems |
| Paper record | D4 #14: `/data/agent-home/anikdang/desk-paper-data/summary.json` (desk-paper-summary/1, written by the paper-record job in 3pacs/gex) | the summary only, never its ledger: per module the look status and counts, and `stats` where the registration allows; LOOK DUE reads "the owner runs the registered look once (not a pass)", NO LOOK reads "description, not evidence"; `errors` and a summary older than 36 h are data problems. Not supplied until the file exists. Format agreed with its owner on 2026-11-21 (side-session note `paper-record-read-format-20261121.md`). |

Identifiers in ledger SQL come from the reviewed config, are checked against `[a-z_][a-z0-9_]*` and against the
table's own columns. Every value is a bound parameter. The config holds each registration's look count (`need`);
`null` where none is registered (selector/1, the plays table), shown as "registered count not supplied".

## Wording (optional)

`--narrate` (`DESK_BRIEF_NARRATE=1`) asks the local GPU model (llama.cpp `Qwen3.8-27B-Q4_K_M` on `gridz4:8080`,
as `selector-shadow` does) for 3-5 sentences about the deterministic text. The call does not go through
`llm.router`, whose chains can reach paid providers when `GRID_ALLOW_PAID_LLM` is set. Any host outside
`LOCAL_LLM_HOSTS` is refused before a request is made. The wording is dropped when any number in it is not in
the deterministic text (`ollama.number_grounding.check_numbers_grounded`, sign included) or when it uses a word that
states a finding (edge, proven, significant, recommend, ...). The brief is complete without it.

## Run by hand

```bash
cd <checkout>   # made under umask 022: the pinned calendars refuse group-writable files
/data/grid_v4/grid_venv-314/bin/python -B -m scripts.desk_brief run --kind post_close --session 2026-11-20 \
    --as-of "$(date -u +%Y-%m-%dT%H:%M:%S+00:00)" --dry-run
```

## Not yet

- The paper record job itself (D4, another session; not deployed): until it writes its summary the section reads not supplied.
- Factory live slots appear through the Records page's `panel-factory` block (feature history, counts only). A
  slot's per-session say appears when the slot owner publishes it.
- The thesis log (#1165) does not exist; the receipt records it as unknown and the brief repeats that.
- Codex's evidence-card adapters (#1204-#1224) render factory development results; the brief does not yet show
  development cards, only forward counts.
