# Factory pre-screen: one older-data look before a live slot

Module: `analysis/factory_prescreen.py` (`factory-prescreen/2`). Ledger records:
`factory_prescreen_registered`, then `factory_prescreen_outcome`
(`analysis/ledger_steered_exploration.py`). Decided by the owner on 2026-11-20; version 2
after the reviewer's gates of the same day.

## Why

A factory candidate's evidence is its live slot in gex-signals: one look at 100 resolved
fires. Measured on the factory ledger (runs 1 to 5, 15,740 tested modules), the rules fire on
5 to 30% of sessions, so a slot takes one to five years to reach its look, and there are ten
slots. The pre-screen gives a selected candidate a first test in minutes, on sessions the
factory has never read, so slots go to rules that held up somewhere else first.

## It changes a rule of the factory design

The factory design of 2026-11-12 said this store "is never copied or read" (decision Q1, and
the load-bearing rules of the parent issue, #1153). The pre-screen is the one exception, by
the owner's amendment of 2026-11-20 (amendment 1 of the design, kept with the design in the
gex repository). The exception is this module and nothing else:

- Discovery still reads the development window only. `module_factory`'s three "holdout
  refused" guards are unchanged, so no ranking run can reach the older store.
- The live slot is still the only test that counts.
- Only SPY rows are staged. No other code path, and no one by hand, reads the store.

The amendment came after the code and after the data was staged, and it records that order.
No look had been run and no registration was on the ledger when it was made.

## What it is and is not

- It is the factory's own row builder (`module_factory.build_rows`) run for the one
  registered module on 2018-05-01 to 2022-11-11, with the settings of the run that selected
  it, in dollars a share. Fires count from 2019-01-02; 2018 fills the histories.
- It is not the live test. A pass earns a slot; the slot's look decides.
- It is not "works now". The window is 2019's rise, the 2020 crash and recovery, 2021's rise
  and the 2022 fall.
- It is not independent evidence. Edge search 9 spent one look on this store for three other
  rules, so the store is previously exposed. Every record carries
  `evidence: "screen on previously exposed older data; not independent evidence"`, every
  registration says in words what the store was used for, and the store takes ten
  registrations, ever.

## Two steps

1. **Register.** `prescreen-register` appends `factory_prescreen_registered`: the trial, the
   rule, the registration file's sha256, the terms' sha256, the store and the data file's
   sha256. It reads the registration and the ledger, and no older data. The hash chain is
   what puts the registration before the look. A timestamp in a file is not.
2. **Look.** `prescreen` needs an independent witness that the registration existed first:
   the nightly Desk source receipt, which reads the factory ledger and records its head in a
   hash-chained log of its own. The look is given three things and no others: the receipt
   log's folder (`--receipts`), an anchor copy held outside that log (`--external-anchors`,
   the vault's witness file) and the session (`--session`). The natural order is: register,
   wait for that night's receipt, look the next day.

What the look checks itself, before the data file is opened (`analysis/desk_receipt_witness.py`,
`factory_prescreen.ledger_witness`): the receipt log's chain, every payload file against its
hash and the log's local anchors; that the anchor copy is another file than the log's own and
covers that session's receipt; that the receipt read the factory ledger; and that the payload
names a line of this ledger as its head. The ledger then requires that head to be at or after
the registration, and the three times to be in order: registered, receipt read, looked.

What it does not accept: a head, a record count or a place stated by the caller, and a time
for the look. The look's time is the module's own clock.

What code cannot check, and the record says so in words instead of claiming it: that the
anchor copy presented is the one held off-host. A person checks that against the vault's
pushed file or the hub post of that session. The record carries the copy's sha256 and the
receipt's head for that comparison.

## The look

PASS needs all three:

1. the lower bound of the mean net of the fires is above zero;
2. the lower bound of that mean minus the mean net of taking the same side at the same time
   on every eligible session is above zero. A long rule cannot pass on a rising market alone;
3. the mean net of the fires without the five largest is above zero. A pass cannot rest on
   a handful of sessions.

Both bounds are at 0.05 / 10 and come from one seeded circular block bootstrap over the
eligible sessions in date order, in blocks of five sessions, so fires on neighbouring days
are resampled together. On a series with one shared shock a week the block bound is about
`sqrt(5)` wider than the bound that treats sessions as independent (tested).

Fewer than 40 fires is UNDERPOWERED: below 40 a percentile bootstrap passes noise two to
three times as often as its stated rate (simulated 2026-11-20). Anything else is FAIL. Means
by year and the share of the total net from the five largest fires are on the record and
are not tested.

## Skipped windows

The declared run's `skip_windows` remove whole (decision time, horizon) windows from that
run. A skipped window has no trial, so a registered rule's window is never one of them, and
the one-module run carries none. A registration whose window is in the declared
`skip_windows` is refused. A skipped window changes no other window's eligibility, history
or labels (tested with and without).

## Guards

| Guard | Where |
|---|---|
| The factory never reads sessions before 2022-11-14 | `module_factory` (three checks, unchanged) |
| The pre-screen never reads a session on or after 2022-11-14 | `factory_prescreen.load_older` |
| The bytes that are hashed are the bytes that are queried: one read of the file, sha256 checked, an in-memory copy queried | `factory_prescreen._sealed` |
| A `-wal`, `-shm` or `-journal` file beside the data file, or a WAL-mode file, is refused | `_sealed` |
| Two rows for one (symbol, day, minute) are refused, not overwritten | `load_older` |
| Only a selected trial, the representative of its cluster | ledger rule |
| One registration per trial; ten per store; a store's data file cannot change | ledger rule |
| A look names its registration record, matches it field for field, and is its only look | ledger rule |
| A look names a head of the ledger at or after its registration, taken from a verified Desk receipt, with the receipt, the anchor copy that covered it, and what a person must still check | `ledger_witness` at the look; ledger rule on append and reopen (shape, the ledger's own lines, the order of the three times) |
| A head stated by the caller, or a time for the look, is refused | `prescreen()`; the command has no such arguments |
| Pass means both bounds and the trimmed mean above zero; under 40 fires is underpowered | ledger rule, on append and on every reopen |
| Everything that does not need the data is checked before the data file is opened | `prescreen()` |
| SPY-only price features and tape features only | `candidate_run` (the store has no complete bars for other symbols, no events, options or macro history) |

## Registration

A JSON file, committed before the look, with exactly: `schema`
(`factory-prescreen-registration/1`), `candidate` (`run_id`, `trial_id`, `result_sha256`,
`family`, `feature`), `data_sha256`, `terms` (equal to `factory_prescreen.TERMS`),
`prior_use` (in words, what the store has already been looked at for) and `registered_at`.

A registration names a selected rule. This repository is public, so registrations are kept
in the private gex repository and only their sha256 reaches the ledger.

## Running a look

```bash
python -m analysis.module_factory prescreen-register --registration REG.json \
    --ledger grid-module-factory.jsonl --anchor grid-module-factory.anchors.jsonl
# ... the night's Desk receipt posts the ledger head off-host ...
python -m analysis.module_factory prescreen --registration REG.json --data bars.db \
    --ledger grid-module-factory.jsonl --anchor grid-module-factory.anchors.jsonl \
    --receipts /data/grid_ledgers/desk-receipts --external-anchors VAULT/05-GRID/Paper-Log/desk-receipts/grid-desk-source-receipts.anchors.jsonl \
    --session YYYY-MM-DD
```

Each appends one record and prints it. A refusal prints the reason, exits 2 and appends
nothing. Each step runs on the owner's word.

## Calendar

`docs/reference/calendar-manifests/prescreen-xnys-2018-04-02-2020-12-31.json` and
`prescreen-xnys-2021-01-01-2022-11-11.json`, pinned by sha256 in `factory_prescreen.PINS`,
built with `scripts/build_factory_calendar_manifest.py`. That script rebuilt the two
reviewed development manifests with identical sessions before it was used for these.
They are named `prescreen-xnys-*` on purpose: `scripts/prepare_factory_calendars.py` and its
tests own every `factory-xnys-*` file. Since #1303 the deployed market-state record counts session
ages on these two parts as well. The same pins are therefore restated in
`factory_session_calendar.PRESCREEN_PINS` (a test keeps them equal), and the deploy normalizes them
too. Run a look from a deployed tree or from a checkout made under `umask 022`; otherwise the reader
refuses a group-writable manifest.

## Where the result shows

`summarize()["live"]["prescreen"]`: registered, registered and not yet looked at, looked,
passed, failed, underpowered, registrations left per store, each verdict, and the evidence
label. A live slot's registration (gex-signals `panel-factory/2`) must carry the sha256 of a
passed look, and gex-signals verifies that record against this ledger before it registers
the slot.

## Not done

- No registration is on the ledger and no look has been run.
- That the anchor copy is the one held off-host is not checked by code; the record says a person must.
- The witness needs the receipt log's anchors exported to the vault file and that file committed and pushed:
  `scripts/desk_receipts_witness_sync.sh` does this after every receipt run once the owner installs it
  (DESK_SOURCE_RECEIPTS.md, "Not done"). Before any registration, the receipt unit must also move to a pinned
  checkout whose factory-ledger reader knows `FACTORY_PRESCREEN_REGISTERED` (7e3a263, the copy running on
  2026-11-21, does not, and would record `factory_ledger` as unknown on the night after a registration).
- Calendar-flag, cross-symbol, options and macro features cannot be pre-screened.
- The block bootstrap handles neighbouring days. Dependence across longer stretches (a whole
  regime) is not modelled; the by-year means are there to be read.
