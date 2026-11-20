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
2. **Look.** `prescreen` needs a head of the ledger at or after the registration that was
   recorded off-host before the look, and where (`--witness-records`, `--witness-head`,
   `--witness-where`). The nightly Desk source receipt reads the factory ledger and its hub
   post carries the head, so the natural order is: register, wait for that night's receipt,
   look the next day. The ledger checks the head against its own lines. Whether the off-host
   copy exists is checked by a person, and the record says so.

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
| A look names a head of the ledger at or after its registration, stated as recorded off-host | ledger rule (consistency with the ledger's own lines) |
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
    --witness-records N --witness-head SHA256 --witness-where "desk-receipt <session>, hub post"
```

Each appends one record and prints it. A refusal prints the reason, exits 2 and appends
nothing. Each step runs on the owner's word.

## Calendar

`docs/reference/calendar-manifests/prescreen-xnys-2018-04-02-2020-12-31.json` and
`prescreen-xnys-2021-01-01-2022-11-11.json`, pinned by sha256 in `factory_prescreen.PINS`,
built with `scripts/build_factory_calendar_manifest.py`. That script rebuilt the two
reviewed development manifests with identical sessions before it was used for these.
They are named `prescreen-xnys-*` on purpose: `scripts/prepare_factory_calendars.py` and its
tests own every `factory-xnys-*` file and normalize only the development pins. Run a look
from a checkout made under `umask 022`, or the reader refuses a group-writable manifest.

## Where the result shows

`summarize()["live"]["prescreen"]`: registered, registered and not yet looked at, looked,
passed, failed, underpowered, registrations left per store, each verdict, and the evidence
label. A live slot's registration (gex-signals `panel-factory/2`) must carry the sha256 of a
passed look, and gex-signals verifies that record against this ledger before it registers
the slot.

## Not done

- No registration is on the ledger and no look has been run.
- The off-host copy of the witnessed head is not checked by code.
- Calendar-flag, cross-symbol, options and macro features cannot be pre-screened.
- The block bootstrap handles neighbouring days. Dependence across longer stretches (a whole
  regime) is not modelled; the by-year means are there to be read.
