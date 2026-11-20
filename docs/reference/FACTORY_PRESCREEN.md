# Factory pre-screen: one older-data look before a live slot

Module: `analysis/factory_prescreen.py`. Ledger record: `factory_prescreen_outcome`
(`analysis/ledger_steered_exploration.py`). Decided by the owner on 2026-11-20.

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
- The store is not untouched. Edge search 9 spent one look on it for three other rules.
  Every registration says so, and each further look wears the store: ten looks, ever.

## The look

PASS needs both, each at 0.05 / 10:

1. the seeded bootstrap lower bound of the mean net of the fires is above zero (the live
   look's statistic, the same function);
2. the lower bound of that mean minus the mean net of taking the same side at the same time
   on every eligible session is above zero. A long rule cannot pass on a rising market alone.

Fewer than 40 fires is UNDERPOWERED: below 40 the bootstrap passes noise two to three times
as often as its stated rate (simulated 2026-11-20). Anything else is FAIL. Means by year are
on the record and are not tested.

## Guards

| Guard | Where |
|---|---|
| The factory never reads sessions before 2022-11-14 | `module_factory` (three checks, unchanged) |
| The pre-screen never reads a session on or after 2022-11-14 | `factory_prescreen.load_older` |
| The data file is the registered one, by sha256 | `load_older` |
| Only a selected trial, the representative of its cluster | ledger rule |
| One look per trial; ten looks per store; a store's data file cannot change | ledger rule |
| Pass means both bounds above zero; under 40 fires is underpowered | ledger rule, on append and on every reopen |
| The trial, the store's looks and the registration are checked before the data file is opened | `prescreen()` |
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
python -m analysis.module_factory prescreen --registration REG.json --data bars.db \
    --ledger grid-module-factory.jsonl --anchor grid-module-factory.anchors.jsonl
```

It appends one record and prints the verdict. A refusal prints the reason, exits 2 and
appends nothing. Each look runs on the owner's word.

## Calendar

`docs/reference/calendar-manifests/prescreen-xnys-2018-04-02-2020-12-31.json` and
`prescreen-xnys-2021-01-01-2022-11-11.json`, pinned by sha256 in `factory_prescreen.PINS`,
built with `scripts/build_factory_calendar_manifest.py`. That script rebuilt the two
reviewed development manifests with identical sessions before it was used for these.
They are named `prescreen-xnys-*` on purpose: `scripts/prepare_factory_calendars.py` and its
tests own every `factory-xnys-*` file and normalize only the development pins. Run a look
from a checkout made under `umask 022`, or the reader refuses a group-writable manifest.

## Where the result shows

`summarize()["live"]["prescreen"]`: looks, passed, failed, underpowered, looks left per
store, and each verdict. A live slot's registration (gex-signals `panel-factory/2`) must
carry the sha256 of a passed pre-screen record.

## Not done

- No look has been run.
- The older bars are not staged on the server.
- Calendar-flag, cross-symbol, options and macro features cannot be pre-screened.
- Fires on consecutive days are treated as independent; clustering would make the bounds
  too tight.
