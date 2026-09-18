# HELD — Signal weight overrides policy flip (GRID W7b)

**Status: HELD. Not deployed. Requires explicit approval before merge/release.**

Branch: `fable/overrides-policy-20260918`
Contains: `fable/learning-safeguards-20260918` (the safeguard capability) plus one
additional commit (this change) that flips two defaults in
`intelligence/signal_weight_overrides.py`.

## What this is

This is a **production weight change**, not a refactor. It flips the two knobs that
gate `intelligence/signal_weight_overrides.py`'s conviction-multiplier table:

| Knob | Old default | New default (this branch) |
|---|---|---|
| `GRID_SIGNAL_OVERRIDES_ENABLED` | `True` | `False` |
| `GRID_SIGNAL_OVERRIDES_REQUIRE_LEDGER` | `False` | `True` |

Any deployment that restarts on this branch, with these env vars unset, will:

- Apply **no** signal weight overrides at all (`GRID_SIGNAL_OVERRIDES_ENABLED=False`),
  where it previously applied the full `SIGNAL_WEIGHT_OVERRIDES` /
  `DEFERRED_SIGNAL_OVERRIDES` tables (vix_exposure ×1.40, credit_cycle ×0.20,
  equity ×1.20, vol ×0.30, fx ×0.40, news_intel ×0.60, etc. — see that module's
  docstring for the full table and derivation).
- Even if an operator re-enables the switch, overrides will apply **only** with a
  matching `governance.promotion_ledger` approval (`GRID_SIGNAL_OVERRIDES_REQUIRE_LEDGER=True`)
  — with no approval, nothing is applied, and one WARNING is logged.

**Today's live weight cuts/boosts stop the moment this branch is deployed**, until a
`kind="weight_override"` promotion is recommended and approved in the ledger for the
current table (see `governance/promotion_ledger.py` and
`docs/reference/LEARNING_PROMOTION_PROTOCOL.md`).

## Why it's held, not merged

The safeguard *capability* (`fable/learning-safeguards-20260918`) was built specifically
so that shipping the ledger-enforcement mechanism would not, by itself, silently change
live trading behaviour. This commit is the deliberate, separate act of actually
exercising that mechanism to change what's live — and a change to live weights needs a
human decision, not a code review alone.

The underlying justification (broken signal meter, contaminated `trade_postmortems`
scoring corpus — see the 2026-09-17 vault audit and this module's SAFEGUARDS docstring
section) is believed sound, but:

- The controller-decision list in `docs/reference/LEARNING_PROMOTION_PROTOCOL.md`
  (historical reconstruction, exclusions, backfill/rescoring, weight disabling, NEUTRAL
  policy, no_data close-out) is still open. Flipping these defaults is itself entry #7
  on that list.
- No `weight_override` promotion has been recommended or approved in
  `governance.promotion_ledger` for the current table as of this writing.

## Who must approve

The GRID W7 operator/controller who owns the promotion-ledger process must:

1. Review this diff and the linked audit/controller-decision list.
2. Decide whether to deploy with overrides off (accept the behaviour change described
   above) or instead go through `governance.promotion_ledger.recommend()` +
   `approve()` for the current override table before/instead of merging this.
3. Only after that decision is recorded should this branch be merged into whatever
   branch feeds production, and production redeployed.

Do not merge or deploy this branch without that sign-off.

## Rollback

This change is a single, self-contained commit. To undo it:

```
git revert <this-commit-sha>
```

That restores `GRID_SIGNAL_OVERRIDES_ENABLED` to default `True` and
`GRID_SIGNAL_OVERRIDES_REQUIRE_LEDGER` to default `False` — i.e. back to the
`fable/learning-safeguards-20260918` state (current production behaviour, with the
ledger-enforcement capability present but not required by default).

No database migration, data backfill, or config file change is needed for either the
flip or the rollback — both defaults are read from `os.environ` at import time with no
persisted state beyond the promotion ledger itself.
