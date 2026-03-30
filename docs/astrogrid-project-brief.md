# AstroGrid Project Brief

## Notion Database Entry

- Name: AstroGrid
- Status: Active
- Stage: Build / Calibration
- Priority: High
- Owner: GRID
- Repo: `GRID`
- Branch: `main`
- Public URL: [https://grid.stepdad.finance/astrogrid/](https://grid.stepdad.finance/astrogrid/)
- Local Path: `/Users/anikdang/dev/GRID`
- Server Path: `/data/grid_v4/astrogrid_dedup`

## One-Line Summary

AstroGrid is a mystical Oracle interface backed by strict prediction logging, postmortems, scoring, backtests, and review-gated weight adjustment on top of GRID data.

## Current Focus

- prediction quality
- scoreability discipline
- group-aware review logic
- historical regime integration
- calibration of mystical weights against GRID-only baselines

## Current State

- live app is up
- prediction logging is implemented
- immediate postmortem persistence is implemented
- scoring loop is implemented
- backtests are implemented
- review runs and weight proposals are implemented
- historical ephemeris features are integrated
- regime slices are populated
- group-aware review is live

## Current Findings

- mystical signal is not uniformly useful
- current value is conditional by asset group
- review logic is reducing mystical weight where it degrades outcomes
- regime slicing is structurally correct, but long-range regime history is still shallow

## Current Risks

- regime history coverage is limited
- some asset groups still have uneven depth/quality
- small-sample slices can mislead if not gated
- mystical features may mostly be noise and should remain skeptical by default

## Next Milestone

- persist question intent and target group explicitly at prediction write
- improve review discipline with stronger sample thresholds
- expand meaningful historical regime coverage

## Key Files

- `/Users/anikdang/dev/GRID/docs/astrogrid-project.md`
- `/Users/anikdang/dev/GRID/store/astrogrid.py`
- `/Users/anikdang/dev/GRID/api/routers/astrogrid.py`
- `/Users/anikdang/dev/GRID/oracle/astrogrid_universe.py`
- `/Users/anikdang/dev/GRID/scripts/run_astrogrid_learning_loop.py`
