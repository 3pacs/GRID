# 09 — Move test CI to the Dell runner ("alien", Precision 5810 on the tailnet)

Branch: `claude/handoff-09-ci-dell-runner`. Lane: code (workflow YAML) + operator
steps on the Dell. Task #26 in the session task list; the operator deferred it
until now.

## Goal

`test.yml` (Lint, Backend Tests, Frontend Build) runs on `ubuntu-latest` and
takes ~10 minutes per PR, mostly pip install and Postgres pull. The Dell
"alien" (Precision 5810, on the tailnet) should become a second self-hosted
runner that runs the test jobs with a warm cache, while grid-svr keeps the
deploy and ops lanes. The grid-svr runner also stalls (19:28 UTC today), so a
second runner is resilience too.

## Steps

1. Operator (on alien): install the GitHub Actions runner for `3pacs/GRID` with
   labels `self-hosted, alien, tests`; Docker or a local PostgreSQL 15 +
   TimescaleDB for the test DB; Python 3.11; Node 20; register as a systemd
   service (`actions.runner.3pacs-GRID.alien`). Write the exact commands into
   `docs/SERVER-SERVICES.md` under a new "alien runner" section.
2. Workflow: add `runs-on` matrix or a `TEST_RUNNER` repository variable so
   `test.yml` jobs run on `[self-hosted, alien]` when the variable is set and
   fall back to `ubuntu-latest` otherwise (so CI never blocks on the Dell being
   down). Cache pip (`~/.cache/pip`) and `pwa/node_modules` on the runner.
   Postgres on the Dell should be a persistent service the job resets
   (`DROP SCHEMA public CASCADE; CREATE SCHEMA public` on the test DB) rather
   than a container pulled per run.
3. Timeouts: `timeout-minutes` on every job; concurrency group per PR so a new
   push cancels the previous run.
4. Keep `deploy.yml` and `ops-exec.yml` on grid-svr. Do not give the Dell
   deploy credentials.
5. Verify: open a trivial PR from your branch, confirm the three jobs run on
   alien in under 4 minutes, then merge.

## Done when

Two consecutive PRs run their test jobs on alien with a warm cache, the
fallback to ubuntu-latest is proven once by unsetting the variable, and the docs
describe the runner set-up.
