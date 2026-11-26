# GRID — agent instructions

This is the shared entry point for Codex, Claude, Gemini and Antigravity.
Read `CLAUDE.md` for the maintained core rules and reference index. The old
running log is preserved byte-for-byte at
`docs/agent-history/AGENTS-legacy-20261125.md`; it is historical evidence, not a
current service inventory. Read matching history when recovering a scoped task.

## Recover the current task

- Inspect Git status, branch, base commit, applicable nested instructions,
  `.coordination.md`, and recent relevant reports before editing.
- Check overlapping PRs and the canonical vault's active claims before taking
  shared ownership. Use the current lane/claim workflow. Do not preempt another
  writer or treat a claim as deployment authorization.
- Before building, consult `docs/MODULE_INVENTORY.md` and search existing modules.
  Extend the existing capability. Follow the reference index in `CLAUDE.md` for
  availability, prediction, research, source receipts and subsystem contracts.
- Delegate only when the current user request or applicable instructions call
  for agent delegation. When dispatch is authorized, use the existing prompt
  template in `docs/AGENT_PROMPT_TEMPLATE.md`.

## Preserve the system's guarantees

- Preserve point-in-time correctness and no-lookahead semantics. Keep source
  capture, availability and publication clocks distinct; repeated reads do not
  make an old observation fresh. Missing, stale and modeled values stay labeled.
- Use parameterized SQL, the existing DB helpers and explicit transaction
  boundaries. Preserve decision-journal immutability and source provenance.
- Use GPU inference through the existing router. Do not recreate retired CPU
  inference services. Paid providers stay off unless `GRID_ALLOW_PAID_LLM` is set
  and the task explicitly authorizes them.
- Keep secrets in approved local stores, never instructions, reports or commits.
  Preserve existing private-use/public-cohort filters and access boundaries.
- Source work and synthetic tests do not authorize ingestion, provider capture,
  backfills, scheduler changes, evaluation admission, trading or deployment.

## Development and verification

- Use the project's `.venv` and current dependency contracts. Python 3.14 setup
  uses `requirements.txt` plus `deploy/python314/requirements-runtime.txt` under
  `deploy/python314/constraints-3.14.txt`. Other supported interpreters follow
  current CI. Do not install dependencies into the system Python.
- Use the Node version pinned by `.github/workflows/test.yml`; respect
  `pwa/.npmrc` engine checks. Frontend dependencies use `npm ci` in `pwa/`.
- Run focused pytest targets for backend work; use a disposable local test DB
  through `GRID_TEST_DB_URL` when needed. Never run destructive tests against
  production. Synthetic tests do not prove PostgreSQL/runtime behavior.
- Frontend changes need the existing `npm test` and `npm run build` checks.
  Run required real-browser/WebGL gates when the affected behavior needs them.
- Before declaring live health, use current service checks and the existing
  error-log audit; a build or passing source test is not a live result.

## Deployment and handoff

- Merges to main need Codex's accept at the exact head, green CI, the merge
  coordinator's ACK and Anik's own typed word. Releases, shared-service
  restarts, migrations and activations are Anik's alone.
- Verify the running service's actual checkout, revision and working directory.
  Use `docs/SERVER-SERVICES.md` and the current deployment workflow. Historical
  `grid_repo`, `astrogrid_dedup` or other tree names are not proof of live source.
- Re-check source hashes and ownership immediately before integration. Preserve
  unrelated edits, stage explicit paths, and do not restart shared services or
  deploy outside task authorization and applicable gates.
- Record material decisions, exact bases, verification and remaining work in
  `.coordination.md` and the appropriate dated report. File through the available
  fleet reporter and inspect its delivery receipt. Put open work in the canonical
  task list. Keep this entry file concise; do not append session transcripts here.
- `.coordination.md` entries append at the bottom, so PRs that each add one
  conflict there when they merge in a batch. Rebase onto current main and keep
  every entry when resolving; never drop another PR's entry.
