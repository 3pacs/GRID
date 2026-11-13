# Contributing to GRID

GRID has one human owner. Software agents (Claude Code sessions and Codex) write and merge
most changes. These rules apply equally to people and to agents.

**Every merge to `main` deploys to production** through
`.github/workflows/deploy.yml`, so `main` only accepts changes through pull requests.

## Before you write code

- Read `CLAUDE.md` and the rule files in `.claude/rules/`. They cover point-in-time (PIT)
  data access, SQL safety, the immutable decision journal, and testing.
- Check that the capability does not already exist: see `docs/MODULE_INVENTORY.md` and
  `docs/reference/CODEBASE_MAP.md`. Most tasks are "extend and wire", not "build new".
- Agents also follow `docs/AGENT_PROMPT_TEMPLATE.md`.

## Making a change

1. Branch from an up-to-date `origin/main`. Use a prefix that names who made it:
   `claude/…`, `codex/…`, `fix/…`, `feat/…`.
2. Add or update tests in `tests/` (backend) or `pwa/src/__tests__/` (frontend). Every new
   module needs a test file.
3. Run the tests locally:

   ```bash
   python -m pytest tests/ -v               # backend
   cd pwa && npm run test && npm run build  # frontend
   ```

4. Open a pull request against `main` and fill in the template.

## Getting a pull request merged

- **Required checks** must pass on the latest commit of the PR: `Backend Tests`,
  `Frontend Build`, `Lint`, `Migration replay (schema.sql + alembic upgrade head)`, and
  `evals-freeze-guard`. The other workflows give useful signal but do not block merges.
- **The branch must be up to date with `main`.** If `main` has moved, run
  `gh pr update-branch <number>` and wait for the checks to pass again.
- **Merge method:** use a merge commit (`gh pr merge <number> --merge`). Squash is allowed
  for a single logical change; rebase merging is turned off. Merge commits keep the SHA
  that CI tested.
- **Auto-merge:** `gh pr merge <number> --auto --merge` merges the PR once the checks
  pass. Use it instead of polling.
- **Never** push directly to `main`, force-push it, or bypass the checks.

### Stacked pull requests

You can stack a PR on another PR's branch. CI workflows only run for PRs that target
`main`, so a stacked PR has **no CI** until it is retargeted. When the parent merges,
GitHub retargets the child to `main`, but that alone does not start CI. Run
`gh pr update-branch <child>` (or push a commit), then wait for the required checks
before you merge the child.

## Emergencies

Only the owner may change or pause branch protection, and the owner records each
override. Agents must not edit rulesets, branch protection, or repository settings. If
CI itself is broken, report it to the owner instead of working around it.

## Security issues

Do not open public issues for vulnerabilities. See `SECURITY.md`.
