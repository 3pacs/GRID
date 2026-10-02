# evals/: frozen, versioned evaluation suites

Every suite under `evals/<dir>/` is hash-pinned by its own `MANIFEST.sha256`
(one `<sha256>  <path>` line per file; text files hashed with CRLF normalised
to LF). What was **released** is recorded once, append-only, in
[`evals/RELEASED.json`](RELEASED.json). The `evals-freeze-guard` workflow
(`.github/workflows/evals-freeze.yml`) checks every PR to `main` and every push
to `main` against the **base** revision, using the **base** revision's copy of
[`evals/released.py`](released.py). A PR therefore cannot weaken the guard and
the thing it guards in the same change.

## Who may change what

| Change | Allowed? | How |
|---|---|---|
| Edit or delete a released entry in `RELEASED.json` | **Never** | Releases are history. Correct a mistake by appending a new entry. |
| Change any file under a released suite path (e.g. `evals/e0/`) | Only as a **new released version** | Re-pin the suite's manifest and append one `suite` entry in the same PR. Owner approval. |
| Keep an old version live next to a new one | Yes | Release the new version as a sibling package (e0-v2 at `evals/e0v2/`). Each path stays pinned to its own latest entry, so e0-v1 at `evals/e0` keeps hash `75489d50…`, which VS1 v8 verifies at run time. |
| Add a new suite (`evals/<x>/` with a `MANIFEST.sha256`) | Yes | Append its first `suite` entry in the same PR. |
| Delete a released suite path | **Never** | Retire it in docs; the files stay. |
| Change a guard file (the latest `guards` entry's `files`) | Only with a **new `guards` entry** | Append `guards-vN+1` with the new hashes. Owner approval. A guards entry may add or re-hash files but never drop one, and `evals/released.py` plus the workflow are always pinned; retiring a guard file needs an owner override. |

Owner approval is a merge decision made by the repository owner. The guard
makes every such change visible as one appended entry; it cannot judge whether
the change is a good idea.

## Releasing a version

1. Make the change, then re-pin the suite with its own tool:
   - E0: bump `evals.e0.VERSION` and `config.json`, then
     `python -m evals.e0 manifest --write --version e0-vN` (in a sibling
     package if the old version must stay live).
   - E1: bump `evals.e1.SUITE_VERSION` (e.g. `e1-v1.1` -> `e1-v1.2`), then
     `python -m evals.e1.manifest --write`. E1's manifest has no version
     header, so `tests/test_evals_released.py` checks that `SUITE_VERSION`
     equals the latest `e1` entry's `version`.
   - E2: bump `evals.e2.VERSION` and `rules.json`, then
     `python -m evals.e2 manifest --write --version e2-vN`.
   - E3: bump `evals.e3.VERSION`, then `python -m evals.e3.manifest --write`
     (versioned header, so `version` must equal it).
2. Hash the new manifest: `python evals/released.py lf-sha256 evals/<dir>/MANIFEST.sha256`.
3. Append **one** entry to `entries` with the next `seq`:

   ```json
   {"seq": <next seq>, "kind": "suite", "suite": "e1", "version": "<new e1 version>", "path": "evals/e1",
    "manifest_sha256": "<64 hex>", "released_in": "#<PR>",
    "approved_by": "<owner>", "note": "why this version exists"}
   ```

   `version` must be new for that suite. For a manifest with a `# version:`
   header (E0, E2), `version` must equal the header. Never edit an existing
   entry, including the previous version for the same path.
4. To change a guard file, append a `guards` entry instead:
   `{"seq": N, "kind": "guards", "version": "guards-vN", "files": {"<path>": "<lf sha256>", ...}}`.
   List every file that should stay pinned, not just the changed one.
   `python evals/released.py lf-sha256 <paths>` prints the hashes.
5. Run the guard locally against a clean export of `origin/main`:

   ```bash
   git worktree add /tmp/evals-base origin/main
   python -I evals/released.py check --base-dir /tmp/evals-base --head-dir .
   ```

Released today (see `RELEASED.json`): e0-v1 at `evals/e0`, e1-v1, e1-v1.1 then
e1-v1.2 at `evals/e1`, e2-v1 at `evals/e2`, e3-v1 at `evals/e3`.

Two PRs that each append an entry will conflict on `RELEASED.json`. That is
intended: the second one rebases and takes the next `seq`.

## Reading the guard's failures

| Rule | Message | Meaning and fix |
|---|---|---|
| R1 | `released entry <seq> changed/deleted` | An existing entry was edited, removed or reordered. Restore it byte-for-byte; append instead. |
| R2 | `schema must be 1`, `seq must be contiguous`, `fields must be exactly …` | The registry is malformed. Fix the JSON shape. |
| R3 | `<suite> manifest is not a released version` | The suite's `MANIFEST.sha256` differs from the latest entry for that path. Append a new released entry, or revert the change. |
| R4 | `changed:` / `missing:` / `unpinned:` / `committed bytecode not allowed` | A file under a suite path does not match its manifest line. Re-pin and release, or revert. Symlinks, non-regular files and (in CI's fresh checkout) committed `__pycache__`/`*.pyc` are refused, because a committed unchecked-hash `.pyc` can be imported instead of the pinned source. |
| R5 | `guard file <path> changed without a new guards entry`, `unpins guard file` | A guard file changed: append a `guards` entry, or revert. A new guards entry must keep every previously pinned file. |
| R6 | `duplicate release <suite> <version>` | A `(suite, version)` pair is registered twice, or one path is given to two suites. Use a new version name. |
| R7 | `unreleased suite evals/<x>`, `symlink not allowed`, `would shadow the guard module` | A directory has a `MANIFEST.sha256` but no entry (append its first entry), or `evals/` holds a symlink or a `released*` name that would shadow `released.py`. |
| R8 | `N new entries for <path> in one change` | Release one version per path per PR. |
| R9 | `released suite <x> is missing from the head tree` | A released suite path was deleted. Restore it. |

## Limits

- The workflow uses `pull_request_target`, so it always runs the base branch's
  copy of itself. It blocks merges only once `evals-freeze-guard` is a
  **required status check** on `main` (owner setting; see EVAL-E0H3). Until then
  it reports but cannot block.
- The first PR that adds the guard, and the push that merges it, print
  `bootstrap: no base guard` because the base has no `evals/released.py` yet.
  `tests/test_evals_released.py` checks the head tree against itself in the
  Backend Tests job meanwhile.
- `RELEASED.json` itself is not pinned in `guards`: R1 protects its released
  entries, and appending is the only legitimate change.
- `pull_request_target` checks the PR head against the base **at event time**
  and does not re-run when `main` moves. Two appends to `RELEASED.json`
  conflict textually, but other combinations may not, so the owner should
  enable "require branches to be up to date" on the ruleset. The `push` run on
  `main` reports anything that slips through after merge.
- Content is checked as checked out. A head `.gitattributes` could make the
  working tree differ from the committed blob, but every checkout (CI and
  deploy) applies the same attributes, so the deployed bytes are the checked
  bytes.
