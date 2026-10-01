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
| Change a guard file (the latest `guards` entry's `files`) | Only with a **new `guards` entry** | Append `guards-vN+1` with the new hashes. Owner approval. |

Owner approval is a merge decision made by the repository owner. The guard
makes every such change visible as one appended entry; it cannot judge whether
the change is a good idea.

## Releasing a version

1. Make the change, then re-pin the suite with its own tool:
   - E0: bump `evals.e0.VERSION` and `config.json`, then
     `python -m evals.e0 manifest --write --version e0-vN` (in a sibling
     package if the old version must stay live).
   - E1: `python -m evals.e1.manifest --write`.
2. Hash the new manifest: `python evals/released.py lf-sha256 evals/<dir>/MANIFEST.sha256`.
3. Append **one** entry to `entries` with the next `seq`:

   ```json
   {"seq": 3, "kind": "suite", "suite": "e1", "version": "e1-v1.1", "path": "evals/e1",
    "manifest_sha256": "<64 hex>", "released_in": "#767",
    "approved_by": "<owner>", "note": "why this version exists"}
   ```

   `version` must be new for that suite. For a manifest with a `# version:`
   header (E0), `version` must equal the header.
4. To change a guard file, append a `guards` entry instead:
   `{"seq": N, "kind": "guards", "version": "guards-v2", "files": {"<path>": "<lf sha256>", ...}}`.
   List every file that should stay pinned, not just the changed one.
   `python evals/released.py lf-sha256 <paths>` prints the hashes.
5. Run the guard locally against a clean export of `origin/main`:

   ```bash
   git worktree add /tmp/evals-base origin/main
   python -I evals/released.py check --base-dir /tmp/evals-base --head-dir .
   ```

Two PRs that each append an entry will conflict on `RELEASED.json`. That is
intended: the second one rebases and takes the next `seq`.

## Reading the guard's failures

| Rule | Message | Meaning and fix |
|---|---|---|
| R1 | `released entry <seq> changed/deleted` | An existing entry was edited, removed or reordered. Restore it byte-for-byte; append instead. |
| R2 | `schema must be 1`, `seq must be contiguous`, `fields must be exactly …` | The registry is malformed. Fix the JSON shape. |
| R3 | `<suite> manifest is not a released version` | The suite's `MANIFEST.sha256` differs from the latest entry for that path. Append a new released entry, or revert the change. |
| R4 | `changed:` / `missing:` / `unpinned:` | A file under a suite path does not match its manifest line. Re-pin and release, or revert. Symlinks and non-regular files are refused. |
| R5 | `guard file <path> changed without a new guards entry` | A guard file changed. Append a `guards` entry, or revert. |
| R6 | `duplicate release <suite> <version>` | A `(suite, version)` pair is registered twice, or one path is given to two suites. Use a new version name. |
| R7 | `unreleased suite evals/<x>` | A directory has a `MANIFEST.sha256` but no entry. Append its first entry. |
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
