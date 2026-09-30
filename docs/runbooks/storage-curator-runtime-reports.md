# Storage curator runtime reports

Hermes `CHECK_STORAGE` dispatches `scripts/storage_curator.py`. When that module
runs from `/data/grid_v4/grid_release.releases/<sha>/`, it writes stamped reports
and `storage_maintenance_latest.{json,md}` to
`/data/grid_v4/storage_maintenance/`, outside the immutable release. The grid
service user must have write permission there. A developer checkout keeps the
relative `outputs/storage_maintenance/` default.

The two generated latest aliases are no longer tracked in Git. Historical
stamped reports remain in the repository for this change; new local reports are
ignored. Repository search found no code reader of the latest aliases, only the
writer, tests, and historical documentation. The JSON and Markdown paths
returned by `run_storage_maintenance` identify the active output directory.

## Release handoff

1. Preserve the existing live `outputs/storage_maintenance/` directory in a
   verified backup before switching releases. Do not delete or overwrite it.
2. Confirm `/data/grid_v4/storage_maintenance/` exists and is writable by the
   `grid` service user. If historical report continuity is needed, copy the
   backed-up stamped and latest files into that directory without replacing
   any existing files; verify counts and hashes before relying on the copy.
3. Deploy the reviewed commit at its exact SHA. Verify the active Hermes and
   goal-worker process paths point into that release and the release worktree
   remains clean after `CHECK_STORAGE` runs. Confirm the returned report paths
   and both latest aliases are under `/data/grid_v4/storage_maintenance/`.

This code change does not perform the live copy or start `CHECK_STORAGE`.
