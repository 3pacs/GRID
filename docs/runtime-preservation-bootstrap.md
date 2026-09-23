# Release runtime preservation gate

The release swap now requires `/data/grid_v4/grid_release.releases/.runtime-preservation` before it builds or swaps a candidate. The file has exactly three lines:

```text
scheduler=/data/grid_v4/grid_release.releases/<immutable-running-scheduler-release>
recovery=/data/grid_v4/grid_release.releases/<immutable-pre-sequence-api-release>
scheduler_sha=<40-character-Git-SHA-of-scheduler-directory>
scheduler_tree=<40-character-Git-tree-of-scheduler-HEAD>
recovery_sha=<40-character-Git-SHA-of-recovery-directory>
recovery_tree=<40-character-Git-tree-of-recovery-HEAD>
```

The two directories must exist as canonical, direct children of the release root. Each must be a Git checkout whose HEAD and HEAD tree match the approved record, with no tracked-file changes; untracked runtime files may remain. The scheduler directory must also match the running `grid-scheduler` PID's resolved cwd, and the effective systemd `WorkingDirectory` must be that exact immutable directory. The script checks all of this while holding its deploy lock. It exits 5 before the build on a missing or invalid record, changed process, or mutable unit path. It retains both directories through every prune pass regardless of mtime, in addition to the candidate and immediate previous release.

Before the first automatic deploy, the release controller must obtain a **fresh** server-UTC bounded receipt: `readlink -f /data/grid_v4/grid_release`, a `find` inventory of the release root, `systemctl show grid-scheduler -p MainPID -p WorkingDirectory`, `/proc/<MainPID>/cwd`, Git HEAD in each release, API/Hermes process cwds and SHAs, on-disk scheduler unit/drop-ins, and Alembic version. Confirm the intended scheduler and recovery identities against that receipt. The historical #606/#624 receipt is not current authorization or a substitute for this check.

With a separate production configuration authorization, create the scheduler drop-in `zz-release-worktree.conf` with `WorkingDirectory=<resolved immutable running scheduler directory>`. Preserve the prior on-disk drop-in separately. Run `systemctl daemon-reload` **without restarting** the scheduler, then verify effective `WorkingDirectory` equals the immutable directory and the same PID, cwd, and SHA remain. Under the release lock, write the six-line record atomically (temporary file then rename) after rechecking the identities. Keep the pre-sequence API release as `recovery` through the full two-deployment sequence and acceptance window. If any identity is absent, outside the root, or different from the approved values, stop before merge.

An explicit `activate_scheduler=true` dispatch now writes the resolved immutable candidate release directory to the unit drop-in before the acknowledged restart. After activation, the existing preservation record intentionally becomes stale and blocks any later deploy. Before a later release, take a fresh runtime/recovery decision and update the record under the deploy lock; do not silently repurpose the old recovery path. `.previous-release` is only one-step metadata. Restoration requires a separate decision covering retained code identity, schema compatibility, writers, and process verification; this change performs no automatic rollback.

Test on Linux with `bash tests/deploy/test_runtime_preservation.sh` and `bash tests/deploy/test_deploy_release_swap.sh`. Git Bash on Windows cannot run the `flock`-based integration tests faithfully.
