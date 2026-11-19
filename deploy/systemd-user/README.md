# GRID systemd user units pinned to one commit

Templates here are for `systemctl --user` units that run from a checkout pinned to one
reviewed commit, written `@CODE@` in the template. They are a different contract from
`deploy/systemd/`:

| | `deploy/systemd/` | `deploy/systemd-user/` |
|---|---|---|
| Runs from | `/data/grid_v4/grid_release`, which every deploy moves to `main` | `@CODE@`, a checkout that moves only when a person moves it |
| Kind | system unit, `User=grid` | user unit of `grid`, no `User=` |
| Kept in sync by deploys | yes, when marked `# deploy-sync: managed` | never |
| Guard | `tests/test_systemd_template_invariants.py` | `tests/test_systemd_user_template_invariants.py` |

A unit belongs here only when its output must not change because `main` did: an append-only
log whose line format is part of what later readers verify. Everything else belongs in
`deploy/systemd/` and runs from the release tree.

The guard for this directory checks that a template uses `@CODE@` for its working directory
and for every repository path it runs, never the release tree or the old `grid_repo`
checkout, takes its environment from the same `.env` file the system units use, and sets no
`User=`. It also checks that no template in `deploy/systemd/` uses `@CODE@`, so a pinned unit
cannot sit in the release-tree directory by accident.

Nothing installs these. The header of each template has the install lines.

* `grid-desk-receipts.{service,timer}`: the Research Desk source receipt after each session
  close. See `docs/reference/DESK_SOURCE_RECEIPTS.md`.
