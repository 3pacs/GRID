# Disposable database teardown receipt (2026-09-18, Fable)

- Host: gridz4 (PostgreSQL 14.24, cluster `main`, port 5432, localhost listener; reached via `ssh -N -L 55432:127.0.0.1:5432 gridz4`).
- Preflight (read-only, before create): host confirmed; cluster online; 376 GB free; `db_collision=0`, `role_collision=0`; local TCP rule `host all all 127.0.0.1/32 scram-sha-256`.
- Created: database `griddb_fable_20260918_1740` owned by role `fable_test_20260918_1740` (LOGIN, NOSUPERUSER, NOCREATEDB, NOCREATEROLE). No other database, role, or setting touched.
- Used by: proof runs 1–3 (compositions 6e8df878 → 783ff735 → 42df4362 → c9e34036); results in RESULTS.md.
- Teardown (after run 3): tunnel PID stopped; `pg_terminate_backend` on the scratch DB's sessions; `DROP DATABASE griddb_fable_20260918_1740`; `DROP ROLE fable_test_20260918_1740`; verification `remaining_db=0`, `remaining_role=0`; local port 55432 closed; local credential file `fable-db.env` deleted.
