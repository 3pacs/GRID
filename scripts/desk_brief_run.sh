#!/usr/bin/env bash
# Write one desk brief (3pacs/GRID#1164) for the session the timer names.
#
#   usage: desk_brief_run.sh pre_open|post_close
#
# Run by grid-desk-brief-{pre-open,post-close}.service from the root of a pinned GRID checkout.
# The brief is a private file: DESK_BRIEF_DIR is created 0700 and each brief is written 0600, once.
# Nothing is posted, served or sent. The database is read in a read-only transaction (peer auth on
# the local socket, no password); the shadow ledgers are opened read-only.
#
# Environment:
#   DESK_BRIEF_DIR      output folder (required)
#   DESK_BRIEF_PYTHON   interpreter (default: python3)
#   DESK_BRIEF_NARRATE  1: ask the local GPU model (gridz4 llama.cpp) for wording; the deterministic
#                       brief is written either way, and wording with an unsupported number is dropped.
#   DESK_BRIEF_DB_URL   SQLAlchemy URL (default postgresql+psycopg2:///griddb, peer auth)
# Exit codes: 0 written, or already written; 2 refused (the brief's clock is still ahead, e.g. a
# holiday morning); anything else: the writer failed and no brief was written.
set -uo pipefail
kind="${1:?usage: desk_brief_run.sh pre_open|post_close}"
: "${DESK_BRIEF_DIR:?set DESK_BRIEF_DIR}"
PY="${DESK_BRIEF_PYTHON:-python3}"
umask 077
extra=()
[ "${DESK_BRIEF_NARRATE:-0}" = "1" ] && extra+=(--narrate)
exec "$PY" -B -m scripts.desk_brief run --kind "$kind" --out "$DESK_BRIEF_DIR" "${extra[@]}"
