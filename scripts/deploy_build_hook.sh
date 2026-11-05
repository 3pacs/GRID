#!/usr/bin/env bash
# The actual fetch/install/build/migrate logic for a GRID deploy, extracted
# from deploy.yml's "Build release tree" step so it can run against an
# isolated candidate directory under scripts/deploy_release_swap.sh instead
# of directly on the live release path. Behavior is otherwise unchanged from
# before this fix -- same commands, same env sourcing, same alembic summary
# logging -- only the working directory (a candidate, not the live path) and
# the fact that a non-zero exit here now means "the live path was never
# touched" (see deploy_release_swap.sh) rather than "the live path was left
# half-updated," are new.
#
# 2026-09-22: the fetch/reset/identity-check step now delegates to
# scripts/deploy_pin_candidate_sha.sh, which fetches the EXACT expected
# commit rather than the floating `main` ref -- see that script's own header
# for why (closes a real, if rare, race with deploy_verify_release_tree.sh's
# exact-SHA-match requirement). No other change to this file.
#
# Usage: deploy_build_hook.sh <candidate_dir> <src_url> <expected_sha> <skip_migrations: true|false>
#
# Exits non-zero on any failure (fetch, install, PWA build, or migration).
# Must not be run with a trailing `|| true` or similar anywhere in its
# caller -- deploy_release_swap.sh relies on a real, unmasked exit code.

set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo "usage: $0 <candidate_dir> <src_url> <expected_sha> <skip_migrations>" >&2
  exit 2
fi

CANDIDATE_DIR="$1"
SRC_URL="$2"
EXPECTED_SHA="$3"
SKIP_MIGRATIONS="$4"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# Invoked via `bash`, not `./script`: this repo has core.filemode=false, so a
# fresh checkout never carries an executable bit regardless of what was
# committed -- relying on it directly fails with "Permission denied" (found
# running this exact change in CI, on the self-hosted runner's fresh
# checkout).
bash "${SCRIPT_DIR}/deploy_pin_candidate_sha.sh" "$CANDIDATE_DIR" "$SRC_URL" "$EXPECTED_SHA"

cd "$CANDIDATE_DIR"

pip install -r requirements.txt

# 2026-11-04: the GRID units run the Python 3.14 venv (deploy/python314/cutover.sh writes a py314.conf drop-in per
# unit), so the same requirements go into it as well, under the 3.14 lock that CI's "Python 3.14" job tests. The
# 3.10 install above stays current so `cutover.sh rollback` keeps working. Like that install, this updates an
# environment the running services use. Skipped when the venv is absent (hosts that never ran build_venv.sh).
GRID_VENV_314="${GRID_VENV_314:-/data/grid_v4/grid_venv-314}"
if [ -x "${GRID_VENV_314}/bin/python" ]; then
  "${GRID_VENV_314}/bin/pip" install -r requirements.txt \
    -r deploy/python314/requirements-runtime.txt -c deploy/python314/constraints-3.14.txt
  "${GRID_VENV_314}/bin/pip" check
fi

# npm/node version pinning is handled by the caller (deploy.yml's "Set up
# Node" step, run once per job before this hook is invoked) via $PATH -- this
# hook does not repeat that setup.
cd pwa && npm ci && npm run build && cd ..

if [ "$SKIP_MIGRATIONS" = "true" ]; then
  echo "::warning::alembic upgrade head skipped via workflow_dispatch input skip_migrations=true"
  exit 0
fi

for f in \
  "${CANDIDATE_DIR}/.env" \
  /home/grid/grid_v4/grid_repo/.env \
  /home/grid/grid_v4/grid_repo/grid/.env
do
  if [ -f "$f" ]; then
    set -a; . "$f"; set +a
    echo "(env sourced from $f)"
    break
  fi
done

{
  echo "## Alembic"
  echo '### current (before upgrade)'
  echo '```'
  python3 -m alembic current
  echo '```'
  echo '### heads'
  echo '```'
  python3 -m alembic heads
  echo '```'
} | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"

python3 -m alembic upgrade head

{
  echo '### current (after upgrade)'
  echo '```'
  python3 -m alembic current
  echo '```'
} | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
