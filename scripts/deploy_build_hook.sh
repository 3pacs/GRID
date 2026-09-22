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

cd "$CANDIDATE_DIR"
echo "before: $(git log --oneline -1 2>/dev/null || echo '<no commit yet>')"
git fetch "$SRC_URL" main
git reset --hard FETCH_HEAD
echo "after:  $(git log --oneline -1)"
git merge-base --is-ancestor "$EXPECTED_SHA" HEAD || {
  echo "::error::candidate release tree is not at the pushed commit $EXPECTED_SHA"
  exit 1
}

pip install -r requirements.txt

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
