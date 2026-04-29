#!/usr/bin/env bash
#
# install_pre_commit_hook.sh -- Install the GRID pre-commit hook.
#
# Creates .git/hooks/pre-commit as a tiny bash wrapper that execs
# scripts/git_hooks/pre_commit_hook.py with the repo root as cwd.
#
# Idempotent: re-running overwrites the wrapper safely.
#
# Usage:
#   bash scripts/install_pre_commit_hook.sh
#
# Uninstall:
#   rm .git/hooks/pre-commit

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
HOOK_PY="${REPO_ROOT}/scripts/git_hooks/pre_commit_hook.py"
GIT_DIR="${REPO_ROOT}/.git"
HOOK_DEST="${GIT_DIR}/hooks/pre-commit"

if [[ ! -d "${GIT_DIR}" ]]; then
    echo "error: ${REPO_ROOT} is not a git repository (no .git directory)" >&2
    exit 1
fi

if [[ ! -f "${HOOK_PY}" ]]; then
    echo "error: hook script not found at ${HOOK_PY}" >&2
    exit 1
fi

mkdir -p "${GIT_DIR}/hooks"

# Write the wrapper. Use a heredoc so the quoting is obvious.
cat > "${HOOK_DEST}" <<'WRAPPER'
#!/usr/bin/env bash
# GRID pre-commit hook wrapper (installed by scripts/install_pre_commit_hook.sh).
# Delegates to scripts/git_hooks/pre_commit_hook.py so the real logic lives in
# the tracked repo, not in .git/hooks/.
set -euo pipefail
REPO_ROOT="$(git rev-parse --show-toplevel)"
exec python3 "${REPO_ROOT}/scripts/git_hooks/pre_commit_hook.py" "$@"
WRAPPER

chmod +x "${HOOK_DEST}"
chmod +x "${HOOK_PY}"

echo "pre-commit hook installed"
echo "  wrapper: ${HOOK_DEST}"
echo "  logic:   ${HOOK_PY}"
echo "to uninstall: rm ${HOOK_DEST}"
