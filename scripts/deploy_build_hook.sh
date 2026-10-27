#!/usr/bin/env bash
# SOURCE PROPOSAL: retired shared-target 4-argument install hook is refused.
# New contract is an isolated builder, never the GRID runtime host/shared venv.
# The called producer retains NOT_ADOPTED/78 until separately qualified.
set -euo pipefail
if [ "$#" -ne 3 ] || [ "$1" != api-isolated ] || [ "$3" != --execute ]; then
  echo 'usage: deploy_build_hook.sh api-isolated <exact-commit> --execute' >&2
  echo 'legacy shared-target install is unsupported by this proposal' >&2
  exit 64
fi
case "$2" in
  *[!0-9a-f]*|'') echo 'invalid exact commit' >&2; exit 64 ;;
esac
[ "${#2}" -eq 40 ] || exit 64
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec python3 -I -S -B "$SCRIPT_DIR/deploy_api_bundle.py" --expected-commit "$2" --execute
