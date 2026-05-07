#!/usr/bin/env bash
#
# Bake + package + (optionally) deploy the GoDaddy static mirror of grid.stepdad.finance.
#
# Usage:
#   scripts/offline_bake/package_godaddy.sh             # bake + zip only
#   scripts/offline_bake/package_godaddy.sh --upload    # bake + zip + rsync to godaddy:public_html/stepdad.fi/
#   scripts/offline_bake/package_godaddy.sh --upload-only  # skip bake, just push existing dist/
#
# Env overrides:
#   GRID_GODADDY_SSH       (default: godaddy)        ssh alias from ~/.ssh/config
#   GRID_GODADDY_DOC_ROOT  (default: public_html/stepdad.fi)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"
DIST="$REPO/dist/godaddy"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
ZIP_DIR="$REPO/output/deploy"
ZIP="$ZIP_DIR/grid-godaddy-cpanel-$TS.zip"
SSH_HOST="${GRID_GODADDY_SSH:-godaddy}"
REMOTE_DIR="${GRID_GODADDY_DOC_ROOT:-public_html/stepdadfi.com}"

UPLOAD=0
SKIP_BAKE=0
case "${1:-}" in
  --upload)        UPLOAD=1 ;;
  --upload-only)   UPLOAD=1; SKIP_BAKE=1 ;;
  "" )             ;;
  -h|--help)       sed -n '1,15p' "$0"; exit 0 ;;
  *) echo "unknown flag: $1" >&2; exit 2 ;;
esac

if [[ $SKIP_BAKE -eq 0 ]]; then
  echo "[1/4] Baking static mirror from live grid.stepdad.finance"
  python3 "$SCRIPT_DIR/bake.py"
else
  echo "[1/4] Skipping bake (--upload-only)"
  [[ -d "$DIST" ]] || { echo "no $DIST to upload — run without --upload-only first" >&2; exit 1; }
fi

echo "[2/4] Packaging zip"
mkdir -p "$ZIP_DIR"
( cd "$DIST" && zip -qr "$ZIP" . )
echo "       -> $ZIP ($(du -h "$ZIP" | cut -f1))"

if [[ $UPLOAD -eq 1 ]]; then
  echo "[3/4] Uploading via rsync to $SSH_HOST:~/$REMOTE_DIR"
  ssh "$SSH_HOST" "mkdir -p ~/$REMOTE_DIR"
  # --delete-after so we leave the live tree intact until rsync confirms the new tree.
  rsync -az --delete-after --exclude='cgi-bin' \
    -e "ssh -o ConnectTimeout=15" \
    "$DIST/" "$SSH_HOST:$REMOTE_DIR/"
  echo "[4/4] Verify"
  ssh "$SSH_HOST" "echo '--- listing ---'; ls -la ~/$REMOTE_DIR | head -20; echo '--- total size ---'; du -sh ~/$REMOTE_DIR"
  echo
  echo "Done. Smoke test:"
  echo "    curl -I https://stepdad.fi/"
  echo "    curl -s https://stepdad.fi/api/health.json"
else
  echo "[3/4] Upload skipped (run with --upload to push to GoDaddy)"
fi
