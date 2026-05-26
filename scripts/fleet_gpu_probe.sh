#!/usr/bin/env bash
# Fleet GPU probe — collects each host's GPU(s) and prints a ready-to-paste
# host_profiles.json for llm/autoresearch + a human-readable summary.
#
# No GRID repo / Python needed on the remote hosts — just `nvidia-smi`.
# Run it from ONE box that can SSH to the others (e.g. grid-svr over tailnet):
#
#     ./scripts/fleet_gpu_probe.sh
#
# Customize the host list / ssh user:
#     HOSTS="grid-svr panda ocr-node koala z400" SSH_USER=grid ./scripts/fleet_gpu_probe.sh
#
# Probe only the local machine (no SSH):
#     ./scripts/fleet_gpu_probe.sh --local
#
# Paste the JSON block at the end back to the agent.

set -u

HOSTS="${HOSTS:-grid-svr panda ocr-node koala z400}"
SSH_USER="${SSH_USER:-}"
SSH_OPTS="-o ConnectTimeout=6 -o BatchMode=yes -o StrictHostKeyChecking=accept-new"

# Remote probe: prints TAG_ lines. POSIX sh, depends only on coreutils + nvidia-smi.
read -r -d '' REMOTE <<'REMOTE_EOF'
HN=$(hostname 2>/dev/null || echo unknown)
if command -v nvidia-smi >/dev/null 2>&1; then
  CSV=$(nvidia-smi --query-gpu=name,memory.total --format=csv,noheader,nounits 2>/dev/null)
else
  CSV=""
fi
COUNT=$(printf '%s\n' "$CSV" | grep -c .)
NAME=$(printf '%s\n' "$CSV" | head -1 | cut -d, -f1 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
MIB=$(printf '%s\n' "$CSV" | head -1 | cut -d, -f2 | tr -dc '0-9')
if [ -n "$MIB" ]; then GB=$(( (MIB + 512) / 1024 )); else GB=0; fi
MODELS="-"
if command -v ollama >/dev/null 2>&1; then
  M=$(ollama list 2>/dev/null | awk 'NR>1{print $1}' | paste -sd, - 2>/dev/null)
  [ -n "$M" ] && MODELS="$M"
fi
echo "TAG_HOST=${HN}"
echo "TAG_COUNT=${COUNT}"
echo "TAG_NAME=${NAME}"
echo "TAG_GB=${GB}"
echo "TAG_MODELS=${MODELS}"
REMOTE_EOF

run_probe() {  # $1 = host alias ("" => local)
  local host="$1"
  if [ -z "$host" ]; then
    bash -c "$REMOTE" 2>/dev/null
  else
    local target="$host"
    [ -n "$SSH_USER" ] && target="${SSH_USER}@${host}"
    # shellcheck disable=SC2086
    ssh $SSH_OPTS "$target" 'bash -s' 2>/dev/null <<< "$REMOTE"
  fi
}

declare -a JSON_FRAGS=()
printf '====================== FLEET GPU PROBE ======================\n'
printf '%-12s %-7s %-5s %-34s %s\n' "HOST" "VRAM" "GPUS" "GPU" "SERVED MODELS"
printf -- '-------------------------------------------------------------------------------\n'

probe_one() {  # $1 = alias for JSON key, $2 = ssh host ("" => local)
  local alias="$1" host="$2"
  local out hn count name gb models
  out="$(run_probe "$host")"
  if [ -z "$out" ]; then
    printf '%-12s %s\n' "$alias" "UNREACHABLE (ssh failed / no output)"
    JSON_FRAGS+=("  \"${alias}\": {\"error\": \"unreachable\"}")
    return
  fi
  hn=$(printf '%s\n' "$out"     | sed -n 's/^TAG_HOST=//p')
  count=$(printf '%s\n' "$out"  | sed -n 's/^TAG_COUNT=//p')
  name=$(printf '%s\n' "$out"   | sed -n 's/^TAG_NAME=//p')
  gb=$(printf '%s\n' "$out"     | sed -n 's/^TAG_GB=//p')
  models=$(printf '%s\n' "$out" | sed -n 's/^TAG_MODELS=//p')
  : "${count:=0}"; : "${gb:=0}"; : "${name:=}"; : "${models:=-}"
  if [ "$count" = "0" ] || [ -z "$name" ]; then
    printf '%-12s %-7s %-5s %-34s %s\n' "$alias" "-" "0" "CPU-only / no nvidia-smi" "$models"
    JSON_FRAGS+=("  \"${alias}\": {\"vram_gb\": 0, \"gpus\": 0, \"gpu_name\": \"CPU-only\"}  // ${hn}")
  else
    printf '%-12s %-7s %-5s %-34s %s\n' "$alias" "${gb}GB" "$count" "$name" "$models"
    JSON_FRAGS+=("  \"${alias}\": {\"vram_gb\": ${gb}, \"gpus\": ${count}, \"gpu_name\": \"${name}\"}")
  fi
}

if [ "${1:-}" = "--local" ]; then
  probe_one "$(hostname)" ""
else
  for h in $HOSTS; do
    probe_one "$h" "$h"
  done
fi

printf -- '-------------------------------------------------------------------------------\n\n'
printf 'PASTE THIS BACK (host_profiles.json) — strip the // comments:\n\n'
printf '{\n'
n=${#JSON_FRAGS[@]}
for i in "${!JSON_FRAGS[@]}"; do
  if [ "$i" -lt $((n - 1)) ]; then
    printf '%s,\n' "${JSON_FRAGS[$i]}"
  else
    printf '%s\n' "${JSON_FRAGS[$i]}"
  fi
done
printf '}\n'
