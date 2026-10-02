# shellcheck shell=bash
# ============================================================
# Shared setup for GRID cron entry points. Source it; do not execute it.
#
# Cron jobs run from the deployed release tree
# (/data/grid_v4/grid_release -> grid_release.releases/<sha>). A release tree
# has no .env, and config.py only auto-loads the .env that sits next to it, so
# every cron entry point has to load the GRID env file itself. The default is
# the file the release systemd units already name in EnvironmentFile=
# (grid-api, grid-hermes, grid-scheduler, the godview writers). Moving that
# file is a separate, owner-gated step (OPS-W1 .env plan, after OPS-SEC's
# rotation runbook). Until then, point GRID_ENV_FILE somewhere else to override.
#
# Functions:
#   grid_cron_load_env   source the env file (fail closed), pin PYTHONPATH
#   grid_cron_python     print the interpreter to use (GRID_PYTHON override)
# ============================================================

GRID_ENV_FILE_DEFAULT="/home/grid/grid_v4/grid_repo/.env"

# Load the GRID env file into the environment, then pin PYTHONPATH to
# GRID_ROOT so a job can never import modules from a different tree (for
# example the old checkout). Returns 78 (EX_CONFIG) when the env file is
# missing or unreadable: running without it would fail later in a less
# obvious way (empty DB_PASSWORD, missing GRID_JWT_SECRET).
grid_cron_load_env() {
    local env_file="${GRID_ENV_FILE:-$GRID_ENV_FILE_DEFAULT}"
    if [[ -z "${GRID_ROOT:-}" ]]; then
        echo "[$(date -Is)] ERROR: GRID_ROOT is not set before grid_cron_load_env" >&2
        return 78
    fi
    if [[ ! -f "$env_file" || ! -r "$env_file" ]]; then
        echo "[$(date -Is)] ERROR: GRID env file missing or unreadable: $env_file (set GRID_ENV_FILE)" >&2
        return 78
    fi
    local had_nounset=0
    [[ $- == *u* ]] && had_nounset=1
    set +u
    set -a
    # shellcheck disable=SC1090
    source "$env_file"
    set +a
    (( had_nounset )) && set -u
    export GRID_ENV_FILE="$env_file"
    export PYTHONPATH="$GRID_ROOT"
    return 0
}

# The release systemd units run /usr/bin/python3 with ~/.local site-packages,
# not the old ~/grid_v4/venv, so the release-path cron jobs do the same.
grid_cron_python() {
    echo "${GRID_PYTHON:-/usr/bin/python3}"
}
