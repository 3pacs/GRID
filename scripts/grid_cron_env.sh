# shellcheck shell=bash
# ============================================================
# Shared setup for GRID cron entry points. Source it; do not execute it.
#
# Cron jobs run from the deployed release tree
# (/data/grid_v4/grid_release -> grid_release.releases/<sha>). A release tree
# has no .env, and config.py only auto-loads the .env that sits next to it, so
# every cron entry point has to load the GRID env file itself. The default is
# /etc/grid/grid.env, where the shared env file lives since 2026-11-17. The old
# checkout path /home/grid/grid_v4/grid_repo/.env is now a symlink to it (the
# release systemd units still name that path in EnvironmentFile=) and stays as
# a fallback for hosts that have not moved the file. GRID_ENV_FILE overrides
# both.
#
# Functions:
#   grid_cron_load_env   source the env file (fail closed), pin PYTHONPATH
#   grid_cron_python     print the interpreter to use (GRID_PYTHON override)
# ============================================================

GRID_ENV_FILE_DEFAULT="/etc/grid/grid.env"
GRID_ENV_FILE_LEGACY="/home/grid/grid_v4/grid_repo/.env"

# Print the env file to load: GRID_ENV_FILE when set, else the default when it
# is readable, else the legacy checkout path (which may itself be missing; the
# caller fails closed on that).
grid_cron_env_file() {
    if [[ -n "${GRID_ENV_FILE:-}" ]]; then
        echo "$GRID_ENV_FILE"
    elif [[ -r "$GRID_ENV_FILE_DEFAULT" ]]; then
        echo "$GRID_ENV_FILE_DEFAULT"
    else
        echo "$GRID_ENV_FILE_LEGACY"
    fi
}

# Load the GRID env file into the environment, then pin PYTHONPATH to
# GRID_ROOT so a job can never import modules from a different tree (for
# example the old checkout). Returns 78 (EX_CONFIG) when the env file is
# missing or unreadable: running without it would fail later in a less
# obvious way (empty DB_PASSWORD, missing GRID_JWT_SECRET).
grid_cron_load_env() {
    local env_file
    env_file="$(grid_cron_env_file)"
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
