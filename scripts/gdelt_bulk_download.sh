#!/usr/bin/env bash
# GDELT Bulk Data Downloader
# Downloads the full GDELT v1 + v2 archives to /data/gdelt/
# Resumable — tracks completed files. Safe to re-run.
#
# Usage: nohup bash scripts/gdelt_bulk_download.sh &> /data/gdelt/download.log &
#
# Priority order: V2 English → V1 Events → V2 Translation → V1 GKG

set -euo pipefail

BASE="/data/gdelt"
PARALLEL=8          # concurrent downloads
RETRY=3             # retries per file
LOG="$BASE/download.log"
STATE_DIR="$BASE/.state"

# Directories
V2_EN="$BASE/v2/english"
V2_TR="$BASE/v2/translation"
V1_EV="$BASE/v1/events"
V1_GKG="$BASE/v1/gkg"

mkdir -p "$V2_EN" "$V2_TR" "$V1_EV" "$V1_GKG" "$STATE_DIR"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# ── Phase 1: V2 English ────────────────────────────────────────────
download_v2() {
    local label="$1"
    local masterlist_url="$2"
    local dest_dir="$3"
    local state_file="$STATE_DIR/${label}.done"
    local masterlist="$STATE_DIR/${label}_masterfilelist.txt"

    log "=== Phase: $label ==="

    # Download master file list if not cached (or older than 6h)
    if [[ ! -f "$masterlist" ]] || [[ $(find "$masterlist" -mmin +360 2>/dev/null) ]]; then
        log "Downloading master file list for $label..."
        curl -sL "$masterlist_url" -o "$masterlist.tmp" && mv "$masterlist.tmp" "$masterlist"
        log "Master file list: $(wc -l < "$masterlist") files"
    else
        log "Using cached master file list: $(wc -l < "$masterlist") files"
    fi

    # Create state file if missing
    touch "$state_file"

    # Extract URLs, filter already-downloaded, download in parallel
    local total=$(wc -l < "$masterlist")
    local done=$(wc -l < "$state_file")
    log "$label: $done/$total already downloaded"

    # Build download list: lines not in done file
    awk '{print $3}' "$masterlist" | while IFS= read -r url; do
        local fname=$(basename "$url")
        if ! grep -qxF "$fname" "$state_file"; then
            echo "$url"
        fi
    done > "$STATE_DIR/${label}_todo.txt"

    local remaining=$(wc -l < "$STATE_DIR/${label}_todo.txt")
    log "$label: $remaining files to download"

    if [[ "$remaining" -eq 0 ]]; then
        log "$label: COMPLETE"
        return
    fi

    # Download with xargs for parallelism
    cat "$STATE_DIR/${label}_todo.txt" | xargs -P "$PARALLEL" -I {} bash -c '
        url="{}"
        fname=$(basename "$url")
        dest="'"$dest_dir"'/$fname"
        state="'"$state_file"'"
        for attempt in $(seq 1 '"$RETRY"'); do
            if curl -sL --retry 3 --connect-timeout 30 --max-time 300 -o "$dest.tmp" "$url" 2>/dev/null; then
                mv "$dest.tmp" "$dest"
                echo "$fname" >> "$state"
                break
            fi
            sleep $((attempt * 2))
        done
    '

    local final_done=$(wc -l < "$state_file")
    log "$label: $final_done/$total complete"
}

# ── Phase 2: V1 Events ────────────────────────────────────────────
download_v1_events() {
    local label="v1_events"
    local state_file="$STATE_DIR/${label}.done"
    local filelist="$STATE_DIR/${label}_files.txt"

    log "=== Phase: V1 Events ==="

    # Get file list from md5sums (format: "md5  filename")
    if [[ ! -f "$filelist" ]] || [[ $(find "$filelist" -mmin +360 2>/dev/null) ]]; then
        log "Fetching V1 Events file list..."
        curl -sL "http://data.gdeltproject.org/events/md5sums" | awk '{print $2}' | grep -E '\.(zip|ZIP)$' > "$filelist.tmp"
        mv "$filelist.tmp" "$filelist"
        log "V1 Events: $(wc -l < "$filelist") files"
    fi

    touch "$state_file"

    cat "$filelist" | while IFS= read -r fname; do
        if ! grep -qxF "$fname" "$state_file"; then
            echo "http://data.gdeltproject.org/events/$fname"
        fi
    done > "$STATE_DIR/${label}_todo.txt"

    local remaining=$(wc -l < "$STATE_DIR/${label}_todo.txt")
    log "V1 Events: $remaining files to download"

    if [[ "$remaining" -eq 0 ]]; then
        log "V1 Events: COMPLETE"
        return
    fi

    cat "$STATE_DIR/${label}_todo.txt" | xargs -P "$PARALLEL" -I {} bash -c '
        url="{}"
        fname=$(basename "$url")
        dest="'"$V1_EV"'/$fname"
        state="'"$state_file"'"
        for attempt in $(seq 1 '"$RETRY"'); do
            if curl -sL --retry 3 --connect-timeout 30 --max-time 300 -o "$dest.tmp" "$url" 2>/dev/null; then
                mv "$dest.tmp" "$dest"
                echo "$fname" >> "$state"
                break
            fi
            sleep $((attempt * 2))
        done
    '

    log "V1 Events: $(wc -l < "$state_file")/$(wc -l < "$filelist") complete"
}

# ── Phase 3: V1 GKG ──────────────────────────────────────────────
download_v1_gkg() {
    local label="v1_gkg"
    local state_file="$STATE_DIR/${label}.done"
    local filelist="$STATE_DIR/${label}_files.txt"

    log "=== Phase: V1 GKG ==="

    if [[ ! -f "$filelist" ]] || [[ $(find "$filelist" -mmin +360 2>/dev/null) ]]; then
        log "Fetching V1 GKG file list..."
        curl -sL "http://data.gdeltproject.org/gkg/md5sums" | awk '{print $2}' | grep -E '\.(zip|ZIP|csv\.zip)$' > "$filelist.tmp"
        mv "$filelist.tmp" "$filelist"
        log "V1 GKG: $(wc -l < "$filelist") files"
    fi

    touch "$state_file"

    cat "$filelist" | while IFS= read -r fname; do
        if ! grep -qxF "$fname" "$state_file"; then
            echo "http://data.gdeltproject.org/gkg/$fname"
        fi
    done > "$STATE_DIR/${label}_todo.txt"

    local remaining=$(wc -l < "$STATE_DIR/${label}_todo.txt")
    log "V1 GKG: $remaining files to download"

    if [[ "$remaining" -eq 0 ]]; then
        log "V1 GKG: COMPLETE"
        return
    fi

    cat "$STATE_DIR/${label}_todo.txt" | xargs -P "$PARALLEL" -I {} bash -c '
        url="{}"
        fname=$(basename "$url")
        dest="'"$V1_GKG"'/$fname"
        state="'"$state_file"'"
        for attempt in $(seq 1 '"$RETRY"'); do
            if curl -sL --retry 3 --connect-timeout 30 --max-time 300 -o "$dest.tmp" "$url" 2>/dev/null; then
                mv "$dest.tmp" "$dest"
                echo "$fname" >> "$state"
                break
            fi
            sleep $((attempt * 2))
        done
    '

    log "V1 GKG: $(wc -l < "$state_file")/$(wc -l < "$filelist") complete"
}

# ── Run all phases ────────────────────────────────────────────────
log "GDELT Bulk Download starting"
log "Target: $BASE ($(df -h /data | tail -1 | awk '{print $4}') free)"

# Phase 1: V2 English (~3-4TB compressed, richest current data)
download_v2 "v2_english" \
    "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt" \
    "$V2_EN"

# Phase 2: V1 Events (~50-80GB compressed, historical depth to 1979)
download_v1_events

# Phase 3: V2 Translation (~3-4TB compressed, 65 languages)
download_v2 "v2_translation" \
    "http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt" \
    "$V2_TR"

# Phase 4: V1 GKG (~150-300GB compressed)
download_v1_gkg

log "=== ALL PHASES COMPLETE ==="
log "Final disk usage:"
du -sh "$BASE"/*/ 2>/dev/null
