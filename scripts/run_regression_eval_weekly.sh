#!/usr/bin/env bash
# Weekly GRID chatbot regression eval + trend tracker.
# Triggered by grid-regression-eval.timer (Mondays 13:00 UTC).
#
# Runs scripts.run_regression_eval, parses the pass-rate line from its
# summary, appends a JSON record to ~/.grid-regression-trend.jsonl,
# compares against the previous run, and emits a single trend line.
# Exits 0 on equal-or-improved pass rate, 1 on regression. Eval scores
# already land in Langfuse; this layer is just for trend alerting.

set -o pipefail

REPO=/data/grid_v4/grid_repo
VENV_PY=/data/grid_v4/venv/bin/python
TREND=${HOME}/.grid-regression-trend.jsonl
ART=${HOME}/.grid-regression-trend
mkdir -p "${ART}"

cd "${REPO}"

# Load .env via python-dotenv (more robust than `set -a; source .env` —
# .env entries may contain $-references that would trip strict bash).
eval "$("${VENV_PY}" -c "
from dotenv import dotenv_values
import shlex
for k, v in dotenv_values('.env').items():
    if v is None: continue
    print(f'export {k}={shlex.quote(v)}')
")"

ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
log="${ART}/run-${ts}.log"

"${VENV_PY}" -m scripts.run_regression_eval --judge-provider openai > "${log}" 2>&1
exit_code=$?

# Parse "Pass rate: N/M" from the summary table.
pass_line=$(grep -Ei "pass[[:space:]]?rate" "${log}" | tail -1 || true)
passed=$(echo "${pass_line}" | grep -oE "[0-9]+/[0-9]+" | head -1 | cut -d/ -f1)
total=$(echo "${pass_line}" | grep -oE "[0-9]+/[0-9]+" | head -1 | cut -d/ -f2)

if [[ -z "${passed:-}" || -z "${total:-}" ]]; then
    echo "regression-eval @ ${ts}: COULD NOT PARSE pass rate (eval exit=${exit_code}); see ${log}" >&2
    exit 2
fi

rate=$(awk -v p="${passed}" -v t="${total}" 'BEGIN{printf "%.4f", (t>0)?(p/t):0}')

# Compare against previous entry's rate (if any).
prev_rate=$(tail -1 "${TREND}" 2>/dev/null | "${VENV_PY}" -c "import json,sys
try: print(json.loads(sys.stdin.read()).get('rate', ''))
except: print('')" 2>/dev/null || true)

trend="first-run"
verdict="ok"
if [[ -n "${prev_rate}" ]]; then
    cmp=$(awk -v a="${rate}" -v b="${prev_rate}" 'BEGIN{
        if (a > b) print "improved";
        else if (a < b) print "regressed";
        else print "flat";
    }')
    trend="${cmp} (${prev_rate} -> ${rate})"
    if [[ "${cmp}" == "regressed" ]]; then verdict="regression"; fi
fi

# Append the new record.
"${VENV_PY}" -c "import json
print(json.dumps({
    'ts': '${ts}',
    'passed': ${passed},
    'total': ${total},
    'rate': ${rate},
    'trend': '${trend}',
    'verdict': '${verdict}',
    'eval_exit': ${exit_code},
    'log': '${log}',
}))" >> "${TREND}"

# Single-line summary to systemd journal.
echo "regression-eval @ ${ts}: ${passed}/${total} (${rate}) — ${trend} — verdict=${verdict}"

# Exit 0 unless we regressed; exit 1 lets the systemd timer / OnFailure unit alert.
[[ "${verdict}" == "regression" ]] && exit 1 || exit 0
