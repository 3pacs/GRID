#!/bin/bash
# AstroGrid production smoke test.
#
# Validates the AstroGrid oracle loop on the standalone AstroGrid app:
#   - internal AstroGrid health/readiness are reachable
#   - score/review/backtest timestamps are fresh and mutually coherent
#   - public guru ask returns a live answer
#   - public guru ask persists to the ledger
#   - latest feed includes the new prediction
#   - public review/latest and weights/current are coherent
#
# Exit codes:
#   0  success
#   1  health/readiness failed
#   2  public guru ask failed
#   3  persistence/feed contract failed
#   4  public review/weights contract failed
#   5  freshness/coherence failed

set -euo pipefail

if [[ -n "${GRID_ROOT:-}" ]]; then
  REPO="$GRID_ROOT"
elif [[ -d /data/grid_v4/astrogrid_dedup ]]; then
  REPO=/data/grid_v4/astrogrid_dedup
else
  REPO="$(cd "$(dirname "$0")/.." && pwd)"
fi

VENV=""
for candidate in ~/grid_v4/venv /home/grid/grid_v4/venv /data/grid_v4/venv "$REPO/.venv"; do
  if [[ -x "$candidate/bin/python3" ]]; then
    VENV="$candidate"
    break
  fi
done
if [[ -z "$VENV" ]]; then
  echo "FAIL: cannot find python environment for AstroGrid smoke" >&2
  exit 1
fi

cd "$REPO"
# shellcheck disable=SC1091
source "$VENV/bin/activate"

python3 - <<'PY'
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlalchemy import text

from astrogrid_api.dependencies import get_astrogrid_store
from astrogrid_api.main import app


EXPECTED_BACKTEST_VARIANTS = tuple(
    item.strip()
    for item in os.getenv(
        "ASTROGRID_EXPECTED_BACKTEST_VARIANTS",
        "grid_only,grid_plus_mystical,mystical_only",
    ).split(",")
    if item.strip()
)
MAX_FRESHNESS_AGE = timedelta(
    minutes=max(1, int(os.getenv("ASTROGRID_FRESHNESS_MAX_AGE_MINUTES", "180")))
)
MAX_PIPELINE_SKEW = timedelta(
    minutes=max(1, int(os.getenv("ASTROGRID_PIPELINE_MAX_SKEW_MINUTES", "90")))
)
MAX_CLOCK_SKEW = timedelta(minutes=5)


def _fail(code: int, message: str, details: dict | None = None) -> None:
    print(f"FAIL: {message}", file=sys.stderr)
    if details is not None:
        print(json.dumps(details, indent=2, sort_keys=True, default=str), file=sys.stderr)
    raise SystemExit(code)


def _parse_ts(value: object, label: str) -> datetime:
    if not value:
        _fail(5, f"{label} missing")
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            _fail(5, f"{label} invalid timestamp", {"value": value, "error": str(exc)})
    else:
        _fail(5, f"{label} invalid timestamp type", {"type": type(value).__name__, "value": value})
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _age_minutes(now: datetime, dt: datetime) -> float:
    return round((now - dt).total_seconds() / 60.0, 1)


def _ensure_recent(label: str, value: object, now: datetime) -> datetime:
    dt = _parse_ts(value, label)
    age = now - dt
    if age < -MAX_CLOCK_SKEW:
        _fail(
            5,
            f"{label} is unexpectedly in the future",
            {"label": label, "now": now.isoformat(), "value": dt.isoformat()},
        )
    if age > MAX_FRESHNESS_AGE:
        _fail(
            5,
            f"{label} is stale",
            {
                "label": label,
                "age_minutes": _age_minutes(now, dt),
                "max_age_minutes": round(MAX_FRESHNESS_AGE.total_seconds() / 60.0, 1),
                "value": dt.isoformat(),
            },
        )
    return dt


def _require_mapping(value: object, label: str, *, code: int = 5) -> dict:
    if not isinstance(value, dict):
        _fail(code, f"{label} missing or invalid", {"type": type(value).__name__})
    return value


def _normalize_score_summary(summary: dict) -> dict:
    return {
        "total_predictions": int(summary.get("total_predictions") or 0),
        "scored": int(summary.get("scored") or 0),
        "pending": int(summary.get("pending") or 0),
        "hits": int(summary.get("hits") or 0),
        "misses": int(summary.get("misses") or 0),
        "partials": int(summary.get("partials") or 0),
        "invalidated": int(summary.get("invalidated") or 0),
        "expired": int(summary.get("expired") or 0),
        "accuracy": float(summary.get("accuracy") or 0.0),
        "avg_realized_return": float(summary.get("avg_realized_return") or 0.0),
        "avg_alpha_vs_benchmark": float(summary.get("avg_alpha_vs_benchmark") or 0.0),
        "avg_mfe": float(summary.get("avg_mfe") or 0.0),
        "avg_mae": float(summary.get("avg_mae") or 0.0),
    }


def _score_summary_matches_readyz(ready_summary: dict, store_summary: dict) -> bool:
    ready = _normalize_score_summary(ready_summary)
    store = _normalize_score_summary(store_summary)
    mutable_deltas = {
        "total_predictions": store["total_predictions"] - ready["total_predictions"],
        "pending": store["pending"] - ready["pending"],
    }
    for key in ("scored", "hits", "misses", "partials", "invalidated", "expired"):
        if store[key] != ready[key]:
            return False
    for key in ("accuracy", "avg_realized_return", "avg_alpha_vs_benchmark", "avg_mfe", "avg_mae"):
        if abs(store[key] - ready[key]) > 1e-9:
            return False
    return mutable_deltas["total_predictions"] in {0, 1} and mutable_deltas["pending"] in {0, 1} and mutable_deltas["total_predictions"] == mutable_deltas["pending"]


client = TestClient(app)
store = get_astrogrid_store()
now = datetime.now(timezone.utc)
question = f"What crypto should I buy right now? smoke {datetime.now(timezone.utc).isoformat()}"

health = client.get("/healthz")
ready = client.get("/readyz")
if health.status_code != 200 or ready.status_code != 200:
    print("FAIL: health/readiness contract failed", file=sys.stderr)
    sys.exit(1)
ready_payload = ready.json()
ready_review_summary = _require_mapping(ready_payload.get("latest_review_summary") or {}, "readyz latest_review_summary")
ready_weight_proposal = _require_mapping(
    ready_payload.get("latest_weight_proposal") or {},
    "readyz latest_weight_proposal",
)
ready_score_summary = _require_mapping(
    ready_payload.get("latest_scoring_summary") or {},
    "readyz latest_scoring_summary",
)
ready_review_dt = _ensure_recent(
    "readyz.latest_successful_review_at",
    ready_payload.get("latest_successful_review_at"),
    now,
)
if int(ready_score_summary.get("total_predictions") or 0) <= 0 or int(ready_score_summary.get("scored") or 0) <= 0:
    _fail(5, "readyz scoring summary is empty", {"latest_scoring_summary": ready_score_summary})
if int(ready_score_summary.get("scored") or 0) > int(ready_score_summary.get("total_predictions") or 0):
    _fail(5, "readyz scoring summary is incoherent", {"latest_scoring_summary": ready_score_summary})

overview = client.get("/api/v1/astrogrid/overview")
snapshot = client.get("/api/v1/astrogrid/snapshot")
if overview.status_code != 200 or snapshot.status_code != 200:
    print("FAIL: public overview/snapshot failed", file=sys.stderr)
    sys.exit(1)

guru = client.post("/api/v1/astrogrid/guru/ask", json={"question": question})
if guru.status_code != 200:
    print("FAIL: public guru ask failed", file=sys.stderr)
    sys.exit(2)
guru_payload = guru.json()
prediction = guru_payload.get("prediction") or {}
if guru_payload.get("persistence_status") != "persisted" or not prediction.get("prediction_id"):
    print("FAIL: guru ask did not persist", file=sys.stderr)
    print(json.dumps(guru_payload, indent=2), file=sys.stderr)
    sys.exit(3)

latest = client.get("/api/v1/astrogrid/predictions/latest?limit=12")
latest_payload = latest.json()
prediction_ids = {item.get("prediction_id") for item in latest_payload.get("predictions", [])}
if prediction.get("prediction_id") not in prediction_ids:
    print("FAIL: latest feed missing persisted guru prediction", file=sys.stderr)
    sys.exit(3)

review = client.get("/api/v1/astrogrid/review/latest")
weights = client.get("/api/v1/astrogrid/weights/current")
if review.status_code != 200 or weights.status_code != 200:
    print("FAIL: public review/weights endpoints failed", file=sys.stderr)
    sys.exit(4)
review_payload = review.json()
weights_payload = weights.json()
if review_payload.get("error"):
    _fail(5, "latest review payload returned an error", review_payload)
if "best_variant_by_group" not in review_payload or "best_variant_by_group" not in weights_payload:
    print("FAIL: missing best_variant_by_group in review/weights", file=sys.stderr)
    sys.exit(4)
if "group_conditionals" not in weights_payload:
    print("FAIL: missing group_conditionals in weights payload", file=sys.stderr)
    sys.exit(4)

review_contract = _require_mapping(review_payload.get("review"), "review payload", code=4)
review_dt = _ensure_recent("review.created_at", review_payload.get("created_at"), now)
if abs((ready_review_dt - review_dt).total_seconds()) > MAX_CLOCK_SKEW.total_seconds():
    _fail(
        5,
        "readyz/latest review timestamps diverged",
        {
            "readyz_latest_successful_review_at": ready_review_dt.isoformat(),
            "review_created_at": review_dt.isoformat(),
        },
    )
if ready_review_summary != review_contract:
    _fail(
        5,
        "readyz review summary does not match /review/latest",
        {
            "readyz_latest_review_summary": ready_review_summary,
            "review_payload_review": review_contract,
        },
    )

review_best_variant_by_group = dict(review_payload.get("best_variant_by_group") or {})
weights_best_variant_by_group = dict(weights_payload.get("best_variant_by_group") or {})
review_group_conditionals = list(review_contract.get("group_conditionals") or [])
weights_group_conditionals = list(weights_payload.get("group_conditionals") or [])
if review_best_variant_by_group != weights_best_variant_by_group:
    _fail(
        5,
        "review/latest and weights/current disagree on best_variant_by_group",
        {
            "review_best_variant_by_group": review_best_variant_by_group,
            "weights_best_variant_by_group": weights_best_variant_by_group,
        },
    )
if review_group_conditionals != weights_group_conditionals:
    _fail(
        5,
        "review/latest and weights/current disagree on group_conditionals",
        {
            "review_group_conditionals": review_group_conditionals,
            "weights_group_conditionals": weights_group_conditionals,
        },
    )

if int(review_payload.get("based_on_prediction_count") or 0) <= 0:
    _fail(5, "latest review is not based on any scored predictions", review_payload)
if not str(review_contract.get("reasoning_summary") or "").strip():
    _fail(5, "latest review is missing reasoning_summary", review_payload)
proposal = _require_mapping(review_payload.get("proposal") or {}, "review proposal")
if not proposal.get("weight_proposal_id"):
    _fail(5, "latest review is missing weight proposal id", review_payload)
if ready_weight_proposal.get("weight_proposal_id") != proposal.get("weight_proposal_id"):
    _fail(
        5,
        "readyz weight proposal does not match /review/latest",
        {
            "readyz_latest_weight_proposal": ready_weight_proposal,
            "review_proposal": proposal,
        },
    )

review_variants = list((review_payload.get("based_on_backtest_window") or {}).get("latest_by_variant") or [])
missing_review_variants = [variant for variant in EXPECTED_BACKTEST_VARIANTS if variant not in review_variants]
if missing_review_variants:
    _fail(
        5,
        "latest review is not based on all expected backtest variants",
        {
            "expected_variants": EXPECTED_BACKTEST_VARIANTS,
            "review_based_on_backtest_window": review_variants,
        },
    )

scoreboard = _require_mapping(store.build_prediction_scoreboard(), "store scoreboard")
scoreboard_overall = _require_mapping(scoreboard.get("overall") or {}, "store scoreboard overall")
if not _score_summary_matches_readyz(ready_score_summary, scoreboard_overall):
    _fail(
        5,
        "readyz scoring summary does not match store scoreboard within smoke-write tolerance",
        {
            "readyz_latest_scoring_summary": ready_score_summary,
            "store_scoreboard_overall": scoreboard_overall,
        },
    )
try:
    with store.engine.connect() as conn:
        latest_score_at = conn.execute(text(f"SELECT MAX(scored_at) FROM {store.schema}.prediction_score")).scalar()
        mature_pending_count = int(
            conn.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM {store.schema}.prediction_run pr
                    LEFT JOIN {store.schema}.prediction_score ps ON ps.prediction_run_id = pr.id
                    WHERE ps.id IS NULL
                      AND pr.scoring_class = 'liquid_market'
                      AND (
                          pr.as_of_ts::date
                          + CASE
                                WHEN pr.horizon_label = 'macro' THEN 30
                                ELSE 7
                            END
                      ) <= CURRENT_DATE
                    """
                )
            ).scalar()
            or 0
        )
except Exception as exc:
    _fail(5, "failed to read latest prediction score freshness state", {"error": str(exc)})

score_dt = _parse_ts(latest_score_at, "prediction_score.max(scored_at)") if latest_score_at else None
if mature_pending_count > 0:
    score_dt = _ensure_recent("prediction_score.max(scored_at)", latest_score_at, now)

backtest_summary = _require_mapping(
    store.get_backtest_summary(limit=max(12, len(EXPECTED_BACKTEST_VARIANTS) * 4)),
    "backtest summary",
)
latest_by_variant = _require_mapping(
    backtest_summary.get("latest_by_variant") or {},
    "backtest latest_by_variant",
)
missing_backtest_variants = [variant for variant in EXPECTED_BACKTEST_VARIANTS if variant not in latest_by_variant]
if missing_backtest_variants:
    _fail(
        5,
        "backtest summary missing expected variants",
        {
            "expected_variants": EXPECTED_BACKTEST_VARIANTS,
            "latest_by_variant_keys": sorted(latest_by_variant.keys()),
        },
    )
if set(review_variants) != set(latest_by_variant.keys()):
    _fail(
        5,
        "review-backed variants do not match latest backtest summary",
        {
            "review_based_on_backtest_window": review_variants,
            "latest_by_variant_keys": sorted(latest_by_variant.keys()),
        },
    )

variant_freshness = {}
latest_backtest_dt = None
for variant in EXPECTED_BACKTEST_VARIANTS:
    item = _require_mapping(latest_by_variant.get(variant) or {}, f"backtest summary for {variant}")
    variant_started_at = _ensure_recent(f"backtest {variant} started_at", item.get("started_at"), now)
    variant_summary = _require_mapping(item.get("summary") or {}, f"backtest {variant} summary")
    if int(variant_summary.get("total_predictions") or 0) <= 0:
        _fail(5, f"backtest {variant} summary is empty", {"variant": variant, "summary": variant_summary})
    variant_results = store.list_backtest_results(strategy_variant=variant, limit=1)
    if not variant_results:
        _fail(5, f"backtest {variant} has no result rows", {"variant": variant})
    latest_result = _require_mapping(variant_results[0], f"backtest {variant} latest result")
    result_created_at = _ensure_recent(
        f"backtest {variant} result created_at",
        latest_result.get("created_at"),
        now,
    )
    result_metrics = _require_mapping(latest_result.get("metrics") or {}, f"backtest {variant} metrics")
    if not result_metrics.get("prediction_id") or not result_metrics.get("verdict"):
        _fail(
            5,
            f"backtest {variant} metrics are incomplete",
            {"variant": variant, "metrics": result_metrics},
        )
    latest_backtest_dt = max(
        candidate
        for candidate in (latest_backtest_dt, variant_started_at, result_created_at)
        if candidate is not None
    )
    variant_freshness[variant] = {
        "started_at": variant_started_at.isoformat(),
        "started_age_minutes": _age_minutes(now, variant_started_at),
        "result_created_at": result_created_at.isoformat(),
        "result_age_minutes": _age_minutes(now, result_created_at),
        "total_predictions": int(variant_summary.get("total_predictions") or 0),
    }

pipeline_timestamps = {
    "review": review_dt,
    "backtest": latest_backtest_dt,
}
if score_dt is not None and mature_pending_count > 0:
    pipeline_timestamps["score"] = score_dt
oldest_label, oldest_dt = min(pipeline_timestamps.items(), key=lambda item: item[1])
newest_label, newest_dt = max(pipeline_timestamps.items(), key=lambda item: item[1])
pipeline_span = newest_dt - oldest_dt
if pipeline_span > MAX_PIPELINE_SKEW:
    _fail(
        5,
        "score/review/backtest freshness drift is too wide",
        {
            "timestamps": {label: dt.isoformat() for label, dt in pipeline_timestamps.items()},
            "age_minutes": {label: _age_minutes(now, dt) for label, dt in pipeline_timestamps.items()},
            "oldest_label": oldest_label,
            "newest_label": newest_label,
            "pipeline_span_minutes": round(pipeline_span.total_seconds() / 60.0, 1),
            "max_pipeline_skew_minutes": round(MAX_PIPELINE_SKEW.total_seconds() / 60.0, 1),
        },
    )

print("AstroGrid smoke passed")
print(json.dumps({
    "prediction_id": prediction.get("prediction_id"),
    "persistence_status": guru_payload.get("persistence_status"),
    "score_freshness_minutes": _age_minutes(now, score_dt) if score_dt is not None else None,
    "mature_pending_predictions": mature_pending_count,
    "review_freshness_minutes": _age_minutes(now, review_dt),
    "backtest_freshness_minutes": _age_minutes(now, latest_backtest_dt),
    "best_variant_by_group": weights_best_variant_by_group,
    "group_conditionals": weights_group_conditionals,
    "backtest_variants": variant_freshness,
}, indent=2))
PY
