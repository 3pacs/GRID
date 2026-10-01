"""The common prediction record every stream adapter normalizes into.

A normalized prediction (a plain dict, canonical-JSON-able)::

    stream            "gex_levels_v1" | "s10_hypothesis_forward_v1" | <generic stream>
    family            the family the scoreboard aggregates by
    sector            sector label or None
    prediction_id     globally unique, stable across runs ("<stream>:...")
    issued_at         when the stream logged it (ISO-8601 UTC, from the stream's own record)
    log_receipt       where it was logged: source file, 0-based line index, the line's
                      sha256 and prev_sha256 (its hash-chain position), the source head and
                      record count E2 saw, the writer's code sha, and the witness basis
    target            {"instrument", "instrument_class", ...} or a series description
    horizon           {"label", "ends_at" (ISO), ...} -- the end of the measured window
    outcome_not_before  the earliest instant the outcome could possibly be observable (ISO,
                      conservative: early); E2 says it witnessed a prediction "before its
                      outcome" only if it ingested it before this instant
    call              the prediction itself; ``kind`` is one of CALL_KINDS
    rule_id           the pre-registered scoring rule (rules.json)
    unit              unit key for unit-level rules (rank IC per date, S10 candidate) or None

A resolution (``status`` resolved | void) carries the outcome, the price
source and vintage it came from (``receipt``) and ``available_at``: the
instant the outcome became observable. E2 never accepts an outcome whose
``available_at`` precedes ``horizon.ends_at`` or follows the run instant.
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

from evals.e2.chain import digest

EASTERN = ZoneInfo("America/New_York")
REGULAR_CLOSE = time(16, 0)
EARLY_CLOSE = time(13, 0)

CALL_KINDS = frozenset({"direction", "probability", "rank_score", "conditional_binary", "rule_trade", "signal"})
REQUIRED = ("stream", "family", "sector", "prediction_id", "issued_at", "log_receipt", "target", "horizon",
            "outcome_not_before", "call", "rule_id", "unit")


class RecordError(ValueError):
    """A normalized record that breaks the E2 record contract."""


class LookAheadError(RuntimeError):
    """An outcome was offered before it could have been observable.

    ``key`` names the offending source record (e.g. a stream line's sha256) so one bad
    record raises one integrity alert, however many predictions it touches.
    """

    def __init__(self, message: str, key: str | None = None) -> None:
        super().__init__(message)
        self.key = key


class PriceSourceLookAhead(LookAheadError):
    """E2's own price source offered a value too early: possibly transient, so the
    prediction stays pending (with an alert) and is voided only after the grace period."""


def parse_ts(value: str) -> datetime:
    ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if ts.tzinfo is None:
        raise RecordError(f"timestamp without a timezone: {value!r}")
    return ts.astimezone(timezone.utc)


def iso(ts: datetime) -> str:
    if ts.tzinfo is None:
        raise RecordError("naive datetime")
    return ts.astimezone(timezone.utc).isoformat()


def session_close_utc(day: date, *, early: bool = False) -> datetime:
    """The US regular session close (16:00 ET, or 13:00 ET on an early close) in UTC."""
    local = datetime.combine(day, EARLY_CLOSE if early else REGULAR_CLOSE, tzinfo=EASTERN)
    return local.astimezone(timezone.utc)


def session_open_utc(day: date) -> datetime:
    return datetime.combine(day, time(9, 30), tzinfo=EASTERN).astimezone(timezone.utc)


def validate_prediction(p: dict, rules: dict) -> dict:
    missing = [k for k in REQUIRED if k not in p]
    if missing:
        raise RecordError(f"prediction lacks {missing}")
    if not isinstance(p["prediction_id"], str) or not p["prediction_id"].startswith(f"{p['stream']}:"):
        raise RecordError(f"prediction_id must start with '{p['stream']}:'")
    if p["call"].get("kind") not in CALL_KINDS:
        raise RecordError(f"{p['prediction_id']}: unknown call kind {p['call'].get('kind')!r}")
    if p["rule_id"] not in rules["rules"]:
        raise RecordError(f"{p['prediction_id']}: rule {p['rule_id']!r} is not pre-registered in rules.json")
    issued, not_before = parse_ts(p["issued_at"]), parse_ts(p["outcome_not_before"])
    parse_ts(p["horizon"]["ends_at"])
    if not issued < not_before:
        raise RecordError(f"{p['prediction_id']}: issued_at must precede outcome_not_before (logged before "
                          "its outcome could be observable)")
    receipt = p["log_receipt"]
    for key in ("log", "line_index", "line_sha256", "prev_sha256"):
        if key not in receipt:
            raise RecordError(f"{p['prediction_id']}: log_receipt lacks {key}")
    return p


def prediction_sha256(p: dict) -> str:
    """Identity of a prediction's content (everything but the ingestion context in ``log_receipt``).

    A rewrite of the source log upstream of a prediction changes its line hash but not
    its content; that is caught once per stream by the board's source-prefix check.
    """
    return digest({k: v for k, v in p.items() if k != "log_receipt"})
