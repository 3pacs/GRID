"""GRID Intelligence — ReasoningBank-style memory layer.

Distilled, immutable strategy items derived from BOTH success and failure
paths (postmortems, oracle scoring, hypothesis kills). Lessons are keyed
on a condition fingerprint (regime / fci_bucket / vol_bucket /
horizon_bucket / ticker / direction) and retrieved at decision time so
new predictions can lean on prior experience.

Inspired by Google's ReasoningBank (2025).

Design rules (CLAUDE.md):
  - Insert-only (PIT correctness — like the decision journal).
  - Parameterized SQL only — never f-strings / .format().
  - Defensive: every DB call wrapped in try/except. Writers return None
    on failure, retrievers return []. Never crash the caller.
  - Leaf module — must NOT import oracle/, intelligence/postmortem.py,
    or anything that could create an import cycle.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# Default fingerprint keys we treat as "overlapping" when ranking lessons
# without an embedding. Mirrors the meta_learning_matrix ConditionTuple
# plus a couple of trade-shape extras the postmortem path also stores.
DEFAULT_OVERLAP_KEYS: tuple[str, ...] = (
    "regime",
    "liquidity_regime",
    "fci_bucket",
    "vol_bucket",
    "vol_regime",
    "horizon_bucket",
    "ticker",
    "direction",
)


OutcomeClass = Literal["success", "failure", "neutral"]
OutcomeFilter = Literal["success", "failure", "neutral", "any"]


@dataclass(frozen=True)
class ReasoningLesson:
    """A compact strategy item distilled from a past trade or prediction.

    The trio (title, description, content) follows the ReasoningBank
    paper: title is a one-line tag, description names the situation,
    content is the actual lesson / pitfall / strategy text.
    """

    title: str
    description: str
    content: str
    outcome_class: OutcomeClass
    condition_fingerprint: dict[str, Any] = field(default_factory=dict)
    source_type: str = "unknown"
    source_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ──────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────

def _vec_to_pg_literal(vec: list[float]) -> str:
    """Convert a Python list of floats to a pgvector string literal.

    Mirrors ``intelligence.rag._vec_to_pg_literal`` so we don't introduce
    a cross-module dependency for a 1-line helper.
    """
    return "[" + ",".join(f"{float(v):.6f}" for v in vec) + "]"


def _is_postgres(engine: Engine) -> bool:
    """Best-effort dialect check. Returns False on any failure."""
    try:
        return getattr(engine, "dialect", None) is not None and (
            engine.dialect.name == "postgresql"
        )
    except Exception:
        return False


def _coerce_outcome(value: Any) -> OutcomeClass:
    """Normalize an outcome string into the constrained set."""
    s = str(value or "").strip().lower()
    if s in ("success", "win", "hit"):
        return "success"
    if s in ("failure", "fail", "loss", "miss"):
        return "failure"
    return "neutral"


def _row_to_lesson(row: Any) -> ReasoningLesson | None:
    """Convert a SQL row into a ReasoningLesson. Returns None on error."""
    try:
        fingerprint = row.condition_fingerprint
        if isinstance(fingerprint, str):
            try:
                fingerprint = json.loads(fingerprint)
            except (TypeError, ValueError):
                fingerprint = {}
        if not isinstance(fingerprint, dict):
            fingerprint = {}
        return ReasoningLesson(
            title=str(row.title or ""),
            description=str(row.description or ""),
            content=str(row.content or ""),
            outcome_class=_coerce_outcome(row.outcome_class),
            condition_fingerprint=fingerprint,
            source_type=str(row.source_type or "unknown"),
            source_id=(
                str(row.source_id) if row.source_id is not None else None
            ),
        )
    except Exception as exc:  # pragma: no cover — defensive
        log.debug("reasoning_bank: row->lesson failed: {e}", e=str(exc))
        return None


# ──────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────


def write_reasoning_lesson(
    engine: Engine,
    lesson: ReasoningLesson,
    embedding: list[float] | None = None,
) -> int | None:
    """Insert a lesson. Returns the new row id, or ``None`` on failure.

    ``embedding`` is a 768-dim list (nomic-embed-text) or ``None`` for
    lazy backfill. Defensive — never raises.
    """
    if lesson is None:
        return None

    fingerprint_json = "{}"
    try:
        fingerprint_json = json.dumps(
            lesson.condition_fingerprint or {}, default=str
        )
    except (TypeError, ValueError) as exc:
        log.warning(
            "reasoning_bank: fingerprint not JSON-serializable, "
            "falling back to empty: {e}",
            e=str(exc),
        )
        fingerprint_json = "{}"

    has_vec = (
        embedding is not None
        and isinstance(embedding, (list, tuple))
        and len(embedding) > 0
    )

    if has_vec and _is_postgres(engine):
        sql = (
            "INSERT INTO reasoning_lessons "
            "(title, description, content, outcome_class, "
            " condition_fingerprint, source_type, source_id, embedding) "
            "VALUES "
            "(:title, :description, :content, :outcome_class, "
            " CAST(:fingerprint AS jsonb), :source_type, :source_id, "
            " CAST(:embedding AS vector)) "
            "RETURNING id"
        )
        params: dict[str, Any] = {
            "title": lesson.title,
            "description": lesson.description,
            "content": lesson.content,
            "outcome_class": _coerce_outcome(lesson.outcome_class),
            "fingerprint": fingerprint_json,
            "source_type": lesson.source_type,
            "source_id": lesson.source_id,
            "embedding": _vec_to_pg_literal(list(embedding)),  # type: ignore[arg-type]
        }
    else:
        sql = (
            "INSERT INTO reasoning_lessons "
            "(title, description, content, outcome_class, "
            " condition_fingerprint, source_type, source_id) "
            "VALUES "
            "(:title, :description, :content, :outcome_class, "
            " CAST(:fingerprint AS jsonb), :source_type, :source_id) "
            "RETURNING id"
        )
        params = {
            "title": lesson.title,
            "description": lesson.description,
            "content": lesson.content,
            "outcome_class": _coerce_outcome(lesson.outcome_class),
            "fingerprint": fingerprint_json,
            "source_type": lesson.source_type,
            "source_id": lesson.source_id,
        }

    try:
        with engine.begin() as conn:
            row = conn.execute(text(sql), params).fetchone()
    except Exception as exc:
        log.warning(
            "reasoning_bank: insert failed for source={s}/{i}: {e}",
            s=lesson.source_type, i=lesson.source_id, e=str(exc),
        )
        return None

    if row is None:
        return None

    try:
        return int(row[0])
    except (TypeError, ValueError, IndexError):
        return None


def retrieve_lessons(
    engine: Engine,
    *,
    fingerprint: dict[str, Any],
    query_embedding: list[float] | None = None,
    top_k: int = 5,
    outcome_class: OutcomeFilter = "any",
    fingerprint_overlap_keys: tuple[str, ...] = (
        "regime",
        "fci_bucket",
        "vol_bucket",
        "horizon_bucket",
    ),
) -> list[ReasoningLesson]:
    """Hybrid retrieve — defensive; never raises.

    Ranking strategy:
      - If ``query_embedding`` is supplied AND the engine is PostgreSQL,
        rank by pgvector cosine distance (``embedding <=> :qvec`` ASC),
        scoped to lessons whose fingerprint overlaps on any of
        ``fingerprint_overlap_keys`` (LATERAL ``jsonb_each_text`` filter).
      - Otherwise fall back to: most-fingerprint-keys-matching DESC,
        then ``created_at`` DESC.

    On any DB error returns ``[]``.
    """
    if top_k <= 0:
        return []

    fingerprint = fingerprint or {}
    keys = tuple(fingerprint_overlap_keys or ())

    # Build the per-key match expression: a CASE-summed integer score
    # that counts how many of the requested keys match between the
    # stored fingerprint and the query fingerprint. Only keys that are
    # actually present in the query fingerprint contribute.
    overlap_keys: list[str] = []
    overlap_params: dict[str, Any] = {}
    for idx, k in enumerate(keys):
        if k in fingerprint and fingerprint[k] is not None:
            overlap_keys.append(k)
            overlap_params[f"fpk_{idx}"] = k
            overlap_params[f"fpv_{idx}"] = str(fingerprint[k])

    # SQL fragment that computes the overlap score.
    if overlap_keys:
        score_terms = []
        for idx, k in enumerate(keys):
            if k not in fingerprint or fingerprint[k] is None:
                continue
            score_terms.append(
                "(CASE WHEN condition_fingerprint ->> "
                f":fpk_{idx} = :fpv_{idx} THEN 1 ELSE 0 END)"
            )
        score_expr = " + ".join(score_terms)
    else:
        score_expr = "0"

    where_parts: list[str] = []
    params: dict[str, Any] = {"top_k": int(top_k), **overlap_params}

    if outcome_class != "any":
        where_parts.append("outcome_class = :outcome_class")
        params["outcome_class"] = outcome_class

    where_clause = ""
    if where_parts:
        where_clause = "WHERE " + " AND ".join(where_parts)

    use_vector = (
        query_embedding is not None
        and isinstance(query_embedding, (list, tuple))
        and len(query_embedding) > 0
        and _is_postgres(engine)
    )

    if use_vector:
        params["qvec"] = _vec_to_pg_literal(list(query_embedding))  # type: ignore[arg-type]
        # Only consider rows that actually have an embedding; tie-break
        # by overlap score then recency for stability.
        sql = (
            "SELECT id, title, description, content, outcome_class, "
            "       condition_fingerprint, source_type, source_id, "
            "       created_at, "
            f"      ({score_expr}) AS overlap_score "
            "FROM reasoning_lessons "
            + (where_clause + " AND " if where_clause else "WHERE ")
            + "embedding IS NOT NULL "
            "ORDER BY embedding <=> CAST(:qvec AS vector) ASC, "
            "         overlap_score DESC, created_at DESC "
            "LIMIT :top_k"
        )
    else:
        sql = (
            "SELECT id, title, description, content, outcome_class, "
            "       condition_fingerprint, source_type, source_id, "
            "       created_at, "
            f"      ({score_expr}) AS overlap_score "
            "FROM reasoning_lessons "
            + where_clause + " "
            "ORDER BY overlap_score DESC, created_at DESC "
            "LIMIT :top_k"
        )

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
    except Exception as exc:
        log.warning("reasoning_bank: retrieve failed: {e}", e=str(exc))
        return []

    out: list[ReasoningLesson] = []
    for row in rows:
        lesson = _row_to_lesson(row)
        if lesson is not None:
            out.append(lesson)
    return out


def lesson_count(
    engine: Engine,
    *,
    outcome_class: str | None = None,
) -> int:
    """Count lessons in the bank (for monitoring). Defensive — returns 0
    on any failure.
    """
    where_clause = ""
    params: dict[str, Any] = {}
    if outcome_class is not None:
        where_clause = "WHERE outcome_class = :outcome_class"
        params["outcome_class"] = _coerce_outcome(outcome_class)

    sql = "SELECT COUNT(*) FROM reasoning_lessons " + where_clause
    try:
        with engine.connect() as conn:
            row = conn.execute(text(sql), params).fetchone()
    except Exception as exc:
        log.debug("reasoning_bank: count failed: {e}", e=str(exc))
        return 0

    if row is None:
        return 0
    try:
        return int(row[0])
    except (TypeError, ValueError, IndexError):
        return 0


# ──────────────────────────────────────────────────────────────────────────
# Convenience: fingerprint construction from a postmortem-style payload
# ──────────────────────────────────────────────────────────────────────────


def _bucket_horizon_days(days: Any) -> str | None:
    """Approximate horizon bucketization without importing meta_learning_matrix.

    We avoid the import to keep this module a leaf — meta_learning_matrix
    re-exports its bucket helpers but importing it would entangle two
    independent layers. The thresholds mirror that module's defaults.
    """
    try:
        d = int(days)
    except (TypeError, ValueError):
        return None
    if d <= 0:
        return None
    if d <= 1:
        return "intraday"
    if d <= 7:
        return "1w"
    if d <= 30:
        return "1m"
    if d <= 90:
        return "3m"
    return "long"


def build_fingerprint_from_decision_data(
    *,
    ticker: str | None,
    direction: str | None,
    data_at_decision: dict[str, Any] | None,
    horizon_days: int | None = None,
) -> dict[str, Any]:
    """Reconstruct a condition fingerprint from a postmortem-style payload.

    Pulls the regime / FCI / vol / horizon hints out of the
    ``data_at_decision`` dict that ``intelligence.postmortem`` already
    builds (or that an oracle/scoring caller provides). Always returns
    a dict — empty fields are simply omitted. Never raises.
    """
    fp: dict[str, Any] = {}
    if ticker:
        fp["ticker"] = str(ticker).upper()
    if direction:
        fp["direction"] = str(direction).lower()

    data = data_at_decision or {}
    if not isinstance(data, dict):
        data = {}

    # regime: prefer the postmortem nested shape, fall back to flat.
    regime_block = data.get("regime")
    if isinstance(regime_block, dict):
        state = regime_block.get("state")
        if state:
            fp["regime"] = str(state)
            fp["liquidity_regime"] = str(state)
    else:
        for k in ("regime", "liquidity_regime"):
            v = data.get(k)
            if v:
                fp[k] = str(v)

    # fci / vol / horizon — flat keys
    for src_key, fp_key in (
        ("fci_regime", "fci_bucket"),
        ("fci_bucket", "fci_bucket"),
        ("vix_level", "vix_level"),
        ("vol_bucket", "vol_bucket"),
        ("vol_regime", "vol_regime"),
    ):
        v = data.get(src_key)
        if v is not None and v != "":
            fp[fp_key] = v if not isinstance(v, str) else v

    # Horizon — caller-provided takes precedence
    bucket = _bucket_horizon_days(horizon_days)
    if bucket is None:
        bucket = _bucket_horizon_days(data.get("horizon_days"))
    if bucket is not None:
        fp["horizon_bucket"] = bucket
    if horizon_days is not None:
        try:
            fp["horizon_days"] = int(horizon_days)
        except (TypeError, ValueError):
            pass

    return fp


# ──────────────────────────────────────────────────────────────────────────
# Conviction-stack adapter (15th adjuster)
# ──────────────────────────────────────────────────────────────────────────


# Range cap for the memory-lesson multiplier. Narrow on purpose: this is
# a strategic prior, not direct firing evidence — the conviction stack
# already has stronger per-event multipliers.
MEMORY_LESSON_MULT_MIN: float = 0.85
MEMORY_LESSON_MULT_MAX: float = 1.15
MEMORY_LESSON_SLOPE: float = 0.15  # so mult = 1 + slope * score, score ∈ [-1, 1]


def memory_lesson_conviction_multiplier(
    engine: Engine,
    *,
    fingerprint: dict[str, Any],
    top_k: int = 20,
    require_direction_match: bool = True,
) -> float:
    """Return a conviction multiplier in [0.85, 1.15] driven by past
    distilled lessons that match the live fingerprint.

    Counts the outcome classes of retrieved lessons:

        score = (n_success - n_failure - 0.3 * n_neutral) / total
        multiplier = clip(1 + 0.15 * score, 0.85, 1.15)

    Where ``neutral`` lessons (e.g. ``oracle_contrast`` divergence
    distillations) act as a mild haircut — they signal that this
    fingerprint has historically produced model disagreement, which is
    weak evidence against high conviction.

    When ``require_direction_match=True`` (default) and the fingerprint
    contains a ``direction``, only lessons whose stored fingerprint has
    the same direction are counted. This prevents a failure-tagged
    bullish lesson from haircut-ing a bearish trade in the same regime.

    Defensive: returns 1.0 on any failure.
    """
    try:
        lessons = retrieve_lessons(
            engine,
            fingerprint=fingerprint,
            top_k=int(top_k) if top_k else 20,
            outcome_class="any",
        )
    except Exception as exc:  # noqa: BLE001 — defensive boundary
        log.debug("reasoning_bank: lesson retrieval failed: {e}", e=str(exc))
        return 1.0

    if not lessons:
        return 1.0

    if require_direction_match:
        live_dir = str(fingerprint.get("direction") or "").lower()
        if live_dir:
            lessons = [
                l for l in lessons
                if str(l.condition_fingerprint.get("direction") or "").lower() == live_dir
            ]
            if not lessons:
                return 1.0

    n_success = sum(1 for l in lessons if l.outcome_class == "success")
    n_failure = sum(1 for l in lessons if l.outcome_class == "failure")
    n_neutral = sum(1 for l in lessons if l.outcome_class == "neutral")
    total = n_success + n_failure + n_neutral
    if total <= 0:
        return 1.0

    score = (n_success - n_failure - 0.3 * n_neutral) / float(total)
    score = max(-1.0, min(1.0, score))
    multiplier = 1.0 + MEMORY_LESSON_SLOPE * score
    return max(MEMORY_LESSON_MULT_MIN, min(MEMORY_LESSON_MULT_MAX, multiplier))


__all__ = [
    "ReasoningLesson",
    "DEFAULT_OVERLAP_KEYS",
    "MEMORY_LESSON_MULT_MIN",
    "MEMORY_LESSON_MULT_MAX",
    "MEMORY_LESSON_SLOPE",
    "write_reasoning_lesson",
    "retrieve_lessons",
    "lesson_count",
    "build_fingerprint_from_decision_data",
    "memory_lesson_conviction_multiplier",
]
