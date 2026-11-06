"""Smart heatmap payloads (``GET /api/v1/discovery/smart-heatmap``).

The route used to build this inline: a 756-day all-feature PIT read for the
orthogonality audit (26 s on grid-svr, 2026-11-06) and then a second read
for the heatmap matrix that never succeeded -- it called
``get_feature_matrix(feature_ids=..., as_of_date=None)`` without the
required ``start_date`` / ``end_date``, so every response carried an empty
``matrix`` after the 26 s audit.

Now the precompute job (``intelligence/api_precompute.py``) calls
:func:`build_smart_heatmap_payloads` once: one PIT read shared by the audit
and the heatmap, then one payload per family filter, all from the same
matrix. The route only reads the stored result.

PIT: the matrix is read with ``as_of_date = end_date = today`` through
``store/pit.py`` (the audit's own window and vintage policy). A feature with
no observation on the matrix's last row gets ``zscore: null``, never ``0``.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

DEFAULT_CORR_THRESHOLD = 0.8


def _registry(engine: Engine) -> list[tuple[int, str, str | None]]:
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, name, family FROM feature_registry "
            "WHERE model_eligible = TRUE ORDER BY id"
        )).fetchall()
    return [(int(r[0]), r[1], r[2]) for r in rows]


def _round_or_none(value: Any, ndigits: int = 4) -> float | None:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(f):
        return None
    return round(f, ndigits)


def heatmap_payload(
    full: pd.DataFrame,
    selected_ids: list[int],
    id_to_name: dict[int, str],
    id_to_family: dict[int, str | None],
    *,
    family: str | None,
    orthogonal_only: bool,
    redundant_pairs: list[dict[str, Any]],
    total_count: int,
    all_families: list[str],
) -> dict[str, Any]:
    """One response body for a feature subset, sliced out of ``full``.

    ``full`` is the wide PIT matrix (index obs_date, columns feature ids).
    Slicing columns and dropping all-empty rows reproduces what a separate
    ``get_feature_matrix`` call for the subset would return.
    """
    ids = [fid for fid in selected_ids if family is None or id_to_family.get(fid) == family]
    base = {
        "families": all_families,
        "filtered_count": len(ids),
        "total_count": total_count,
        "redundant_pairs": redundant_pairs[:10],
        "orthogonal_only": orthogonal_only,
        "family_filter": family,
    }
    if not ids:
        return {"features": [], "matrix": [], "z_scores": [], **base,
                "as_of_date": None}

    cols = [fid for fid in ids if fid in full.columns]
    features = [id_to_name[fid] for fid in ids if fid in id_to_name]
    if not cols:
        return {"features": features, "matrix": [], "z_scores": [], **base,
                "as_of_date": None}

    df = full[cols].dropna(how="all")
    if df.empty:
        return {"features": features, "matrix": [], "z_scores": [], **base,
                "as_of_date": None}

    corr = df.corr()
    matrix = [[_round_or_none(v) for v in row] for row in corr.to_numpy()]
    last_row = df.iloc[-1]
    z = (last_row - df.mean()) / df.std().replace(0, 1)
    features = [id_to_name.get(fid, str(fid)) for fid in cols]
    z_scores = [
        {
            "feature": id_to_name.get(fid, str(fid)),
            "zscore": _round_or_none(z.get(fid)),
            "family": id_to_family.get(fid) or "other",
        }
        for fid in cols
    ]
    as_of = df.index.max()
    return {
        "features": features,
        "matrix": matrix,
        "z_scores": z_scores,
        **base,
        "as_of_date": as_of.date().isoformat() if hasattr(as_of, "date") else str(as_of),
    }


def build_smart_heatmap_payloads(
    engine: Engine,
    pit_store: Any,
    corr_threshold: float = DEFAULT_CORR_THRESHOLD,
    today: date | None = None,
) -> list[tuple[dict[str, Any], dict[str, Any], date | None]]:
    """Every precomputed variant: ``[(params, payload, as_of), ...]``.

    Variants: ``orthogonal_only=True`` with no family filter, and one per
    family present in the orthogonal set. ``orthogonal_only=False`` is not
    precomputed (no client asks for it).
    """
    from discovery.orthogonality import ORTHOGONALITY_WINDOW_DAYS, OrthogonalityAudit

    today = today or date.today()  # noqa: DTZ011 -- same local-date convention as store/pit.py readers
    reg = _registry(engine)
    if not reg:
        return []
    all_ids = [r[0] for r in reg]
    id_to_name = {r[0]: r[1] for r in reg}
    id_to_family = {r[0]: r[2] for r in reg}
    all_families = sorted({f for f in id_to_family.values() if f is not None})

    full = pit_store.get_feature_matrix(
        feature_ids=all_ids,
        start_date=today - timedelta(days=ORTHOGONALITY_WINDOW_DAYS),
        end_date=today,
        as_of_date=today,
    )
    if full is None:
        full = pd.DataFrame()

    audit = OrthogonalityAudit(db_engine=engine, pit_store=pit_store)
    ortho = audit.get_orthogonal_features(corr_threshold=corr_threshold, matrix=full)
    selected = list(ortho["orthogonal_ids"])
    redundant = list(ortho["redundant_pairs"])

    families: list[str | None] = [None] + sorted(
        {id_to_family[f] for f in selected if id_to_family.get(f) is not None}
    )
    out: list[tuple[dict[str, Any], dict[str, Any], date | None]] = []
    for fam in families:
        payload = heatmap_payload(
            full, selected, id_to_name, id_to_family,
            family=fam, orthogonal_only=True, redundant_pairs=redundant,
            total_count=len(all_ids), all_families=all_families,
        )
        as_of = date.fromisoformat(payload["as_of_date"]) if payload.get("as_of_date") else None
        params = {"family": fam, "orthogonal_only": True, "corr_threshold": corr_threshold}
        out.append((params, payload, as_of))
    log.info("smart heatmap: {n} variants from one {r}x{c} matrix",
             n=len(out), r=full.shape[0], c=full.shape[1])
    return out
