"""Serve a precomputed endpoint result (W3-2).

Routes that used to compute for 11-160 s call :func:`serve_precomputed`
instead: one primary-key read of ``api_precomputed_results`` and the stored
JSON returned as-is with a ``precompute`` freshness block spliced in (see
``store/precomputed.py``). Nothing is ever computed in the request.

* stored result -> 200, the endpoint's usual body plus ``precompute``
  (``computed_at``, data ``as_of``, ``age_seconds``, ``stale``, ...).
* nothing stored for these parameters -> 503 with an AVAILABILITY_CONTRACT
  ``unavailable`` body (the endpoint's fields as ``null``), a reason that
  says whether the job has not run yet or the parameters are not
  precomputed, and ``Retry-After``.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any

from fastapi.responses import JSONResponse, Response
from loguru import logger as log

from api.dependencies import get_db_engine
from intelligence.api_precompute import TICK_MINUTES, job_for_key
from store import precomputed as store
from store.availability import unavailable


def _unavailable_response(
    key: str, params: Mapping[str, Any], reason: str, meta: dict[str, Any], empty_fields: tuple[str, ...],
    precomputed_params: list[dict[str, Any]],
) -> JSONResponse:
    body = unavailable(reason, **{f: None for f in empty_fields})
    body["precompute"] = meta
    body["precomputed_params"] = precomputed_params
    return JSONResponse(status_code=503, content=body,
                        headers={"Retry-After": str(TICK_MINUTES * 60)})


def serve_precomputed(
    key: str,
    params: Mapping[str, Any] | None = None,
    *,
    transform: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> Response:
    """Return the stored result for ``(key, params)`` without computing it.

    ``transform`` derives a variant from the stored body (e.g. dropping an
    optional section); it forces a decode, so use it only when needed.
    """
    params = dict(params or {})
    job = job_for_key(key)
    empty_fields = tuple(job.empty_fields.get(key, ()))
    declared = [p for k, p in job.declared() if k == key]
    try:
        row = store.read_result(get_db_engine(), key, params)
    except Exception as exc:  # noqa: BLE001 -- table missing / DB down: say so, don't compute
        log.warning("precomputed read failed for {k}: {e}", k=key, e=str(exc))
        meta = store.freshness_meta(None, key=key, params=params,
                                    stale_after_s=job.stale_after_s, cadence_s=job.cadence_s)
        return _unavailable_response(
            key, params, f"precomputed result store unreadable: {type(exc).__name__}",
            meta, empty_fields, declared,
        )

    meta = store.freshness_meta(row, key=key, params=params,
                                stale_after_s=job.stale_after_s, cadence_s=job.cadence_s)
    if row is None or not row.has_payload:
        if row is not None and row.last_status in (store.STATUS_ERROR, store.STATUS_UNAVAILABLE):
            reason = (f"background job '{job.name}' has not produced a result yet; "
                      f"last attempt {row.last_status}: {row.last_error}")
        elif row is not None or params in declared:
            reason = (f"not computed yet: background job '{job.name}' refreshes every "
                      f"{job.cadence_s // 60} min; this endpoint never computes inline")
        else:
            reason = ("these parameters are not precomputed; this endpoint never computes "
                      "inline -- use one of precomputed_params")
        return _unavailable_response(key, params, reason, meta, empty_fields, declared)

    assert row.payload_json is not None
    if transform is not None:
        body = transform(json.loads(row.payload_json))
        body["precompute"] = meta
        return Response(content=store.dumps(body), media_type="application/json")
    return Response(content=store.splice_meta(row.payload_json, meta), media_type="application/json")


def read_precomputed(key: str, params: Mapping[str, Any] | None = None) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Decoded stored body (or ``None``) plus its ``precompute`` block.

    For routes that combine a precomputed part with cheap live parts
    (``/system/architecture``). Never computes.
    """
    params = dict(params or {})
    job = job_for_key(key)
    try:
        row = store.read_result(get_db_engine(), key, params)
    except Exception as exc:  # noqa: BLE001
        log.warning("precomputed read failed for {k}: {e}", k=key, e=str(exc))
        row = None
    meta = store.freshness_meta(row, key=key, params=params,
                                stale_after_s=job.stale_after_s, cadence_s=job.cadence_s)
    if row is None or not row.has_payload:
        return None, meta
    return row.payload(), meta
