"""Capability-gated reader for frozen feature artifacts (GD5 ``write_frozen_artifact`` output).

Panel mode reads constructs only from frozen artifacts (standing rule R1):
a deterministic parquet file plus a receipt JSON next to it
(``<artifact>.receipt.json``). GD5's writer is not merged yet; this reader
defines the receipt contract against a fixture and adopts GD5's receipt once
it lands (the field names below follow the GD5 brief, section 2).

Refused: an unknown hash (the file's sha256 must equal the hash the caller
froze; likewise the receipt file's sha256), a receipt that does not name that hash, an artifact kind outside the
allowed list, a missing receipt field, a column set or order other than the
receipt's, a naive or non-UTC timestamp (column values or the receipt's
``as_of``), a missing ``known_at``, and any ``known_at`` later than ``as_of``
or than its row's ``decision_at``. The frame is parsed from the very bytes
that were hashed. Returns the frame and the receipt for the run manifest.
"""

from __future__ import annotations

import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

from analysis.offline_research_proof import digest

#: Receipt fields every frozen artifact carries (GD5 section 2).
RECEIPT_KEYS: tuple[str, ...] = (
    "kind",
    "artifact_sha256",
    "columns",
    "row_count",
    "timestamp_columns",
    "input_event_set_sha256",
    "spec_sha256s",
    "code_sha",
    "as_of",
    "membership_map_sha256",
)
RECEIPT_SUFFIX = ".receipt.json"


def _hex64(value) -> bool:
    return isinstance(value, str) and len(value) == 64 and not set(value) - set("0123456789abcdef")


def _utc(value: str, what: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise PermissionError(f"{what} is not an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
        raise PermissionError(f"{what} must be a UTC timestamp (got {value!r})")
    return parsed.astimezone(timezone.utc)


def receipt_path(artifact: Path) -> Path:
    return Path(f"{artifact}{RECEIPT_SUFFIX}")


def read_frozen_artifact(
    artifact: Path,
    *,
    expected_sha256: str,
    expected_receipt_sha256: str,
    allowed_kinds: Iterable[str],
) -> tuple[pd.DataFrame, dict]:
    """The frozen frame and its receipt, after every check above (nothing is read on refusal)."""
    artifact = Path(artifact)
    if not _hex64(expected_sha256) or not _hex64(expected_receipt_sha256):
        raise PermissionError("the expected artifact and receipt sha256 must be frozen sha256 hex digests")
    allowed = frozenset(allowed_kinds)
    if not allowed:
        raise PermissionError("declare the artifact kinds this construct may read")
    data = artifact.read_bytes()
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected_sha256:
        raise PermissionError(f"artifact hashes to {actual[:12]}, frozen {expected_sha256[:12]}: unknown artifact")
    try:
        receipt_bytes = receipt_path(artifact).read_bytes()
    except FileNotFoundError as exc:
        raise PermissionError("the artifact has no receipt") from exc
    if hashlib.sha256(receipt_bytes).hexdigest() != expected_receipt_sha256:
        raise PermissionError("the receipt is not the frozen one (its sha256 differs)")
    try:
        receipt = json.loads(receipt_bytes.decode("utf-8"))
    except ValueError as exc:
        raise PermissionError("the receipt is not JSON") from exc
    if not isinstance(receipt, dict):
        raise PermissionError("the receipt is not a JSON object")
    missing = [k for k in RECEIPT_KEYS if k not in receipt]
    if missing:
        raise PermissionError(f"the receipt lacks {missing}")
    if receipt["artifact_sha256"] != expected_sha256:
        raise PermissionError("the receipt names another artifact")
    if receipt["kind"] not in allowed:
        raise PermissionError(f"artifact kind {receipt['kind']!r} is not one of {sorted(allowed)}")
    for k in ("input_event_set_sha256", "membership_map_sha256"):
        if not _hex64(receipt[k]):
            raise PermissionError(f"receipt {k} must be a sha256 hex digest")
    specs = receipt["spec_sha256s"]
    if not isinstance(specs, dict) or not specs or any(not _hex64(v) for v in specs.values()):
        raise PermissionError("receipt spec_sha256s must map spec names to sha256 digests")
    as_of = _utc(receipt["as_of"], "receipt as_of")
    frame = pd.read_parquet(io.BytesIO(data))
    if list(frame.columns) != list(receipt["columns"]):
        raise PermissionError(f"artifact columns {list(frame.columns)} differ from the receipt's {receipt['columns']}")
    if len(frame) != receipt["row_count"]:
        raise PermissionError("artifact row count differs from the receipt's")
    stamps = receipt["timestamp_columns"]
    if not isinstance(stamps, list) or not set(stamps) <= set(frame.columns):
        raise PermissionError("receipt timestamp_columns must name artifact columns")
    for column in frame.columns:
        dtype = frame[column].dtype
        is_time = isinstance(dtype, pd.DatetimeTZDtype) or pd.api.types.is_datetime64_any_dtype(dtype)
        if is_time and column not in stamps:
            raise PermissionError(f"timestamp column {column!r} is not declared in the receipt")
    for column in stamps:
        dtype = frame[column].dtype
        if not isinstance(dtype, pd.DatetimeTZDtype):
            raise PermissionError(f"{column!r} is a naive timestamp column (timestamps must be UTC)")
        if str(dtype.tz) != "UTC":
            raise PermissionError(f"{column!r} is in {dtype.tz}, not UTC")
    if "known_at" in frame.columns:
        if "known_at" not in stamps:
            raise PermissionError("known_at must be a declared UTC timestamp column")
        if frame["known_at"].isna().any():
            raise PermissionError("a row has no known_at")
        if (frame["known_at"] > pd.Timestamp(as_of)).any():
            raise PermissionError("an event's known_at is later than the artifact's as_of (look-ahead)")
        if "decision_at" in frame.columns and (frame["known_at"] > frame["decision_at"]).any():
            raise PermissionError("a row's known_at is later than its decision_at (look-ahead)")
    return frame, {**receipt, "receipt_sha256": digest(receipt)}


__all__ = ["RECEIPT_KEYS", "read_frozen_artifact", "receipt_path"]
