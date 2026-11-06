"""Regression tests for scripts/generate_crypto_predictions.py (2026-11-05).

Two production failures on grid-svr's ``grid-predict-crypto`` unit:

* From 17:31Z every run crashed: ``market_overlay_snapshot`` was passed as
  ``json.dumps(...)`` and ``AstroGridStore.save_prediction`` calls ``dict()``
  on it. The same JSON-string payloads had already been persisting
  ``target_symbols`` as a list of characters (``['[', '"', 'B', ...]``).
* Before that, 6 of 16 calls per run hit ``prediction_run_prediction_id_key``
  because the id only hashed symbol/horizon/direction/now, and the run still
  exited 0.
"""
from __future__ import annotations

import importlib
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from store.astrogrid import (
    LEGACY_CRYPTO_PAYLOAD_CUTOFF,
    AstroGridStore,
    _decode_legacy_json_object,
    _decode_legacy_target_symbols,
    _is_legacy_crypto_row,
)

NOW = datetime(2026, 11, 5, 16, 29, 44, 719091, tzinfo=timezone.utc)
PRICES = {"BTC": 77453.86, "ETH": 2274.77, "SOL": 92.83}
THESIS = ("neutral", 0.4)


@pytest.mark.parametrize("available, errors, expected", [
    (True, [], 0), (False, [], 1), (True, ["persistence failed"], 1),
])
def test_generator_reports_evidence_and_persistence_failure(monkeypatch, available, errors, expected):
    gen = importlib.import_module("scripts.generate_crypto_predictions")
    import db
    monkeypatch.setattr(db, "get_engine", lambda: MagicMock())
    monkeypatch.setattr(gen, "AstroGridStore", MagicMock())
    monkeypatch.setattr(gen, "produce_insights", lambda *args: {
        "observations": [{"available": available}], "errors": errors,
    })
    assert gen.main() == expected


# --- Read-side normalizer for the legacy rows (42,488 grid-crypto-v1 rows on
# grid-svr written 2026-03-29..2026-11-05). Stored bytes stay untouched, and
# only grid-crypto-v1 rows created before LEGACY_CRYPTO_PAYLOAD_CUTOFF decode.

LEGACY_BTC = list(json.dumps(["BTC"]))  # ['[', '"', 'B', 'T', 'C', '"', ']']
BEFORE_CUTOFF = NOW  # 2026-11-05T16:29:44Z, the last legacy write
AFTER_CUTOFF = LEGACY_CRYPTO_PAYLOAD_CUTOFF + timedelta(seconds=1)


@pytest.mark.parametrize(
    "stored, expected, decoded",
    [
        (LEGACY_BTC, ["BTC"], True),
        (list(json.dumps(["ETH", "SOL"])), ["ETH", "SOL"], True),
        (json.dumps(LEGACY_BTC), ["BTC"], True),  # driver handed back JSON text
        (["BTC"], ["BTC"], False),
        (["F", "T"], ["F", "T"], False),  # real one-letter tickers stay as-is
        (["[", "]"], ["[", "]"], False),  # joins to an empty list
        (list(json.dumps([1])), list(json.dumps([1])), False),  # non-string symbols
        (list(json.dumps(["btc"])), list(json.dumps(["btc"])), False),  # not ticker-shaped
        (list(json.dumps(["<script>"])), list(json.dumps(["<script>"])), False),
        (list(json.dumps(["ABCDEFGHIJK"])), list(json.dumps(["ABCDEFGHIJK"])), False),  # >10 chars
        (list(json.dumps({"BTC": 1})), list(json.dumps({"BTC": 1})), False),  # not a list
        (list("[" * 600), list("[" * 600), False),  # oversized
        (list("[" * 400 + "]" * 400)[:500], list("[" * 400 + "]" * 400)[:500], False),  # bad JSON
        ([], [], False),
        (None, [], False),
    ],
)
def test_decode_legacy_target_symbols_on_legacy_row(stored, expected, decoded) -> None:
    assert _decode_legacy_target_symbols(stored, legacy_row=True) == (expected, decoded)


def test_decode_legacy_target_symbols_survives_deep_nesting() -> None:
    nested = list("[" * 256 + "]" * 256)  # 512 chars: under the cap, deeply nested
    assert _decode_legacy_target_symbols(nested, legacy_row=True) == (nested, False)


def test_decode_legacy_target_symbols_needs_legacy_row() -> None:
    assert _decode_legacy_target_symbols(LEGACY_BTC, legacy_row=False) == (LEGACY_BTC, False)


@pytest.mark.parametrize(
    "model_version, created_at, expected",
    [
        ("grid-crypto-v1", BEFORE_CUTOFF, True),
        ("grid-crypto-v1", AFTER_CUTOFF, False),
        ("grid-crypto-v1", LEGACY_CRYPTO_PAYLOAD_CUTOFF, False),
        ("astrogrid-oracle-v1", BEFORE_CUTOFF, False),
        ("grid-crypto-v1", BEFORE_CUTOFF.replace(tzinfo=None), False),  # naive: fail safe
        ("grid-crypto-v1", None, False),
    ],
)
def test_is_legacy_crypto_row(model_version, created_at, expected) -> None:
    assert _is_legacy_crypto_row(model_version, created_at) is expected


def test_decode_legacy_json_object_flags_legacy_jsonb_strings_only() -> None:
    # psycopg2 hands back a jsonb string as str and a jsonb object as dict.
    text_overlay = json.dumps({"prices": PRICES})
    assert _decode_legacy_json_object(text_overlay, legacy_row=True) == ({"prices": PRICES}, True)
    assert _decode_legacy_json_object(text_overlay, legacy_row=False) == ({"prices": PRICES}, False)
    assert _decode_legacy_json_object({"prices": PRICES}, legacy_row=True) == ({"prices": PRICES}, False)
    assert _decode_legacy_json_object(None, legacy_row=True) == ({}, False)


def _detailed_row(target_symbols, overlay, mystical, grid, *,
                  model_version="grid-crypto-v1", created_at=BEFORE_CUTOFF) -> tuple:
    row = [None] * 31
    row[0] = "legacy-1"
    row[1] = created_at
    row[2] = NOW
    row[3], row[4], row[5] = "swing", "hybrid", "liquid_market"
    row[6] = target_symbols
    row[7:14] = ["q", "call", "timing", "setup", "inv", "note", ""]
    row[14], row[15], row[16] = overlay, mystical, grid
    row[17], row[18], row[19], row[20] = "astrogrid-v1", model_version, "live", "pending"
    row[26] = row[27] = []
    row[29] = row[30] = {}
    return tuple(row)


def _legacy_detailed_row(**kwargs) -> tuple:
    return _detailed_row(LEGACY_BTC, json.dumps({"prices": PRICES}), json.dumps({}),
                         json.dumps({"thesis": "neutral", "conviction": 0.4}), **kwargs)


def test_prediction_detail_decodes_legacy_row_and_flags_it() -> None:
    data = AstroGridStore(MagicMock())._prediction_row_to_dict(_legacy_detailed_row(), detailed=True)
    assert data["target_symbols"] == ["BTC"]
    assert data["market_overlay_snapshot"] == {"prices": PRICES}
    assert data["grid_feature_payload"] == {"thesis": "neutral", "conviction": 0.4}
    assert data["legacy_payload_decoded"] is True
    assert data["legacy_decoded_fields"] == [
        "target_symbols", "market_overlay_snapshot", "mystical_feature_payload", "grid_feature_payload",
    ]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"model_version": "astrogrid-oracle-v1"},  # same shape, other model (e.g. via the API)
        {"created_at": AFTER_CUTOFF},  # same shape, written after the fix
    ],
)
def test_prediction_detail_leaves_same_shape_on_non_legacy_rows(kwargs) -> None:
    data = AstroGridStore(MagicMock())._prediction_row_to_dict(_legacy_detailed_row(**kwargs), detailed=True)
    assert data["target_symbols"] == LEGACY_BTC
    assert data["legacy_payload_decoded"] is False
    assert data["legacy_decoded_fields"] == []


def test_prediction_list_row_uses_list_column_positions() -> None:
    detailed = list(_legacy_detailed_row())
    # The list query omits the three payload columns (indexes 14-16).
    list_row = tuple(detailed[:14] + detailed[17:30])
    data = AstroGridStore(MagicMock())._prediction_row_to_dict(list_row)
    assert data["model_version"] == "grid-crypto-v1"
    assert data["target_symbols"] == ["BTC"]
    assert data["legacy_decoded_fields"] == ["target_symbols"]


def test_prediction_detail_for_clean_row_is_not_flagged() -> None:
    row = _detailed_row(["BTC"], {"prices": PRICES}, {}, {"thesis": "neutral"})
    data = AstroGridStore(MagicMock())._prediction_row_to_dict(row, detailed=True)
    assert data["target_symbols"] == ["BTC"]
    assert data["legacy_payload_decoded"] is False
    assert data["legacy_decoded_fields"] == []


def _postmortem_row(target_symbols, model_version="grid-crypto-v1", created_at=BEFORE_CUTOFF) -> list:
    return ["legacy-1", created_at, "swing", "liquid_market", target_symbols, "call", "t", "inv",
            "pending", "pending", "summary", [], [], "inv", {}, model_version]


def test_postmortem_list_decodes_legacy_symbols() -> None:
    data = AstroGridStore(MagicMock())._postmortem_row_to_dict(_postmortem_row(LEGACY_BTC))
    assert data["target_symbols"] == ["BTC"]
    assert data["legacy_payload_decoded"] is True
    assert data["legacy_decoded_fields"] == ["target_symbols"]


@pytest.mark.parametrize(
    "row",
    [
        _postmortem_row(LEGACY_BTC, model_version="psi-oracle-v1"),
        _postmortem_row(LEGACY_BTC, created_at=AFTER_CUTOFF),
        _postmortem_row(["F", "T"]),
    ],
)
def test_postmortem_list_leaves_non_legacy_rows(row) -> None:
    data = AstroGridStore(MagicMock())._postmortem_row_to_dict(row)
    assert data["target_symbols"] == row[4]
    assert data["legacy_payload_decoded"] is False


def test_list_postmortems_selects_model_version_for_the_gate() -> None:
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = [_postmortem_row(LEGACY_BTC)]
    result = AstroGridStore(engine).list_postmortems()
    assert "pr.model_version" in str(conn.execute.call_args.args[0])
    assert result[0]["target_symbols"] == ["BTC"]
