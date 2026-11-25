"""Executable examples for contract §5.6 and §6.7, as hash-sealed receipts that verify themselves (research only).

A receipt is built from records, sealed with its own sha256, and ``verify_receipt`` re-derives
what it claims instead of trusting it: the day-0 clocks are recomputed from the acceptance
datetime, the net of a fly session is recomputed from its stored quotes, outcome slots must be
``PENDING``, and the hash must match. Four kinds:

* ``D1/TRACEABLE`` (§5.6): 8-K Item 2.02 -> acceptance datetime -> entry open -> card with "already
  moved", every outcome slot PENDING, bound to the #958 chain shape (input, materialization row or
  unavailable reason, retained bytes). M0 does not prove accuracy.
* ``D1/REFUSAL`` (§5.6): an ``earnings_calendar`` compat-stub row (``earnings_date`` equals a SEC
  ``period_end``, ``reported`` true) refused as ``EXCLUDED_UNKNOWN_AVAILABILITY``. Nothing is
  deleted, rewritten or quarantined: the builder reads one row and returns one receipt.
* ``D2/TRACEABLE`` (§6.7): one graded ``fly-shadow/1`` session: capture id, entry and exit quotes
  with their line times and offsets from 15:00 / 15:55, fills labelled ``MODELED_BBO``, the net, and
  the ledger row hash.
* ``D2/REFUSAL`` (§6.7): one session the ledger did not grade, with the exclusion reason as stored.

What the ledger does not store is said, not invented: it carries no chain capture id (the id is the
sha256 of the capture file when given, else ``NOT_STORED``), no per-leg quote time (the minute
line's time stands in and is labelled so), and no row hash (computed here over the stored columns).

These builders read records handed to them. They open no ledger, database or network connection.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Any, Collection, Mapping

from analysis import d1_selection as S

RECEIPT_SCHEMA = "d0-example-receipt/1"
PENDING = "PENDING"
MODELED_BBO = "MODELED_BBO"
NOT_STORED = "NOT_STORED"
KINDS = ("D1/TRACEABLE", "D1/REFUSAL", "D2/TRACEABLE", "D2/REFUSAL")
FLY_VERSION = "fly-shadow/1"
FLY_FEE = 0.0003  # per leg-side, the fly-shadow/1 registration value
FLY_COLS = ("day", "version", "status", "reason", "graded_at", "entry_t", "spot_in", "k", "credit", "x1", "x2", "x3",
            "settle", "detail")
FLY_JUDGED_FROM = "2026-11-12"
NET_TOLERANCE = 1e-4
STUB_NOTE = ("2,131 rows is the author's measurement from 2026-11-12; date equality alone does not prove every "
             "matching row is a stub, so no row is deleted, rewritten or quarantined on that count")


class ReceiptError(ValueError):
    """The record handed to a builder cannot make the receipt it is for."""


def canonical(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def seal(body: dict[str, Any]) -> dict[str, Any]:
    body = {k: v for k, v in body.items() if k != "receipt_sha256"}
    return {**body, "receipt_sha256": sha256_hex(canonical(body))}


def _base(kind: str, polarity: str, **fields: Any) -> dict[str, Any]:
    return {"schema": RECEIPT_SCHEMA, "kind": kind, "polarity": polarity, **fields}


# ----------------------------------------------------------------------------------------- D1 refusal

def calendar_stub_refusal(row: Mapping[str, Any], sec_period_ends: Collection[str]) -> dict[str, Any]:
    """§5.6 refusal: one compat-stub ``earnings_calendar`` row, refused. Raises if the row is not that shape."""
    if not isinstance(row, Mapping) or row.get("reported") is not True:
        raise ReceiptError("the row is not reported = TRUE")
    ed = row.get("earnings_date")
    if not isinstance(ed, str) or ed not in set(sec_period_ends):
        raise ReceiptError("earnings_date does not equal a SEC period_end")
    return seal(_base("D1/REFUSAL", "refusal", refused_as=S.EXCLUDED_UNKNOWN_AVAILABILITY,
                      row={"ticker": row.get("ticker"), "earnings_date": ed, "reported": True},
                      matched_sec_period_end=ed, rows_changed=0, note=STUB_NOTE))


# ----------------------------------------------------------------------------------------- D1 traceable

def d1_traceable_receipt(filing: S.Filing, result: S.SelectionResult, tape: Mapping[date, S.Price], *,
                         materialization: Mapping[str, Any] | None = None,
                         unavailable_reason: str | None = None) -> dict[str, Any]:
    """§5.6 traceable: one selected Item 2.02 event, from acceptance to a card whose outcome slots are PENDING."""
    if (materialization is None) == (unavailable_reason is None):
        raise ReceiptError("give exactly one of a materialization row or an unavailable reason (#958 chain shape)")
    ev = next((e for e in result.selected if e.accession == filing.accession), None)
    dec = next((d for d in result.decisions if d.accession == filing.accession), None)
    if ev is None or dec is None or dec.status != S.SELECTED:
        raise ReceiptError("the filing was not selected, so it has no traceable card")
    if "2.02" not in {str(i).strip() for i in filing.items}:
        raise ReceiptError("the traceable example is an 8-K Item 2.02")
    p_prev, c0 = tape[ev.p_prev_date].close, tape[ev.day0].close
    chain = {"input": {"table": "earnings_events", "accession": filing.accession,
                       "filing_date": filing.acceptance.astimezone(S.ET).date().isoformat(),
                       "acceptance": filing.acceptance.isoformat()},
             "materialization_row": dict(materialization) if materialization is not None else None,
             "unavailable_reason": unavailable_reason,
             "retained_bytes_sha256": sha256_hex(canonical({"accession": filing.accession,
                                                            "items": list(filing.items),
                                                            "acceptance": filing.acceptance.isoformat(),
                                                            "content_sha256": filing.content_sha256}))}
    card = {"already_moved": c0 / p_prev - 1.0, "claimed": False, "direction": ev.direction,
            "strength": ev.strength, "quintile": ev.quintile,
            "entry_open": {"day": ev.entry_day.isoformat(), "status": PENDING, "clock": "outcome"},
            "outcome": {"h5": PENDING, "h20": PENDING, "sector_relative": PENDING, "spy_relative": PENDING}}
    clocks = {"day0": ev.day0.isoformat(), "p_prev_date": ev.p_prev_date.isoformat(), "cutoff_o": ev.cutoff_o.isoformat(),
              "deadline_d": ev.deadline_d.isoformat(), "entry_day": ev.entry_day.isoformat()}
    return seal(_base("D1/TRACEABLE", "positive", chain=chain, clocks=clocks, card=card, family=ev.family,
                      note="M0 does not prove accuracy"))


# ----------------------------------------------------------------------------------------- D2 fly-shadow

def _fly_row(row: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(row, Mapping) or row.get("version") != FLY_VERSION:
        raise ReceiptError(f"not a {FLY_VERSION} ledger row")
    return {c: row.get(c) for c in FLY_COLS}


def fly_row_sha256(row: Mapping[str, Any]) -> str:
    return sha256_hex(canonical(_fly_row(row)))


def _regime(day: str) -> str:
    return "PRE_REGISTRATION (outside the judged sample)" if day < FLY_JUDGED_FROM else "JUDGED_SAMPLE"


def _offset_seconds(target_hm: str, line_iso: str) -> int:
    t = datetime.fromisoformat(line_iso)
    h, m = int(target_hm[:2]), int(target_hm[3:])
    target = t.astimezone(S.ET).replace(hour=h, minute=m, second=0, microsecond=0)
    return int((t - target).total_seconds())


def _legs(detail: Mapping[str, Any], block: str) -> dict[str, tuple[float, float]]:
    out = detail.get(block)
    if not isinstance(out, Mapping) or set(out) != {"sc", "sp", "lc", "lp"}:
        raise ReceiptError(f"detail.{block} must hold the four legs sc, sp, lc, lp")
    legs = {}
    for n, q in out.items():
        if not (isinstance(q, (list, tuple)) and len(q) == 2 and all(isinstance(x, (int, float)) for x in q)):
            raise ReceiptError(f"detail.{block}.{n} has no readable bid/ask")
        legs[n] = (float(q[0]), float(q[1]))
    return legs


def fly_net(entry: Mapping[str, tuple[float, float]], exit_: Mapping[str, tuple[float, float]]) -> tuple[float, float]:
    """(credit, net) per share from stored quotes: shorts sold at bid and bought back at ask, wings the reverse."""
    credit = round(entry["sc"][0] + entry["sp"][0] - entry["lc"][1] - entry["lp"][1], 4)
    cost = exit_["sc"][1] + exit_["sp"][1] - exit_["lc"][0] - exit_["lp"][0]
    return credit, round(credit - cost - 8 * FLY_FEE, 4)


def fly_traceable_receipt(row: Mapping[str, Any], *, capture_bytes: bytes | None = None) -> dict[str, Any]:
    """§6.7 traceable: one graded session, with the net re-derived from its stored quotes."""
    r = _fly_row(row)
    if r["status"] != "traded" or r["x2"] is None:
        raise ReceiptError("only a traded session with a graded x2 has a traceable example")
    try:
        detail = json.loads(r["detail"])
    except (TypeError, ValueError) as exc:
        raise ReceiptError("detail is not readable JSON") from exc
    entry, exit_ = _legs(detail, "in"), _legs(detail, "x2_out")
    credit, net = fly_net(entry, exit_)
    if abs(credit - float(r["credit"])) > NET_TOLERANCE or abs(net - float(r["x2"])) > NET_TOLERANCE:
        raise ReceiptError(f"stored credit/x2 ({r['credit']}/{r['x2']}) do not follow from the stored quotes "
                           f"({credit}/{net})")
    quotes = {"entry": {"target": "15:00", "line_time": r["entry_t"], "line_offset_seconds":
                        _offset_seconds("15:00", r["entry_t"]), "legs": {n: list(q) for n, q in entry.items()}},
              "exit": {"target": "15:55", "line_time": detail.get("x2_t"), "line_offset_seconds":
                       _offset_seconds("15:55", detail["x2_t"]), "legs": {n: list(q) for n, q in exit_.items()}},
              "leg_quote_time": f"{NOT_STORED}: the minute line's time stands in for each leg"}
    fills = {"label": MODELED_BBO, "entry": "shorts sold at bid, wings bought at ask",
             "exit": "shorts bought at ask, wings sold at bid", "fee_per_leg_side": FLY_FEE, "leg_sides": 8}
    capture = ({"id": sha256_hex(capture_bytes), "id_is": "sha256 of the capture file"} if capture_bytes is not None
               else {"id": NOT_STORED, "id_is": "the ledger carries no capture id; pass the capture file to name it"})
    return seal(_base("D2/TRACEABLE", "positive", session=r["day"], regime=_regime(r["day"]), capture=capture,
                      quotes=quotes, fills=fills, credit=credit, net=net, ledger_row_sha256=fly_row_sha256(r),
                      ledger_row=r))


def fly_refusal_receipt(row: Mapping[str, Any]) -> dict[str, Any]:
    """§6.7 refusal: one session the ledger did not grade, with its exclusion reason exactly as stored."""
    r = _fly_row(row)
    if r["status"] not in ("skipped", "ungraded"):
        raise ReceiptError("a refusal example is a skipped or ungraded session")
    if not isinstance(r["reason"], str) or not r["reason"]:
        raise ReceiptError("the ledger row carries no exclusion reason")
    return seal(_base("D2/REFUSAL", "refusal", session=r["day"], regime=_regime(r["day"]), status=r["status"],
                      reason_as_stored=r["reason"], graded_value=None, ledger_row_sha256=fly_row_sha256(r),
                      ledger_row=r))


# ----------------------------------------------------------------------------------------- verification

def verify_receipt(rec: Any) -> list[str]:
    """Why a receipt does not verify (empty = it does). Re-derives what it can; it does not trust its own fields."""
    if not isinstance(rec, Mapping):
        return ["receipt is not a mapping"]
    errs: list[str] = []
    if rec.get("schema") != RECEIPT_SCHEMA:
        errs.append(f"schema is not {RECEIPT_SCHEMA}")
    kind = rec.get("kind")
    if kind not in KINDS:
        return errs + [f"kind {kind!r} is not one of {KINDS}"]
    if rec.get("polarity") != ("refusal" if kind.endswith("REFUSAL") else "positive"):
        errs.append("polarity does not match the kind")
    body = {k: v for k, v in rec.items() if k != "receipt_sha256"}
    try:
        if rec.get("receipt_sha256") != sha256_hex(canonical(body)):
            errs.append("receipt_sha256 does not match the receipt body")
    except (TypeError, ValueError):
        return errs + ["receipt body is not canonical JSON"]
    try:
        errs += {"D1/REFUSAL": _verify_d1_refusal, "D1/TRACEABLE": _verify_d1_traceable,
                 "D2/TRACEABLE": _verify_d2_traceable, "D2/REFUSAL": _verify_d2_refusal}[kind](rec)
    except (KeyError, TypeError, ValueError, AttributeError, IndexError) as exc:
        errs.append(f"receipt is malformed: {type(exc).__name__}: {exc}")
    return errs


def _verify_d1_refusal(rec: Mapping[str, Any]) -> list[str]:
    errs = []
    if rec["refused_as"] != S.EXCLUDED_UNKNOWN_AVAILABILITY:
        errs.append("a compat-stub row is refused as EXCLUDED_UNKNOWN_AVAILABILITY")
    if rec["rows_changed"] != 0:
        errs.append("no row may be changed on this count")
    if rec["row"]["earnings_date"] != rec["matched_sec_period_end"] or rec["row"]["reported"] is not True:
        errs.append("the row is not a compat stub (earnings_date must equal the SEC period_end, reported TRUE)")
    return errs


def _verify_d1_traceable(rec: Mapping[str, Any]) -> list[str]:
    errs = []
    chain, clocks, card = rec["chain"], rec["clocks"], rec["card"]
    if (chain["materialization_row"] is None) == (chain["unavailable_reason"] is None):
        errs.append("exactly one of a materialization row or an unavailable reason")
    a = datetime.fromisoformat(chain["input"]["acceptance"])
    if a.tzinfo is None:
        errs.append("acceptance has no time zone")
        return errs
    cal = S.RegularCalendar()
    day0 = S.day0_for(a, cal)
    want = {"day0": day0.isoformat(), "p_prev_date": S.prev_close_date(a, cal).isoformat(),
            "entry_day": S.next_session(day0, cal).isoformat(),
            "deadline_d": datetime.combine(day0, S.DEADLINE_ET, tzinfo=S.ET).isoformat()}
    for k, v in want.items():
        if clocks.get(k) != v:
            errs.append(f"clocks.{k} {clocks.get(k)!r} is not what the acceptance gives ({v!r})")
    if chain["input"]["filing_date"] != a.astimezone(S.ET).date().isoformat():
        errs.append("filing_date is not the acceptance's date in ET")
    if set(card["outcome"].values()) != {PENDING} or card["entry_open"]["status"] != PENDING:
        errs.append("every outcome slot, the entry open included, must be PENDING")
    if card["claimed"] is not False:
        errs.append("the day-0 move is recorded as 'already moved' and never claimed")
    return errs


def _verify_d2_traceable(rec: Mapping[str, Any]) -> list[str]:
    errs = []
    q = rec["quotes"]
    entry = {n: tuple(v) for n, v in q["entry"]["legs"].items()}
    exit_ = {n: tuple(v) for n, v in q["exit"]["legs"].items()}
    credit, net = fly_net(entry, exit_)
    if abs(credit - rec["credit"]) > NET_TOLERANCE or abs(net - rec["net"]) > NET_TOLERANCE:
        errs.append(f"credit/net {rec['credit']}/{rec['net']} do not follow from the quotes ({credit}/{net})")
    if rec["fills"]["label"] != MODELED_BBO:
        errs.append("fills must be labelled MODELED_BBO")
    if rec["ledger_row_sha256"] != fly_row_sha256(rec["ledger_row"]):
        errs.append("ledger_row_sha256 does not match the ledger row")
    if rec["ledger_row"]["status"] != "traded" or abs(float(rec["ledger_row"]["x2"]) - rec["net"]) > NET_TOLERANCE:
        errs.append("the ledger row is not the traded session this net belongs to")
    if rec["regime"] != _regime(rec["session"]):
        errs.append("regime does not match the session day")
    return errs


def _verify_d2_refusal(rec: Mapping[str, Any]) -> list[str]:
    errs = []
    row = rec["ledger_row"]
    if rec["ledger_row_sha256"] != fly_row_sha256(row):
        errs.append("ledger_row_sha256 does not match the ledger row")
    if rec["reason_as_stored"] != row["reason"] or not rec["reason_as_stored"]:
        errs.append("reason_as_stored must equal the ledger row's reason, verbatim and non-empty")
    if row["status"] not in ("skipped", "ungraded") or rec["graded_value"] is not None:
        errs.append("a refused session carries no graded value")
    return errs
