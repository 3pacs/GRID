"""Executable examples for contract §5.6 and §6.7, as hash-sealed receipts that verify themselves (research only).

A receipt is built from records, sealed with its own sha256, and ``verify_receipt`` re-derives
every field it can from the evidence the receipt itself carries instead of trusting it. A D1
receipt embeds the filing and the closes it used and is verified by re-running the selection
path (#1457) on them; a D2 receipt embeds the ledger row and every quote, time, offset and leg
identity is rebound to that row's stored detail and to the permitted target windows, not only the
recomputed net. The hash must match. Four kinds:

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

from analysis import d1_ranking as R

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

D1_NOT_VERIFIED = ["materialization_row", "unavailable_reason"]


def _iso(t: Any) -> str | None:
    return t.isoformat() if isinstance(t, datetime) else None


def _from_iso(x: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(x) if isinstance(x, str) else None
    except ValueError:
        return None


def _d1_derive(evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Re-run the selection path on the embedded evidence and return the receipt fields it determines."""
    fe = evidence["filing"]
    filing = S.Filing(fe["accession"], tuple(fe["items"]), _from_iso(fe["acceptance"]), _from_iso(fe["retrieved_at"]),
                      _from_iso(fe["visible_at"]), fe["content_sha256"])
    tape = {date.fromisoformat(d): S.Price(v[0], _from_iso(v[1]), _from_iso(v[2])) for d, v in evidence["prices"].items()}
    bp = evidence["breakpoints"]
    breakpoints = None if bp is None else R.Breakpoints(R.OK, tuple(bp), None, 0)
    res = S.select_events([filing], tape, breakpoints=breakpoints)
    dec = res.decisions[0]
    if dec.status != S.SELECTED or len(res.selected) != 1:
        raise ReceiptError(f"re-running the selection on the receipt's evidence gives {dec.status}: {dec.reason}")
    ev = res.selected[0]
    if "2.02" not in {str(i).strip() for i in filing.items}:
        raise ReceiptError("the traceable example is an 8-K Item 2.02")
    if set(tape) != set(S.used_price_dates(ev)):
        raise ReceiptError("the embedded prices are not exactly the closes the event used")
    p_prev, c0 = tape[ev.p_prev_date].close, tape[ev.day0].close
    return {
        "family": ev.family,
        "clocks": {"day0": ev.day0.isoformat(), "p_prev_date": ev.p_prev_date.isoformat(),
                   "cutoff_o": ev.cutoff_o.isoformat(), "deadline_d": ev.deadline_d.isoformat(),
                   "entry_day": ev.entry_day.isoformat()},
        "card": {"already_moved": c0 / p_prev - 1.0, "claimed": False, "direction": ev.direction,
                 "strength": ev.strength, "quintile": ev.quintile,
                 "entry_open": {"day": ev.entry_day.isoformat(), "status": PENDING, "clock": "outcome"},
                 "outcome": {"h5": PENDING, "h20": PENDING, "sector_relative": PENDING, "spy_relative": PENDING}},
        "chain_input": {"table": "earnings_events", "accession": filing.accession,
                        "filing_date": filing.acceptance.astimezone(S.ET).date().isoformat(),
                        "acceptance": filing.acceptance.isoformat()},
        "retained_bytes_sha256": sha256_hex(canonical({"accession": filing.accession, "items": list(filing.items),
                                                       "acceptance": filing.acceptance.isoformat(),
                                                       "content_sha256": filing.content_sha256})),
    }


def d1_traceable_receipt(filing: S.Filing, tape: Mapping[date, S.Price], *,
                         breakpoints: R.Breakpoints | None = None,
                         materialization: Mapping[str, Any] | None = None,
                         unavailable_reason: str | None = None) -> dict[str, Any]:
    """§5.6 traceable: one selected Item 2.02 event, from acceptance to a card whose outcome slots are PENDING.

    The receipt embeds the filing and exactly the closes the event used, so ``verify_receipt`` can re-run the
    selection. The materialization row (or the unavailable reason) is carried, not verified.
    """
    if (materialization is None) == (unavailable_reason is None):
        raise ReceiptError("give exactly one of a materialization row or an unavailable reason (#958 chain shape)")
    res = S.select_events([filing], tape, breakpoints=breakpoints)
    if not res.selected or res.decisions[0].status != S.SELECTED:
        raise ReceiptError("the filing was not selected, so it has no traceable card")
    ev = res.selected[0]
    evidence = {"filing": {"accession": filing.accession, "items": list(filing.items),
                           "acceptance": _iso(filing.acceptance), "retrieved_at": _iso(filing.retrieved_at),
                           "visible_at": _iso(filing.visible_at), "content_sha256": filing.content_sha256},
                "prices": {d.isoformat(): [tape[d].close, _iso(tape[d].retrieved_at), _iso(tape[d].visible_at)]
                           for d in S.used_price_dates(ev)},
                "breakpoints": None if breakpoints is None or breakpoints.values is None else list(breakpoints.values)}
    derived = _d1_derive(evidence)
    chain = {"input": derived["chain_input"],
             "materialization_row": dict(materialization) if materialization is not None else None,
             "unavailable_reason": unavailable_reason, "retained_bytes_sha256": derived["retained_bytes_sha256"]}
    return seal(_base("D1/TRACEABLE", "positive", evidence=evidence, chain=chain, clocks=derived["clocks"],
                      card=derived["card"], family=derived["family"], not_verified=D1_NOT_VERIFIED,
                      note="M0 does not prove accuracy"))


# ----------------------------------------------------------------------------------------- D2 fly-shadow

WING = 5
FLY_ENTRY_TARGET, FLY_ENTRY_FWD_SECONDS = "15:00", 120   # fly_shadow.ENTRY_HM, ENTRY_FWD_MIN = 2: at or up to 2 min after
FLY_EXIT_TARGET, FLY_EXIT_BACK_SECONDS = "15:55", 180    # EXITS["x2"], EXIT_BACK_MIN = 3: at or up to 3 min before
CAPTURE_KEYS = {"id", "id_is"}


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
        if not (isinstance(q, (list, tuple)) and len(q) == 2
                and all(isinstance(x, (int, float)) and not isinstance(x, bool) for x in q)):
            raise ReceiptError(f"detail.{block}.{n} has no readable bid/ask")
        legs[n] = (float(q[0]), float(q[1]))
    return legs


def fly_net(entry: Mapping[str, tuple[float, float]], exit_: Mapping[str, tuple[float, float]]) -> tuple[float, float]:
    """(credit, net) per share from stored quotes: shorts sold at bid and bought back at ask, wings the reverse."""
    credit = round(entry["sc"][0] + entry["sp"][0] - entry["lc"][1] - entry["lp"][1], 4)
    cost = exit_["sc"][1] + exit_["sp"][1] - exit_["lc"][0] - exit_["lp"][0]
    return credit, round(credit - cost - 8 * FLY_FEE, 4)


def _aware_time(x: Any, what: str) -> datetime:
    t = datetime.fromisoformat(x) if isinstance(x, str) else None
    if t is None or t.tzinfo is None or t.utcoffset() is None:
        raise ReceiptError(f"{what} is missing or has no time zone")
    return t


def _fly_traced(row: Mapping[str, Any]) -> dict[str, Any]:
    """Every receipt field a traded ledger row determines. All of it is rebound to the row, never taken on trust."""
    r = _fly_row(row)
    if r["status"] != "traded" or r["x2"] is None:
        raise ReceiptError("only a traded session with a graded x2 has a traceable example")
    if not isinstance(r["day"], str):
        raise ReceiptError("the ledger row has no day")
    try:
        detail = json.loads(r["detail"])
    except (TypeError, ValueError) as exc:
        raise ReceiptError("detail is not readable JSON") from exc
    if not isinstance(detail, Mapping):
        raise ReceiptError("detail is not a mapping")
    entry, exit_ = _legs(detail, "in"), _legs(detail, "x2_out")
    k = r["k"]
    if not isinstance(k, (int, float)) or isinstance(k, bool):
        raise ReceiptError("the ledger row has no strike")
    want_legs = {"sc": "C%g" % k, "sp": "P%g" % k, "lc": "C%g" % (k + WING), "lp": "P%g" % (k - WING)}
    if detail.get("legs") != want_legs:
        raise ReceiptError(f"leg identity {detail.get('legs')!r} is not the ATM short pair with {WING}-point wings "
                           f"at {k:g} ({want_legs!r})")
    entry_t, exit_t = _aware_time(r["entry_t"], "entry_t"), _aware_time(detail.get("x2_t"), "x2_t")
    for what, t in (("entry", entry_t), ("exit", exit_t)):
        if t.astimezone(S.ET).date().isoformat() != r["day"]:
            raise ReceiptError(f"the {what} line is not on the session day {r['day']} (ET)")
    off_in = _offset_seconds(FLY_ENTRY_TARGET, r["entry_t"])
    off_out = _offset_seconds(FLY_EXIT_TARGET, detail["x2_t"])
    if not 0 <= off_in <= FLY_ENTRY_FWD_SECONDS:
        raise ReceiptError(f"the entry line is {off_in} s from {FLY_ENTRY_TARGET}; allowed 0..{FLY_ENTRY_FWD_SECONDS}")
    if not -FLY_EXIT_BACK_SECONDS <= off_out <= 0:
        raise ReceiptError(f"the exit line is {off_out} s from {FLY_EXIT_TARGET}; allowed -{FLY_EXIT_BACK_SECONDS}..0")
    credit, net = fly_net(entry, exit_)
    if (not isinstance(r["credit"], (int, float)) or not isinstance(r["x2"], (int, float))
            or abs(credit - float(r["credit"])) > NET_TOLERANCE or abs(net - float(r["x2"])) > NET_TOLERANCE):
        raise ReceiptError(f"stored credit/x2 ({r['credit']}/{r['x2']}) do not follow from the stored quotes "
                           f"({credit}/{net})")
    return {"session": r["day"], "regime": _regime(r["day"]),
            "quotes": {"entry": {"target": FLY_ENTRY_TARGET, "line_time": r["entry_t"], "line_offset_seconds": off_in,
                                 "legs": {n: list(q) for n, q in entry.items()}, "leg_ids": want_legs},
                       "exit": {"target": FLY_EXIT_TARGET, "line_time": detail["x2_t"], "line_offset_seconds": off_out,
                                "legs": {n: list(q) for n, q in exit_.items()}, "leg_ids": want_legs},
                       "leg_quote_time": f"{NOT_STORED}: the minute line's time stands in for each leg"},
            "fills": {"label": MODELED_BBO, "entry": "shorts sold at bid, wings bought at ask",
                      "exit": "shorts bought at ask, wings sold at bid", "fee_per_leg_side": FLY_FEE, "leg_sides": 8},
            "credit": credit, "net": net, "ledger_row_sha256": fly_row_sha256(r), "ledger_row": r}


def _fly_refused(row: Mapping[str, Any]) -> dict[str, Any]:
    r = _fly_row(row)
    if r["status"] not in ("skipped", "ungraded"):
        raise ReceiptError("a refusal example is a skipped or ungraded session")
    if not isinstance(r["reason"], str) or not r["reason"]:
        raise ReceiptError("the ledger row carries no exclusion reason")
    if not isinstance(r["day"], str):
        raise ReceiptError("the ledger row has no day")
    return {"session": r["day"], "regime": _regime(r["day"]), "status": r["status"], "reason_as_stored": r["reason"],
            "graded_value": None, "ledger_row_sha256": fly_row_sha256(r), "ledger_row": r}


def fly_traceable_receipt(row: Mapping[str, Any], *, capture_bytes: bytes | None = None) -> dict[str, Any]:
    """§6.7 traceable: one graded session; every field is rebound to the ledger row's stored detail."""
    capture = ({"id": sha256_hex(capture_bytes), "id_is": "sha256 of the capture file"} if capture_bytes is not None
               else {"id": NOT_STORED, "id_is": "the ledger carries no capture id; pass the capture file to name it"})
    return seal(_base("D2/TRACEABLE", "positive", capture=capture, **_fly_traced(row)))


def fly_refusal_receipt(row: Mapping[str, Any]) -> dict[str, Any]:
    """§6.7 refusal: one session the ledger did not grade, with its exclusion reason exactly as stored."""
    return seal(_base("D2/REFUSAL", "refusal", **_fly_refused(row)))


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
    except (KeyError, TypeError, ValueError, AttributeError, IndexError, OverflowError) as exc:
        errs.append(f"receipt is malformed: {type(exc).__name__}: {exc}")
    return errs


def _same(a: Any, b: Any) -> bool:
    try:
        return canonical(a) == canonical(b)
    except (TypeError, ValueError):
        return False


def _compare(rec: Mapping[str, Any], expected: Mapping[str, Any], extra: Collection[str]) -> list[str]:
    errs = [f"{k} does not match what its own evidence derives" for k in expected if not _same(rec.get(k), expected[k])]
    allowed = {"schema", "kind", "polarity", "receipt_sha256", *expected, *extra}
    errs += [f"unexpected field {k!r}" for k in rec if k not in allowed]
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
    chain = rec["chain"]
    if (chain["materialization_row"] is None) == (chain["unavailable_reason"] is None):
        errs.append("exactly one of a materialization row or an unavailable reason")
    if not _same(rec.get("not_verified"), D1_NOT_VERIFIED):
        errs.append("not_verified must name exactly the carried-not-verified fields")
    try:
        d = _d1_derive(rec["evidence"])
    except ReceiptError as exc:
        return errs + [str(exc)]
    expected = {"family": d["family"], "clocks": d["clocks"], "card": d["card"]}
    errs += _compare(rec, expected, ("evidence", "chain", "not_verified", "note"))
    if not _same(chain.get("input"), d["chain_input"]):
        errs.append("chain.input does not match the filing in the evidence")
    if chain.get("retained_bytes_sha256") != d["retained_bytes_sha256"]:
        errs.append("chain.retained_bytes_sha256 does not match the filing in the evidence")
    if set(chain) != {"input", "materialization_row", "unavailable_reason", "retained_bytes_sha256"}:
        errs.append("chain has the wrong fields")
    return errs


def _verify_d2_traceable(rec: Mapping[str, Any]) -> list[str]:
    errs = _compare(rec, _fly_traced(rec["ledger_row"]), ("capture",))
    cap = rec["capture"]
    sha = isinstance(cap, Mapping) and isinstance(cap.get("id"), str) and len(cap["id"]) == 64 and all(
        c in "0123456789abcdef" for c in cap["id"])
    if not isinstance(cap, Mapping) or set(cap) != CAPTURE_KEYS or not (cap["id"] == NOT_STORED or sha):
        errs.append("capture must name a sha256 or say NOT_STORED (a capture id cannot be re-derived from the ledger)")
    return errs


def _verify_d2_refusal(rec: Mapping[str, Any]) -> list[str]:
    return _compare(rec, _fly_refused(rec["ledger_row"]), ())
