"""R3 (#1085): NVIDIA commitment-to-valuation evidence ledger (research artifact only).

The ledger is a JSON Lines file of primary-source records (one per disclosed
number or commitment), frozen before NVIDIA's Q3 FY2027 report so that what was
public beforehand is fixed. This module:

* validates every record (enums, finite numbers, verbatim quotes for primary
  numbers, nothing published after the freeze cutoff, unique ids);
* deduplicates overlapping announcements (one ``counted`` record per
  ``dedup_group``) and splits the commitment stack by stage and basis, so
  whole-project spend and counterparty capex are never added to NVIDIA revenue;
* resolves bear/base/bull scenarios whose inputs are either an ``observed``
  ledger id or an explicit ``assumption`` with a written reason, and computes a
  per-share value and its sensitivity to a dated price/share/net-cash basis.

Nothing here reads the database, calls a provider, trades or forecasts. A
scenario is a conditional calculation, not a forecast. A missing observed input
makes the scenario UNAVAILABLE rather than being filled in.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import sys
from typing import Any, Iterable

REPO = Path(__file__).resolve().parents[1]
SCHEMA = "r3-nvda-ledger/0"
LEDGER_PATH = Path("docs/research/r3-nvda/ledger-v2.jsonl")
SCENARIOS_PATH = Path("docs/research/r3-nvda/scenarios-v2.json")
#: v0 (frozen 2026-11-13, superseded by v1 after the independent review on #1150)
#: stays in the repo unchanged as the record of what was frozen first.
V0_LEDGER_PATH = Path("docs/research/r3-nvda/ledger-v0.jsonl")
#: Records must have become public strictly before this instant. NVIDIA's
#: Q3 FY2027 release is scheduled after the 2026-11-17 close (~21:20Z).
FREEZE_CUTOFF = "2026-11-17T21:00:00+00:00"
#: v2 pins: sha256 of the canonical v2 ledger bytes and the build instant. No
#: record may claim a retrieval after LEDGER_BUILT_AT. Any change after the freeze is v3.
FROZEN_LEDGER_SHA256: str | None = "c593b3975239a1c0ab6eff6acc4fd812f8b1f6d6c16536a6d8b11df3ff6c5dc6"
LEDGER_BUILT_AT: str | None = "2026-11-13T04:33:42Z"
FROZEN_AT = "2026-11-13"
#: The other frozen v2 artifacts (raw-byte sha256), checked by the tests.
FROZEN_FILES: dict[str, str] = {
    "docs/research/r3-nvda/scenarios-v2.json": "19215c253633e570dff8322da8abfc1ec95e1aabbec25d78102ceaa70133bb88",
    "docs/research/r3-nvda/health-v2.json": "f5b8dbb0c1088a4224e1bf855851548ad9b39c8fb37fffa938c3e012824684ed",
    "docs/research/r3-nvda/synthesis-v2.md": "ba9df7bd9787acaeef63d86444e0b9d5d993ab0088ff85338f9780d88ef7ca3b",
    "docs/research/r3-nvda/health-rubric-v0.md": "5e11a86e02cef71a14be3abb403ffe04c35c9011d911e52f74b1b926984c909c",
}
#: v1 artifacts (raw-byte sha256), superseded by v2 after the re-review on #1150.
V1_FROZEN_FILES: dict[str, str] = {
    "docs/research/r3-nvda/ledger-v1.jsonl": "fdbe3cb66db6aab3c8ce0969b2891bb5a7b3ecf98f8cf608714ae723426a8e06",
    "docs/research/r3-nvda/scenarios-v1.json": "e5564229cb5f1a8d4797f4890aa8699c2129621e50546163e6f7613a89b3ebea",
    "docs/research/r3-nvda/health-v1.json": "ad7a7b6cd03a2bd315a80562c6a1fafaedc311fcaf2e8b59249c7531bb95a1d0",
    "docs/research/r3-nvda/synthesis-v1.md": "9b3262acd991cdb5d155c440dc3d6317644a1e686a65a046e15964499abfe4d8",
}
V1_LEDGER_PATH = Path("docs/research/r3-nvda/ledger-v1.jsonl")
#: v0 artifacts (raw-byte sha256). They must never change: they are the record of
#: the first freeze, superseded by v1 after the review on #1150.
V0_FROZEN_FILES: dict[str, str] = {
    "docs/research/r3-nvda/ledger-v0.jsonl": "484f39994d26e9e2cf4185b686edbf58f79b8ed09258d84f34146b91dab9e23c",
    "docs/research/r3-nvda/scenarios-v0.json": "26df5078b631c0ffa8c18befcb14bd4053ecbf66dc8fe5e87f29e82c6b70e2d7",
    "docs/research/r3-nvda/health-v0.json": "35f5e4ffd337c7d4c69e7d518421528816a3b8af133658d2b5d7cd0e12c8bad4",
    "docs/research/r3-nvda/synthesis-v0.md": "b599c3c05668bd580ee34067b7715ffcca528b4aa91c269e19e6bc7325a7fe06",
    "docs/research/r3-nvda/health-rubric-v0.md": "5e11a86e02cef71a14be3abb403ffe04c35c9011d911e52f74b1b926984c909c",
}

RECORD_KINDS = ("commitment", "financial_fact", "event")
STAGES = (
    "announced_intention",
    "capacity_plan",
    "purchase_commitment",
    "order_backlog",
    "delivery",
    "recognized_revenue",
    "cash_receipt",
    "cancellation",
    "nvidia_investment",
    "nvidia_obligation",
    "financing",
    "financial_fact",
    "governance",
)
DIRECTIONS = ("demand", "supply", "infrastructure", "financing", "nvidia_financials", "governance")
BASES = (
    "nvidia_revenue",
    "nvidia_obligation",
    "nvidia_investment",
    "counterparty_capex",
    "whole_project",
    "counterparty_commitment_to_nvidia",
    "balance_sheet",
    "cash_flow",
    "unknown",
)
CIRCULARITY = (
    "none",
    "nvidia_investment_in_customer",
    "vendor_financing",
    "capacity_backstop",
    "lease_guarantee",
    "unknown",
)
CONFIDENCE = ("primary_verbatim", "primary_paraphrase", "secondary_only")
UNITS = ("billions", "millions", "GW", "units", "percent", None)
#: How binding a counted commitment is (v2). "partly_cancellable": the filer says
#: some of it may be cancelled "in certain instances", share not disclosed.
#: "post_balance" is a timing label (agreed after the balance date), kept apart.
FIRMNESS = ("firm", "partly_cancellable", "contingent", "post_balance")
HEX = set("0123456789abcdef")
ISO_PREFIX = re.compile(r"^\d{4}-\d{2}-\d{2}(T|$)")
#: Bases whose dollars can be NVIDIA's (demand on NVIDIA, or NVIDIA's own
#: obligations). Whole-project spend and counterparty capex are context only.
NVIDIA_DOLLAR_BASES = (
    "nvidia_revenue",
    "nvidia_obligation",
    "nvidia_investment",
    "counterparty_commitment_to_nvidia",
)
REQUIRED = (
    "id",
    "record_kind",
    "stage",
    "direction",
    "issuer",
    "counterparty",
    "source",
    "fiscal_period",
    "amount",
    "timing",
    "conditions",
    "contradictions",
    "circularity",
    "dedup_group",
    "counted",
    "unknowns",
    "confidence",
)
SOURCE_REQUIRED = (
    "url",
    "doc_type",
    "accession",
    "section",
    "quote",
    "published_at",
    "retrieved_at",
    "retrieved_sha256",
)


def _parse_ts(value: str, *, end_of_day: bool = False) -> datetime:
    """Parse an ISO date or datetime. A date-only value means midnight, or 23:59:59 with ``end_of_day``."""
    text = value.strip()
    if not ISO_PREFIX.match(text):
        raise ValueError(f"not an ISO YYYY-MM-DD date/time: {value!r}")
    if len(text) == 10:
        text += "T23:59:59+00:00" if end_of_day else "T00:00:00+00:00"
    text = text.replace("Z", "+00:00")
    ts = datetime.fromisoformat(text)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def _finite(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def canonical(record: dict[str, Any]) -> str:
    return json.dumps(record, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def canonical_bytes(records: Iterable[dict[str, Any]]) -> bytes:
    return "".join(canonical(r) + "\n" for r in records).encode("utf-8")


def ledger_sha256(records: Iterable[dict[str, Any]]) -> str:
    return hashlib.sha256(canonical_bytes(records)).hexdigest()


def load_ledger(path: Path) -> list[dict[str, Any]]:
    records = []
    for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise ValueError(f"{path}:{n}: not JSON: {exc}") from exc
    return records


def validate_record(r: dict[str, Any], cutoff: str = FREEZE_CUTOFF, built_at: str | None = None) -> list[str]:
    rid = r.get("id", "<no id>")
    errs: list[str] = []
    missing = [k for k in REQUIRED if k not in r]
    if missing:
        return [f"{rid}: missing fields {missing}"]
    if not isinstance(r["id"], str) or not r["id"]:
        errs.append(f"{rid}: id must be a non-empty string")
    for field, allowed in (
        ("record_kind", RECORD_KINDS),
        ("stage", STAGES),
        ("direction", DIRECTIONS),
        ("circularity", CIRCULARITY),
        ("confidence", CONFIDENCE),
    ):
        if r[field] not in allowed:
            errs.append(f"{rid}: {field}={r[field]!r} not in {allowed}")
    if not isinstance(r["counted"], bool):
        errs.append(f"{rid}: counted must be true/false")
    for field in ("conditions", "contradictions", "unknowns"):
        if not isinstance(r[field], list):
            errs.append(f"{rid}: {field} must be a list")

    src = r["source"]
    if not isinstance(src, dict):
        return errs + [f"{rid}: source must be an object"]
    smissing = [k for k in SOURCE_REQUIRED if k not in src]
    if smissing:
        errs.append(f"{rid}: source missing {smissing}")
        return errs
    if not isinstance(src["url"], str) or not src["url"].startswith("https://"):
        errs.append(f"{rid}: source.url must be an https URL")
    secondary_uncounted = r["confidence"] == "secondary_only" and r["counted"] is False
    pub = src["published_at"]
    pub_ts = None
    if pub is None:
        if not secondary_uncounted:
            errs.append(f"{rid}: published_at may be null only on an uncounted secondary_only record")
    else:
        try:
            if not isinstance(pub, str):
                raise TypeError("published_at must be a string")
            # A date-only publication counts as the end of that day (it could be any time on it).
            if _parse_ts(pub, end_of_day=True) >= _parse_ts(cutoff):
                errs.append(f"{rid}: published_at {pub} is not before the freeze cutoff {cutoff}")
            pub_ts = _parse_ts(pub)
        except (TypeError, ValueError):
            errs.append(f"{rid}: published_at {pub!r} is not an ISO date/time")
    ret = src["retrieved_at"]
    if ret is None:
        if not secondary_uncounted:
            errs.append(f"{rid}: retrieved_at may be null only on an uncounted secondary_only record")
    else:
        try:
            if not isinstance(ret, str):
                raise TypeError("retrieved_at must be a string")
            ret_ts = _parse_ts(ret, end_of_day=True)
            if built_at is not None and _parse_ts(ret, end_of_day=True) > _parse_ts(built_at):
                errs.append(f"{rid}: retrieved_at {ret} is after the ledger build time {built_at}")
            if pub_ts is not None and ret_ts < pub_ts:
                errs.append(f"{rid}: retrieved_at {ret} is before published_at {pub}")
        except (TypeError, ValueError):
            errs.append(f"{rid}: retrieved_at {ret!r} is not an ISO date/time")
    sha = src["retrieved_sha256"]
    if sha is None and ret is not None:
        errs.append(f"{rid}: a retrieval time needs the sha256 of the retrieved bytes (no bytes kept -> null)")
    if sha is not None and (not isinstance(sha, str) or len(sha) != 64 or not set(sha) <= HEX):
        errs.append(f"{rid}: retrieved_sha256 must be 64 lowercase hex chars or null")

    amt = r["amount"]
    if not isinstance(amt, dict) or not {"value", "currency", "units", "basis"} <= set(amt):
        return errs + [f"{rid}: amount must have value/currency/units/basis"]
    if amt["value"] is not None and not _finite(amt["value"]):
        errs.append(f"{rid}: amount.value must be a finite number or null (got {amt['value']!r})")
    if amt["units"] not in UNITS:
        errs.append(f"{rid}: amount.units={amt['units']!r} not in {UNITS}")
    if amt["basis"] not in BASES:
        errs.append(f"{rid}: amount.basis={amt['basis']!r} not in {BASES}")
    if amt["value"] is not None and r["confidence"] == "primary_verbatim":
        if not isinstance(src["quote"], str) or len(src["quote"].strip()) < 8:
            errs.append(f"{rid}: a primary_verbatim number needs its verbatim quote")
    if r["counted"] and (amt["value"] is None or r["confidence"] == "secondary_only"):
        errs.append(f"{rid}: only a primary record with a value can be counted")
    if r["counted"] and _finite(amt["value"]) and amt["value"] < 0:
        errs.append(f"{rid}: a counted commitment cannot be negative")
    if r["counted"] and src.get("retrieved_sha256") is None:
        errs.append(f"{rid}: a counted record needs the sha256 of its source bytes")
    if r["counted"] and r.get("firmness") not in FIRMNESS:
        errs.append(f"{rid}: a counted record needs firmness in {FIRMNESS}")
    total_of = r.get("total_of")
    if total_of is not None and (not isinstance(total_of, list) or not all(isinstance(x, str) for x in total_of)):
        errs.append(f"{rid}: total_of must be a list of record ids")
    return errs


def validate_ledger(
    records: list[dict[str, Any]], cutoff: str = FREEZE_CUTOFF, built_at: str | None = None
) -> list[str]:
    errs: list[str] = []
    seen: set[str] = set()
    for r in records:
        errs.extend(validate_record(r, cutoff, built_at))
        rid = r.get("id")
        if rid in seen:
            errs.append(f"{rid}: duplicate id")
        seen.add(rid)
    groups: dict[str, list[dict[str, Any]]] = {}
    for r in records:
        if r.get("dedup_group"):
            groups.setdefault(r["dedup_group"], []).append(r)
    for g, members in sorted(groups.items()):
        counted = [m["id"] for m in members if m.get("counted")]
        if len(counted) > 1:
            errs.append(f"dedup_group {g}: {len(counted)} counted records {counted}; at most one may count")
    by_id = {r.get("id"): r for r in records}
    for r in records:
        for comp in r.get("total_of") or []:
            if comp not in by_id:
                errs.append(f"{r.get('id')}: total_of names unknown record {comp}")
            elif r.get("counted") and by_id[comp].get("counted"):
                errs.append(f"{r.get('id')}: a total and its component {comp} are both counted")
        comps = [by_id[c] for c in (r.get("total_of") or []) if c in by_id]
        if comps and len(comps) == len(r.get("total_of") or []):
            tot = _to_billions(r["amount"]) if isinstance(r.get("amount"), dict) else None
            parts = [_to_billions(c["amount"]) for c in comps]
            if tot is not None and all(x is not None for x in parts) and abs(tot - sum(parts)) > 0.5:
                errs.append(f"{r.get('id')}: total {tot} != sum of its components {sum(parts)}")
    return errs


def _to_billions(amount: dict[str, Any]) -> float | None:
    if amount.get("currency") != "USD" or not _finite(amount.get("value")):
        return None
    if amount["units"] == "billions":
        return float(amount["value"])
    if amount["units"] == "millions":
        return float(amount["value"]) / 1000.0
    return None


def commitment_stack(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Counted commitment dollars by stage x basis (USD bn), plus what was left out and why."""
    stack: dict[str, dict[str, float]] = {}
    by_circ: dict[str, float] = {}
    by_firm: dict[str, float] = {}
    excluded: list[dict[str, str]] = []
    for r in records:
        if r["record_kind"] != "commitment":
            continue
        basis = r["amount"]["basis"]
        if not r["counted"]:
            excluded.append({"id": r["id"], "why": "not counted (duplicate, unvalued or secondary)"})
            continue
        if basis not in NVIDIA_DOLLAR_BASES:
            excluded.append({"id": r["id"], "why": f"basis {basis} is not NVIDIA dollars (context only)"})
            continue
        bn = _to_billions(r["amount"])
        if bn is None:
            excluded.append({"id": r["id"], "why": "not a USD amount in millions/billions"})
            continue
        stack.setdefault(r["stage"], {}).setdefault(basis, 0.0)
        stack[r["stage"]][basis] += bn
        by_circ.setdefault(r["circularity"], 0.0)
        by_circ[r["circularity"]] += bn
        firm = r.get("firmness") or "unspecified"
        by_firm.setdefault(firm, 0.0)
        by_firm[firm] += bn
    circular = sorted(
        r["id"] for r in records if r["record_kind"] == "commitment" and r["circularity"] not in ("none", "unknown")
    )
    return {
        "stack_usd_bn": stack,
        "counted_by_circularity_usd_bn": by_circ,
        "counted_by_firmness_usd_bn": by_firm,
        # A sum across firm, cancellable, contingent and post-balance lines has no
        # single meaning; read it only next to counted_by_firmness_usd_bn.
        "counted_total_usd_bn": sum(by_circ.values()),
        "excluded": excluded,
        "circular_ids": circular,
    }


# --------------------------------------------------------------------------- scenarios


class Unavailable(Exception):
    """An observed scenario input is missing or not usable; the scenario is not computed."""


@dataclass(frozen=True)
class Resolved:
    value: float
    origin: str  # "observed:<id>" or "assumption"


def _index(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {r["id"]: r for r in records}


def resolve_input(spec: dict[str, Any], idx: dict[str, dict[str, Any]], *, units: str) -> Resolved:
    if "observed" in spec:
        rid = spec["observed"]
        r = idx.get(rid)
        if r is None:
            raise Unavailable(f"observed input {rid} is not in the ledger")
        if r["confidence"] == "secondary_only":
            raise Unavailable(f"observed input {rid} is secondary_only")
        amt = r["amount"]
        if not _finite(amt["value"]):
            raise Unavailable(f"observed input {rid} has no value")
        if units == "usd_bn":
            bn = _to_billions(amt)
            if bn is None:
                raise Unavailable(f"observed input {rid} is not USD millions/billions")
            return Resolved(bn, f"observed:{rid}")
        if units == "percent":
            if amt["units"] != "percent":
                raise Unavailable(f"observed input {rid} is not a percent")
            return Resolved(float(amt["value"]) / 100.0, f"observed:{rid}")
        if units == "shares_bn":
            if amt["units"] not in ("billions", "millions"):
                raise Unavailable(f"observed input {rid} share count units {amt['units']!r}")
            v = float(amt["value"]) / (1000.0 if amt["units"] == "millions" else 1.0)
            return Resolved(v, f"observed:{rid}")
        raise ValueError(f"unknown units {units}")
    if "assumption" in spec:
        # Assumptions are plain numbers in the unit the caller uses (fractions
        # for rates and margins, USD for a price, a multiple for a multiple).
        if not spec.get("why"):
            raise ValueError("every assumption needs a written 'why'")
        v = spec["assumption"]
        if not _finite(v):
            raise ValueError(f"assumption {v!r} is not a finite number")
        return Resolved(float(v), "assumption")
    raise ValueError(f"input spec needs 'observed' or 'assumption': {spec}")


def value_per_share(
    *,
    base_quarter_revenue_bn: float,
    qoq_growth: list[float],
    fcf_margin: float,
    fcf_multiple: float,
    net_cash_bn: float,
    diluted_shares_bn: float,
) -> dict[str, float]:
    """Forward-4Q revenue path -> FCF -> value per share. Pure arithmetic, no forecast."""
    if len(qoq_growth) != 4:
        raise ValueError("qoq_growth must have 4 quarters")
    if diluted_shares_bn <= 0:
        raise ValueError("diluted_shares_bn must be positive")
    rev = []
    q = base_quarter_revenue_bn
    for g in qoq_growth:
        q = q * (1.0 + g)
        rev.append(q)
    fwd_rev = sum(rev)
    fwd_fcf = fwd_rev * fcf_margin
    equity = fwd_fcf * fcf_multiple + net_cash_bn
    return {
        "fwd4q_revenue_bn": fwd_rev,
        "fwd4q_fcf_bn": fwd_fcf,
        "equity_value_bn": equity,
        "value_per_share": equity / diluted_shares_bn,
        "quarters_bn": rev,
    }


def implied_flat_growth(
    *,
    price: float,
    base_quarter_revenue_bn: float,
    fcf_margin: float,
    fcf_multiple: float,
    net_cash_bn: float,
    diluted_shares_bn: float,
    lo: float = -0.5,
    hi: float = 1.0,
) -> float | None:
    """Constant q/q growth at which value_per_share equals price (bisection), or None if outside [lo, hi]."""

    def f(g: float) -> float:
        return value_per_share(
            base_quarter_revenue_bn=base_quarter_revenue_bn,
            qoq_growth=[g] * 4,
            fcf_margin=fcf_margin,
            fcf_multiple=fcf_multiple,
            net_cash_bn=net_cash_bn,
            diluted_shares_bn=diluted_shares_bn,
        )["value_per_share"] - price

    flo, fhi = f(lo), f(hi)
    if flo > 0 or fhi < 0:
        return None
    for _ in range(200):
        mid = (lo + hi) / 2.0
        if f(mid) < 0:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def run_scenarios(records: list[dict[str, Any]], spec: dict[str, Any]) -> dict[str, Any]:
    """Resolve the shared basis and each scenario; UNAVAILABLE where an observed input is missing."""
    idx = _index(records)
    out: dict[str, Any] = {"schema": "r3-nvda-scenarios-result/2", "basis": {}, "scenarios": {}}
    try:
        basis = spec["basis"]
        price_spec = basis["price"]
        if "observed" in price_spec:
            raise ValueError("the price is a dated assumption, never an observed ledger id")
        if not price_spec.get("as_of") or not price_spec.get("source"):
            raise ValueError("the price basis needs 'as_of' and 'source' (a dated, labelled price)")
        price_v = resolve_input(price_spec, idx, units="usd_bn").value
        shares = resolve_input(basis["diluted_shares"], idx, units="shares_bn")
        cash = resolve_input(basis["cash_and_securities"], idx, units="usd_bn")
        debt = resolve_input(basis["total_debt"], idx, units="usd_bn")
        base_rev = resolve_input(basis["base_quarter_revenue"], idx, units="usd_bn")
    except Unavailable as exc:
        out["basis"] = {"status": "UNAVAILABLE", "why": str(exc)}
        for name in spec.get("scenarios", {}):
            out["scenarios"][name] = {"status": "UNAVAILABLE", "why": "basis unavailable"}
        return out
    net_cash = cash.value - debt.value
    out["basis"] = {
        "status": "OK",
        "price": price_v,
        "price_as_of": price_spec["as_of"],
        "price_source": price_spec["source"],
        "diluted_shares_bn": shares.value,
        "net_cash_bn": net_cash,
        "base_quarter_revenue_bn": base_rev.value,
        "origins": {
            "diluted_shares": shares.origin,
            "cash_and_securities": cash.origin,
            "total_debt": debt.origin,
            "base_quarter_revenue": base_rev.origin,
        },
    }
    for name, sc in spec["scenarios"].items():
        try:
            if "observed" in sc["fcf_multiple"]:
                raise ValueError("fcf_multiple is always an assumption")
            growth = [resolve_input(g, idx, units="percent").value for g in sc["qoq_growth"]]
            fcf_margin = resolve_input(sc["fcf_margin"], idx, units="percent")
            mult = resolve_input(sc["fcf_multiple"], idx, units="percent")
        except Unavailable as exc:
            out["scenarios"][name] = {"status": "UNAVAILABLE", "why": str(exc)}
            continue
        res = value_per_share(
            base_quarter_revenue_bn=base_rev.value,
            qoq_growth=growth,
            fcf_margin=fcf_margin.value,
            fcf_multiple=mult.value,
            net_cash_bn=net_cash,
            diluted_shares_bn=shares.value,
        )
        res["vs_price_pct"] = (res["value_per_share"] / price_v - 1.0) * 100.0
        if "reference_fcf_margin" in sc:
            ref = resolve_input(sc["reference_fcf_margin"], idx, units="percent").value
            extra = (ref - fcf_margin.value) * res["fwd4q_revenue_bn"]
            res["reference_fcf_margin"] = ref
            res["implied_extra_working_capital_bn"] = extra
            # Extra days of forward revenue tied up if all of it sat in receivables.
            res["implied_extra_dso_days_if_all_receivables"] = extra / (res["fwd4q_revenue_bn"] / 364.0)
        res["implied_flat_qoq_growth_at_price"] = implied_flat_growth(
            price=price_v,
            base_quarter_revenue_bn=base_rev.value,
            fcf_margin=fcf_margin.value,
            fcf_multiple=mult.value,
            net_cash_bn=net_cash,
            diluted_shares_bn=shares.value,
        )
        res["status"] = "OK"
        res["label"] = "conditional calculation, not a forecast"
        out["scenarios"][name] = res
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ledger", type=Path, default=REPO / LEDGER_PATH)
    ap.add_argument("--scenarios", type=Path, default=REPO / SCENARIOS_PATH)
    ap.add_argument("--cutoff", default=FREEZE_CUTOFF)
    args = ap.parse_args(argv)
    records = load_ledger(args.ledger)
    built_at = LEDGER_BUILT_AT if args.ledger.resolve() == (REPO / LEDGER_PATH).resolve() else None
    errs = validate_ledger(records, args.cutoff, built_at)
    report: dict[str, Any] = {
        "schema": SCHEMA,
        "records": len(records),
        "ledger_sha256": ledger_sha256(records),
        "frozen_sha256": FROZEN_LEDGER_SHA256,
        "errors": errs,
        "stack": commitment_stack(records) if not errs else None,
    }
    if not errs and args.scenarios.exists():
        report["scenarios"] = run_scenarios(records, json.loads(args.scenarios.read_text(encoding="utf-8")))
    json.dump(report, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 1 if errs else 0


if __name__ == "__main__":
    raise SystemExit(main())
