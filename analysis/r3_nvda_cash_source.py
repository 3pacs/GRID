"""Pinned-filing cash-flow extraction. No network; originals remain unchanged."""

import hashlib
import json
import re
import sys
from html.parser import HTMLParser
from pathlib import Path

SPECS = {
    "Q1": dict(
        filename="000104581026000052__nvda-20260426.htm",
        sha256="ac4b0aa2f61a454a29df784d128b9da2f2410654276f65580fe29c79bd4bf0b4",
        url="https://www.sec.gov/Archives/edgar/data/1045810/000104581026000052/nvda-20260426.htm",
        start="2026-01-26",
        end="2026-04-26",
        published_at="2026-05-20T16:35:52-04:00",
        retrieved_at="2026-11-13T00:17:53Z",
        header="Three Months Ended",
    ),
    "H1": dict(
        filename="000104581026000075__nvda-20260726.htm",
        sha256="66cbd404bb7d5c6d67e57d5440029fd8dd6be64dfce5ad9ae04622d2e7d7fa24",
        url="https://www.sec.gov/Archives/edgar/data/1045810/000104581026000075/nvda-20260726.htm",
        start="2026-01-26",
        end="2026-07-26",
        published_at="2026-08-26T16:36:00-04:00",
        retrieved_at="2026-11-13T00:17:54Z",
        header="Six Months Ended",
    ),
}
LABELS = {
    "net_income": "Net income",
    "sbc": "Stock-based compensation expense",
    "deferred_tax": "Deferred income taxes",
    "da": "Depreciation and amortization",
    "equity_gains": None,
    "other": "Other",
    "ar": "Accounts receivable",
    "inventory": "Inventories",
    "prepaid_other_assets": "Prepaid expenses and other assets",
    "ap": "Accounts payable",
    "accrued_current": "Accrued and other current liabilities",
    "other_longterm": "Other long-term liabilities",
    "cfo": "Net cash provided by operating activities",
    "capex": "Purchases related to property and equipment and intangible assets",
    "principal": "Principal payments on property and equipment and intangible assets",
}


class Rows(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.buf = None

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self.buf = []

    def handle_data(self, data):
        if self.buf is not None:
            self.buf.append(data)

    def handle_endtag(self, tag):
        if tag == "tr" and self.buf is not None:
            self.rows.append(" ".join(" ".join(self.buf).split()))
            self.buf = None


def first_value(row, label):
    if not row.startswith(label + " "):
        raise ValueError("label mismatch")
    text = row[len(label) :].strip().lstrip("$").strip()
    m = re.match(r"^(\(\s*)?([0-9][0-9,]*)(\s*\))?(?:\s|$)", text)
    if not m or bool(m[1]) != bool(m[3]):
        raise ValueError("unparseable signed value")
    return int(m[2].replace(",", "")) * (-1 if m[1] else 1)


def extract(raw, period):
    spec = SPECS[period]
    if hashlib.sha256(raw).hexdigest() != spec["sha256"]:
        raise ValueError("source hash mismatch")
    p = Rows()
    p.feed(raw.decode("utf-8"))
    starts = [
        i
        for i, r in enumerate(p.rows)
        if r.startswith("Cash flows from operating activities:")
    ]
    if len(starts) != 1:
        raise ValueError("ambiguous cash-flow statement")
    start = starts[0]
    if spec["header"] not in " ".join(p.rows[max(0, start - 4) : start]):
        raise ValueError("period header mismatch")
    end = next(
        i
        for i in range(start + 1, len(p.rows))
        if p.rows[i].startswith("Cash and cash equivalents at end of period")
    )
    rows = p.rows[start : end + 1]
    cfo_end = next(i for i, r in enumerate(rows) if r.startswith(LABELS["cfo"]))
    out = {}
    for key, base in LABELS.items():
        label = base or (
            "(Gains) losses from equity securities, net"
            if period == "Q1"
            else "Gains from equity securities, net"
        )
        scope = rows if key in ("capex", "principal") else rows[: cfo_end + 1]
        found = [
            r for r in scope if re.match(re.escape(label) + r"\s+\$?\s*(?:\(|[0-9])", r)
        ]
        if len(found) != 1:
            raise ValueError("missing/ambiguous " + key)
        out[key] = dict(
            value=first_value(found[0], label),
            kind="observed",
            currency="USD",
            unit="millions",
            period_start=spec["start"],
            period_end=spec["end"],
            source_url=spec["url"],
            source_sha256=spec["sha256"],
            published_at=spec["published_at"],
            retrieved_at=spec["retrieved_at"],
            row_text=found[0],
            section="Condensed Consolidated Statements of Cash Flows",
            column="first/current-period",
        )
    return out


def reconcile(facts):
    vals = {k: v["value"] for k, v in facts.items()}
    residual = (
        sum(vals[k] for k in LABELS if k not in ("cfo", "capex", "principal"))
        - vals["cfo"]
    )
    if residual:
        raise ValueError("CFO reconciliation residual " + str(residual))
    return dict(
        cfo=vals["cfo"],
        cfo_less_capex=vals["cfo"] + vals["capex"],
        issuer_style_fcf=vals["cfo"] + vals["capex"] + vals["principal"],
        cfo_residual=residual,
    )


def derive_q2(q1, h1):
    out = {}
    for k in LABELS:
        a, b = q1[k], h1[k]
        if (
            any(a[x] != b[x] for x in ("currency", "unit", "period_start"))
            or a["period_end"] != "2026-04-26"
            or b["period_end"] != "2026-07-26"
        ):
            raise ValueError("incompatible periods/units")
        out[k] = dict(
            value=b["value"] - a["value"],
            kind="derived",
            currency=a["currency"],
            unit=a["unit"],
            period_start="2026-04-27",
            period_end=b["period_end"],
            formula="H1 minus Q1",
            inputs=[
                {"period": "H1", "field": k, "source_sha256": b["source_sha256"]},
                {"period": "Q1", "field": k, "source_sha256": a["source_sha256"]},
            ],
            available_at=max(a["published_at"], b["published_at"]),
        )
    return out


def build(root):
    data = {
        p: extract((Path(root) / s["filename"]).read_bytes(), p)
        for p, s in SPECS.items()
    }
    data["Q2"] = derive_q2(data["Q1"], data["H1"])
    return dict(
        schema="nvda-cash-reconciliation/1",
        facts=data,
        reconciliations={p: reconcile(v) for p, v in data.items()},
        limitations=[
            "Pinned historical sources, not a refreshed investment view",
            "Publication/retrieval timestamps inherited from frozen ledger; source bytes independently hash checked",
            "Cash flow is not FCFF or FCFE; no valuation",
            "Net-of-acquisition cash-flow adjustments used, not raw stock differences",
        ],
    )


if __name__ == "__main__":
    print(json.dumps(build(sys.argv[1]), indent=2))
