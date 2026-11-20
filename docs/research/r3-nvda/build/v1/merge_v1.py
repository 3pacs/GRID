"""v1: merge owner/sec/cp/health records into the R3 ledger with owner dedup, count, circularity and firmness decisions."""
import json, sys
from pathlib import Path
S = Path(sys.argv[1]); OUT = Path(sys.argv[2])
recs = []
for f in ("owner-records-v1.jsonl", "sec-records-reviewed.jsonl", "cp-records-reviewed.jsonl", "health-labs-records.jsonl", "health-neo-records.jsonl", "health-ora-records.jsonl"):
    for l in open(S / f, encoding="utf-8"):
        if l.strip():
            r = json.loads(l); r["_file"] = f
            if f.startswith("health-"):
                r.setdefault("review", {"by": "health worker (quotes script-checked against raw download; no separate critic)", "verdict": "NOT_CHECKED", "finding": None})
                if not r["source"].get("published_at"):
                    import re as _re
                    m = _re.search(r"/(20\d\d)/(\d\d)/(\d\d)/", r["source"]["url"])
                    if m:
                        r["source"]["published_at"] = "-".join(m.groups())
                        r["unknowns"].append("[owner] publication date taken from the article URL")
                    else:
                        r["source"]["published_at"] = None  # v1: never invent a time
                        r["unknowns"].append("[owner] publication time unknown (left null)")
            recs.append(r)
ids = [r["id"] for r in recs]
dups = {i for i in ids if ids.count(i) > 1}
assert not dups, dups

# group -> (counted id or None, members, reason for the rest)
G = {
 "supply-commitments": ("nvda-10q-q2fy27-supply-commitments",
    ["nvda-10k-fy26-supply-commitments", "nvda-10k-fy26-cam-supply-obligations", "nvda-cfo-q4fy26-supply-commitments",
     "nvda-10q-q1fy27-supply-commitments", "nvda-10q-q2fy27-supply-commitments-text", "nvda-cfo-q2fy27-supply-memory"],
    "same balance at an earlier date or restated elsewhere; the latest point-in-time balance (Q2 FY2027) counts"),
 "cloud-service-agreements": ("nvda-10q-q2fy27-cloud-service-agreements",
    ["nvda-10k-fy26-cloud-service-agreements", "nvda-10q-q1fy27-cloud-service-agreements"],
    "earlier point-in-time balance of the same agreements"),
 "ai-cloud-agreements": ("nvda-10q-q2fy27-ai-cloud-agreements", ["nvda-10q-q2fy27-ai-cloud-duration"], "same dollars, duration text"),
 "investment-commitments": ("nvda-10q-q2fy27-equity-investment-commitments",
    ["nvda-10k-fy26-investment-commitments", "nvda-10q-q1fy27-investment-commitments"], "earlier point-in-time balance"),
 "ports-pike-rvg": ("nvda-10q-q2fy27-sb-energy-guarantee",
    ["nvda-8k-20260817-sb-energy-rvg", "nvda-8k-20260817-rvg-105b-cap"], "same $105B guarantee cap"),
 "lps-guarantees": ("nvda-10q-q2fy27-lps-guarantees-ai-clouds", ["nvda-10k-fy26-lps-guarantees"], "same $3.5B"),
 "sb-energy-investment": (None, ["nvda-8k-20260817-ex99-sb-energy-investment", "nvda-sbenergy-1.5b-investment"],
    "may sit inside the Q2 FY2027 equity-investment commitments ($25B); not counted separately"),
 "q2-commitment-totals": (None, ["nvda-10q-q2fy27-total-commitments", "nvda-10q-q2fy27-additional-commitments-total", "nvda-10q-q2fy27-guarantee-total"],
    "a total of components that are counted individually"),
 "older-other-commitments": (None, ["nvda-10q-q1fy27-other-vendor-commitments", "nvda-10k-fy26-other-commitments"],
    "earlier-date balance not carried as a separate Q2 FY2027 line; not counted"),
 "financing-platform-mous": (None, ["nvda-10q-q2fy27-financing-platform-mous", "nvda-pr-q2fy27-financing-platforms"],
    "third-party capital MOUs, not an NVIDIA obligation"),
 "openai-investment": (None, ["oai-nvda-loi-100b-investment-20250922", "oai-nvda-30b-equity-2026", "nvda-10k-fy26-openai-finalizing", "nvda-sec-openai-equity-not-disclosed"],
    "non-binding LOI / secondary report; any funded or committed amount is inside NVIDIA's aggregates, which do not name investees"),
 "named-investments-in-aggregate": (None, ["crwv-nvda-2b-equity-20260123", "nscale-nvda-seriesb-400m", "nscale-nvda-seriesc-777m", "nscale-nvda-cln-1b-202609",
     "nscale-nvda-sale-1b-20261116", "nbis-nvda-2b-prefunded-warrants", "anthropic-nvda-10b-investment", "intc-nvda-5b-equity-closed",
     "intc-nvda-collab-20250918", "xai-series-e-nvda-strategic", "nvda-10k-fy26-intel-investment", "nvda-pr-q4fy26-anthropic"],
    "a named investment; NVIDIA reports investments only in aggregate (funded $99B, committed $25B), so naming them separately would double count"),
 "capacity-buybacks-in-aggregate": (None, ["crwv-nvda-capacity-order-6.3b-20250909", "nscale-nvda-msa-1.2b"],
    "a named NVIDIA cloud-capacity purchase; likely inside the Q2 FY2027 cloud-service or AI-cloud agreement balances, which do not name counterparties"),
}
COUNT_ALONE = {  # counted, no group
 "nvda-10q-q2fy27-dc-leases-not-commenced": "Q2 FY2027 data-center leases not commenced",
 "nvda-10q-q2fy27-dc-leases-third-party": "Q2 FY2027 leases signed to reassign to third parties",
 "nvda-10q-q2fy27-capex-commitments": "Q2 FY2027 capital-expenditure commitments",
 "nvda-8k-20260903-hugging-face": "acquisition agreed 2026-09-03, after the Q2 FY2027 balance date, so not inside it",
}
idx = {r["id"]: r for r in recs}
for r in recs:
    r["dedup_group"] = r.get("dedup_group"); r["counted"] = False
    r["count_reason"] = "context only (not NVIDIA dollars, unvalued, or not a commitment)"
for g, (c, members, why) in G.items():
    for m in ([c] if c else []) + members:
        assert m in idx, (g, m)
        idx[m]["dedup_group"] = g
        idx[m]["count_reason"] = why
    if c:
        idx[c]["counted"] = True; idx[c]["count_reason"] = f"counted: latest primary figure for {g}"
for i, why in COUNT_ALONE.items():
    idx[i]["counted"] = True; idx[i]["count_reason"] = "counted: " + why
# --- v1 (independent review on #1150): circularity as NVIDIA's own wording supports it
RECLASS = {
 "unknown": (["nvda-10k-fy26-cloud-service-agreements", "nvda-10q-q1fy27-cloud-service-agreements", "nvda-10q-q2fy27-cloud-service-agreements"],
             "10-Q: 'These commitments provide the cloud infrastructure to support our research and development of our open models ... and our autonomous vehicle software' - R&D compute, not a buy-back; vendors not named"),
 "vendor_financing": (["nvda-10q-q2fy27-dc-leases-third-party"],
             "leases NVIDIA signed (about 15 years, commencing FY2028-29) and expects to reassign to third parties: credit support, not a capacity buy-back"),
}
for circ, (ids, why) in RECLASS.items():
    for i in ids:
        idx[i]["circularity"] = circ; idx[i]["unknowns"].append("[v1 reclassified] " + why)
for i in ("nvda-10k-fy26-investment-commitments", "nvda-10q-q1fy27-investment-commitments", "nvda-10q-q2fy27-equity-investment-commitments"):
    idx[i]["circularity"] = "unknown"
    idx[i]["unknowns"].append("[v1 reclassified] 10-Q: commitments to 'AI model makers, infrastructure financiers, and other private companies, subject to certain contingencies' - not all customers")
FIRM = {
 "nvda-10q-q2fy27-supply-commitments": "cancellable",   # 'may be cancelable, rescheduled, or adjustable ... prior to placing firm orders'
 "nvda-10q-q2fy27-cloud-service-agreements": "firm",
 "nvda-10q-q2fy27-ai-cloud-agreements": "firm",
 "nvda-10q-q2fy27-dc-leases-not-commenced": "firm",
 "nvda-10q-q2fy27-dc-leases-third-party": "firm",
 "nvda-10q-q2fy27-capex-commitments": "firm",
 "nvda-10q-q2fy27-equity-investment-commitments": "contingent",  # 'subject to certain contingencies'
 "nvda-10q-q2fy27-sb-energy-guarantee": "contingent",             # cap, effective lease by lease from FY2029
 "nvda-10q-q2fy27-lps-guarantees-ai-clouds": "contingent",
 "nvda-8k-20260903-hugging-face": "post_balance",                  # agreed after 2026-07-26; closing conditional
}
for i, f in FIRM.items():
    assert idx[i]["counted"], i
    idx[i]["firmness"] = f
assert all(r.get("firmness") for r in recs if r["counted"])
TOTALS = {
 "nvda-10q-q2fy27-total-commitments": ["nvda-10q-q2fy27-supply-commitments", "nvda-10q-q2fy27-cloud-service-agreements",
     "nvda-10q-q2fy27-dc-leases-not-commenced", "nvda-10q-q2fy27-equity-investment-commitments", "nvda-10q-q2fy27-capex-commitments"],
 "nvda-10q-q2fy27-additional-commitments-total": ["nvda-10q-q2fy27-ai-cloud-agreements", "nvda-10q-q2fy27-dc-leases-third-party"],
 "nvda-10q-q2fy27-guarantee-total": ["nvda-10q-q2fy27-sb-energy-guarantee", "nvda-10q-q2fy27-lps-guarantees-ai-clouds"],
}
for tot, comps in TOTALS.items():
    idx[tot]["total_of"] = comps
for r in recs:
    r.pop("_file")
recs.sort(key=lambda r: r["id"])
OUT.parent.mkdir(parents=True, exist_ok=True)
with open(OUT, "w", encoding="utf-8") as f:
    for r in recs:
        f.write(json.dumps(r, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n")
print(len(recs), "records ->", OUT)
