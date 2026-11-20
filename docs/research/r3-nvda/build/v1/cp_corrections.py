"""Apply the counterparty critic's corrections and omissions (owner step, reproducible)."""
import json, sys, copy, re, html, hashlib
from pathlib import Path
S = Path(sys.argv[1]); D = S / "critic-cp-dl"
recs, order = {}, []
for l in open(S / "cp-records.jsonl", encoding="utf-8"):
    r = json.loads(l); recs[r["id"]] = r; order.append(r["id"])
crit = {json.loads(l)["id"]: json.loads(l) for l in open(S / "critic-cp.jsonl", encoding="utf-8")}
def note(i, t): recs[i]["unknowns"].append("[review] " + t)
for i in order:
    c = crit.get(i); r = recs[i]
    r["review"] = {"by": "critic-cp (independent agent re-fetch)", "verdict": c["verdict"] if c else "NOT_CHECKED", "finding": (c or {}).get("finding")}
    if c and (c.get("checks") or {}).get("sha256_match") is False and c.get("my_sha256"):
        r["source"]["retrieved_sha256"] = c["my_sha256"]
        note(i, "retrieved_sha256 replaced with the critic's hash of the document at source.url (the worker had hashed the full-submission .txt)")
    elif c and (c.get("checks") or {}).get("sha256_match") is False:
        r["source"]["retrieved_sha256"] = None
        note(i, "worker hash was of a different file; no verified hash of the cited document")
# wrong document URLs (critic gave url + sha)
for i, url, sha in [
    ("crwv-capex-guidance-fy2026", "https://www.sec.gov/Archives/edgar/data/1769628/000176962826000461/coreweave3q26earningspress.htm", "8a8a47dcb600c6b10aa33ac2a12f0e2da63a3dcb6297ab728a9c5baf2528d68b"),
    ("orcl-gpus-delivered-q1fy27", "https://www.sec.gov/Archives/edgar/data/1341439/000119312526387905/orcl-ex99_1.htm", "b8778d2eb78bc17950e401faa386305d711f33898b95f6e0483091d7b5b2b8cf"),
    ("nbis-nvda-2b-prefunded-warrants", "https://www.sec.gov/Archives/edgar/data/1513845/000110465926126238/nbis-ex99d2.htm", "400cc7a6130e2e11317e0a2f015c86cb78afc6abd2b9cc7765cba23c344c90c4")]:
    recs[i]["source"]["url"] = url; recs[i]["source"]["retrieved_sha256"] = sha; recs[i]["source"]["doc_type"] = "8-K EX-99.1" if "orcl" in i or "crwv" in i else "6-K"
    note(i, "quote is in the exhibit, not the cover document; url and hash corrected")
# value / classification fixes
recs["oai-nvda-loi-10gw-20250922"]["circularity"] = "nvidia_investment_in_customer"
recs["oai-nvda-loi-10gw-20250922"]["amount"]["basis"] = "unknown"
recs["oai-nvda-loi-10gw-20250922"]["contradictions"] = ["NVIDIA 10-Q Q3 FY26 (0001045810-25-000230): no assurance of definitive agreements for the OpenAI opportunity",
    "NVIDIA 10-K FY26 (0001045810-26-000021): 'We are finalizing an investment and partnership agreement with OpenAI' (no amount)"]
note("oai-nvda-loi-10gw-20250922", "a letter of intent; not a commitment to NVIDIA")
note("nvda-sbenergy-1.5b-investment", "NVIDIA's total is $3.0B per SB Energy S-1/A: $1.5B prepaid forward (funded 2026-08-17) + $1.5B private placement contingent on the IPO (see sbenergy-s1a-nvda-3.0b)")
r = recs["crwv-nvda-msa-payments-2025"]; r["amount"]["basis"] = "nvidia_obligation"; r["circularity"] = "capacity_backstop"
note(r["id"], "NVIDIA paying CoreWeave under the April-2023 MSA and order forms, not the $6.3B order alone")
note("orcl-no-nvidia-named-negative", "true for Oracle 10-Q/10-K only; Oracle 8-K EX-99.1 2025-12-10 names NVIDIA among new RPO commitments (NVIDIA is an Oracle cloud customer) and the 2026-02-02 FWP names NVIDIA and OpenAI as OCI customers")
r = recs["nscale-nvda-seriesc-777m"]; r["amount"]["value"] = 777.399382; note(r["id"], "value is the table total: $300M SAFE + $477.4M C-4")
r = recs["anthropic-azure-30b-nvda-1gw"]; r["contradictions"].append("superseded: Anthropic S-1/A (0001628280-26-072484) commits at least $47.2B and up to $49.2B with Microsoft for Nov 2026 - Jun 2033 (anthropic-s1a-microsoft-47.2b)")
r["dedup_group"] = "anthropic-microsoft"
r = recs["skhynix-lta-q3-2026"]; note(r["id"], "quote not in the cited page; primary is news.skhynix.com/q3-2026-business-results/ (see skhynix-q3-2026-ltas)")
recs["nscale-anthropic-44.6b-vr"]["dedup_group"] = "anthropic-nscale"

# --- omission records, quotes extracted verbatim from the critic's downloads
def text(f):
    t = open(D / f, encoding="utf-8", errors="replace").read()
    t = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", t, flags=re.S); t = re.sub(r"<ix:header>.*?</ix:header>", " ", t, flags=re.S); t = re.sub(r"<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", html.unescape(t)).replace("​", "")
def quote(f, phrase, after=260, before=0):
    t = text(f); k = t.find(phrase)
    assert k >= 0, (f, phrase)
    q = t[max(0, k - before): k + after]
    return q.strip()
def sha(f): return hashlib.sha256(open(D / f, "rb").read()).hexdigest()
def mtime(f):  # v1: the real time the critic downloaded these bytes
    import os, datetime
    return datetime.datetime.fromtimestamp(os.path.getmtime(D / f), datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
DOCS = {
 "anth": ("c6e1b6b42938_anthropicpbc_sx1ax2.htm", "https://www.sec.gov/Archives/edgar/data/2130357/000162828026072484/anthropicpbc-sx1ax2.htm", "S-1", "0001628280-26-072484", "2026-11-10T06:29:24-05:00", "Anthropic PBC"),
 "sbe": ("bbcb05d0ba4e_sbenergy_sx1a2.htm", "https://www.sec.gov/Archives/edgar/data/2133037/000162828026062846/sbenergy-sx1a2.htm", "S-1", "0001628280-26-062846", "2026-09-21T12:39:58-04:00", "SB Energy Corp."),
 "orcl": ("ee4d00cc1fe7_orcl_20260831.htm", "https://www.sec.gov/Archives/edgar/data/1341439/000119312526389274/orcl-20260831.htm", "10-Q", "0001193125-26-389274", "2026-09-11T16:10:12-04:00", "Oracle Corporation"),
 "meta": ("4530a1716849_meta_20260930.htm", "https://www.sec.gov/Archives/edgar/data/1326801/000162828026066773/meta-20260930.htm", "10-Q", "0001628280-26-066773", "2026-10-21T20:00:08-04:00", "Meta Platforms, Inc."),
 "goog": ("b85dd4fa2c80_goog_20260930.htm", "https://www.sec.gov/Archives/edgar/data/1652044/000165204426000084/goog-20260930.htm", "10-Q", "0001652044-26-000084", "2026-10-28T20:29:06-04:00", "Alphabet Inc."),
 "msft": ("fc089e1d4880_msft_20260930.htm", "https://www.sec.gov/Archives/edgar/data/789019/000119312526436133/msft-20260930.htm", "10-Q", "0001193125-26-436133", "2026-10-28T16:07:39-04:00", "Microsoft Corporation"),
 "amd": ("6948afd9edec_amd_20260627.htm", "https://www.sec.gov/Archives/edgar/data/2488/000000248826000123/amd-20260627.htm", "10-Q", "0000002488-26-000123", "2026-08-04T17:40:01-04:00", "Advanced Micro Devices, Inc."),
}
def add(rid, doc, phrase, *, stage, direction, basis, value, units, counterparty, circ="none", kind="commitment", unknowns=(), group=None, after=260, period=None):
    f, url, dt, acc, pub, issuer = DOCS[doc]
    assert rid not in recs
    recs[rid] = {"id": rid, "record_kind": kind, "stage": stage, "direction": direction, "issuer": issuer, "counterparty": counterparty,
        "source": {"url": url, "doc_type": dt, "accession": acc, "section": "located by phrase: " + phrase[:60], "quote": quote(f, phrase, after),
                   "published_at": pub, "retrieved_at": mtime(f), "retrieved_sha256": sha(f)},
        "fiscal_period": period, "amount": {"value": value, "currency": "USD" if units in ("billions", "millions") else None, "units": units, "basis": basis},
        "timing": {"start": None, "end": None, "text": None}, "conditions": [], "contradictions": [], "circularity": circ,
        "dedup_group": group, "counted": False, "unknowns": list(unknowns), "confidence": "primary_verbatim",
        "review": {"by": "owner from critic-cp omission (quote re-extracted from the critic's download)", "verdict": "ADDED", "finding": None}}
    order.append(rid)
add("anthropic-s1a-nvda-private-placement", "anth", "NVIDIA", stage="nvidia_investment", direction="financing", basis="nvidia_investment", value=None, units=None,
    counterparty="NVIDIA Corporation", circ="nvidia_investment_in_customer", group="named-investments-in-aggregate",
    unknowns=["critic reads ~$10.0B (135,135,135 Class N shares at the $74 midpoint) within a $26.0B concurrent private placement contingent on the IPO; value left null until the exact sentence is pinned"])
add("anthropic-s1a-microsoft-47.2b", "anth", "we committed to spend an aggregate of at least $47.2 billion", stage="purchase_commitment", direction="demand", basis="unknown",
    value=47.2, units="billions", counterparty="Microsoft Corporation", group="anthropic-microsoft", after=200,
    unknowns=["a cloud contract between two non-NVIDIA parties; not NVIDIA revenue"])
recs["anthropic-s1a-microsoft-47.2b"]["source"]["quote"] = quote(DOCS["anth"][0], "In November 2025, we entered into an agreement with Microsoft", 330)
add("anthropic-s1a-spacexai-120.5b", "anth", "120.5 billion", stage="purchase_commitment", direction="demand", basis="unknown", value=120.5, units="billions",
    counterparty="SpaceXAI (formerly X.AI LLC)", after=120, unknowns=["cancellable by either party on 90 days' notice after non-cancelable periods ending Jan/Mar 2027 (critic reading)"])
recs["anthropic-s1a-spacexai-120.5b"]["source"]["quote"] = quote(DOCS["anth"][0], "120.5 billion", 120, before=200)
add("anthropic-s1a-four-chip-families", "anth", "four major chip families", stage="financial_fact", kind="financial_fact", direction="supply", basis="unknown",
    value=None, units=None, counterparty=None, after=160, unknowns=["substitution evidence: AMD Helios commitment 'to exceed $20.0 billion' and non-cancelable Google TPU commitments (critic reading)"])
recs["anthropic-s1a-four-chip-families"]["source"]["quote"] = quote(DOCS["anth"][0], "four major chip families", 120, before=80)
add("sbenergy-s1a-nvda-3.0b", "sbe", "In total, NVIDIA will invest $3.0 billion", stage="nvidia_investment", direction="financing", basis="nvidia_investment", value=3.0, units="billions",
    counterparty="NVIDIA Corporation", circ="nvidia_investment_in_customer", group="sb-energy-investment", after=110)
add("oracle-q1fy27-leases-not-commenced-288b", "orcl", "288 billion of additional lease commitments, substantially all", stage="purchase_commitment", direction="infrastructure", basis="counterparty_capex",
    value=288, units="billions", counterparty=None, after=250, period="Q1 FY2027 (ended 2026-08-31)")
add("oracle-q1fy27-customer-prepayments-11.4b", "orcl", "11.4 billion of prepayments from customers", stage="cash_receipt", direction="financing", basis="unknown", value=11.4, units="billions",
    counterparty=None, after=150, period="Q1 FY2027 (ended 2026-08-31)")
recs["oracle-q1fy27-customer-prepayments-11.4b"]["source"]["quote"] = quote(DOCS["orcl"][0], "11.4 billion of prepayments from customers", 120, before=70)
add("meta-q3-2026-noncancelable-454.52b", "meta", "454.52 billion of non-cancelable contractual commitments, comprising", stage="purchase_commitment", direction="infrastructure", basis="counterparty_capex",
    value=454.52, units="billions", counterparty=None, after=300, period="Q3 2026 (ended 2026-09-30)")
recs["meta-q3-2026-noncancelable-454.52b"]["source"]["quote"] = quote(DOCS["meta"][0], "454.52 billion of non-cancelable contractual commitments, comprising", 300, before=20)
add("googl-q3-2026-tpu-repurchase-guarantees", "goog", "maximum potential future payments under these guarantees were", stage="financial_fact", kind="financial_fact", direction="supply", basis="unknown", value=6.6, units="billions",
    counterparty=None, after=120, period="Q3 2026 (ended 2026-09-30)", unknowns=["Google now sells TPU systems to third parties: merchant competition with NVIDIA (critic reading)"])
recs["googl-q3-2026-tpu-repurchase-guarantees"]["source"]["quote"] = quote(DOCS["goog"][0], "maximum potential future payments under these guarantees were $ 6.6 billion", 120, before=200)
add("msft-q1fy27-openai-receivable-and-funding", "msft", "Accounts receivable from OpenAI as of September 30, 2026", stage="financial_fact", kind="financial_fact", direction="financing", basis="balance_sheet", value=7.2, units="billions",
    counterparty="OpenAI", period="Q1 FY2027 (ended 2026-09-30)", after=300, unknowns=["same passage: OpenAI revenue to Microsoft of $10.0B in the quarter incl. revenue-sharing; $13.0B funding commitments of which $12.0B funded"])
add("amd-q2-2026-openai-meta-warrants", "amd", "each entitling the holder the right to purchase up to 160 million shares", stage="financial_fact", kind="financial_fact", direction="supply",
    basis="unknown", value=160, units="units", counterparty="OpenAI; Meta", after=200, period="Q2 2026", unknowns=["substitution evidence: warrants vest on AMD Instinct GPU purchase milestones"])
recs["amd-q2-2026-openai-meta-warrants"]["source"]["quote"] = quote(DOCS["amd"][0], "each entitling the holder the right to purchase up to 160 million shares", 200, before=150)
with open(S / "cp-records-reviewed.jsonl", "w", encoding="utf-8") as f:
    for i in order: f.write(json.dumps(recs[i], ensure_ascii=False) + "\n")
print(len(order), "reviewed cp records")
