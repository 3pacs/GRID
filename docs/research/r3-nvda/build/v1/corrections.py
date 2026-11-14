"""Apply the SEC critic's corrections and omissions to the worker's records (owner step, reproducible)."""
import json, sys, copy
from pathlib import Path
S = Path(sys.argv[1])
recs = {}
order = []
for l in open(S / "sec-records.jsonl", encoding="utf-8"):
    r = json.loads(l); recs[r["id"]] = r; order.append(r["id"])
crit = {}
for l in open(S / "critic-sec.jsonl", encoding="utf-8"):
    c = json.loads(l); crit[c["id"]] = c
for i in order:
    c = crit.get(i)
    recs[i]["review"] = {"by": "critic-sec (independent agent re-fetch)", "verdict": c["verdict"] if c else "NOT_CHECKED",
                         "finding": (c or {}).get("finding") or None}
q2 = recs["nvda-10q-q2fy27-debt"]["source"]  # Q2 10-Q identity (url, accession, published_at, sha)
def src10q(section, quote):
    s = copy.deepcopy(q2); s["section"] = section; s["quote"] = quote; return s  # v1: keep the worker's real retrieval time of these bytes
def note(i, txt): recs[i]["unknowns"].append("[review] " + txt)

# 1 publicly-held equity: use the stated balance-sheet line, not a sum
r = recs["nvda-10q-q2fy27-publicly-held-equity"]
r["amount"].update(value=42783, units="millions"); r["confidence"] = "primary_verbatim"
r["source"] = src10q("Condensed Consolidated Balance Sheets", "Marketable equity securities | 42,783 | 12,886")
note(r["id"], "worker had 47.7 (a sum of fair-value levels); corrected to the stated current-asset line; a further ~$5.0B locked equity sits in Other assets")
# 2 notes offering: cite the 10-Q total
r = recs["nvda-8k-20260618-notes-offering"]
r["source"] = src10q("Note 9 - Debt", "In June 2026, we issued an aggregate of $25.0 billion of senior unsecured notes across seven tranches for general corporate purposes.")
note(r["id"], "the June 8-K lists seven tranches but states no total; the total is stated in the Q2 FY2027 10-Q")
# 3 Q1 debt is stated
r = recs["nvda-10q-q1fy27-debt"]; r["confidence"] = "primary_verbatim"
r["source"]["quote"] = "Net carrying amount | $ | 8,470"; r["source"]["section"] = "Note 9 - Debt"
r["unknowns"] = [u for u in r["unknowns"] if "sum" not in u.lower()]; note(r["id"], "stated in Note 9, not a sum")
# 4/5 guarantees: signed in Q3, effective from FY2029, $105B is a cap
for i in ("nvda-10q-q2fy27-sb-energy-guarantee", "nvda-10q-q2fy27-guarantee-total", "nvda-8k-20260817-sb-energy-rvg"):
    r = recs[i]
    r["fiscal_period"] = "Q3 FY2027 event (signed 2026-08-17; disclosed in the Q2 FY2027 10-Q filed 2026-08-26)"
    r["timing"]["start"] = None
    r["timing"]["text"] = "Each guarantee generally becomes effective upon commencement of the applicable lease ... the first of which is expected in fiscal year 2029"
    r["conditions"].append("not yet effective: a cap that rises phase by phase as nine data centers are placed in service from FY2029; exposure declines as OpenAI pays; ends if OpenAI reaches a satisfactory credit rating")
note("nvda-10q-q2fy27-guarantee-total", "as of 2026-07-26 the stated exposure was only the $3.5B AI-cloud lease guarantees; $108.5B includes the August SB Energy cap")
# 6 Groq: $13.0B was paid
r = recs["nvda-10k-fy26-groq-license"]; r["record_kind"] = "financial_fact"; r["stage"] = "financial_fact"; r["direction"] = "nvidia_financials"
note(r["id"], "$13.0B was paid at closing (investing outflow), not an outstanding obligation; the obligation was the $4B payable within one year (accrued $3,921M at FY26 end)")
# 7 named stakes do appear in NVIDIA's 13G/13F filings
note("nvda-sec-openai-equity-not-disclosed", "true only for 10-K/10-Q text and private investees (OpenAI, Anthropic, xAI); NVIDIA's 13G/13F filings name CoreWeave, Nebius, Intel and SpaceX stakes (see nvda-13f-20260630-*)")
# 8 provisions overlap
r = recs["nvda-10k-fy26-inventory-provisions"]; r["dedup_group"] = "fy26-provisions"
note(r["id"], "inventory-only; like-for-like FY26 total incl. excess purchase obligations is $7.2B incl. $4.5B H20 (do not add to nvda-10k-fy26-h20-charge)")
recs["nvda-10k-fy26-h20-charge"]["dedup_group"] = "fy26-provisions"
# 9/10 buyback authorizations are not cash flows
for i in ("nvda-pr-q1fy27-buyback", "nvda-pr-q4fy26-buyback-remaining"):
    recs[i]["amount"]["basis"] = "unknown"; note(i, "a repurchase authorization, not a cash flow")

# omissions -> new records (verbatim from the critic's independent re-fetch)
base = {"record_kind": "financial_fact", "stage": "financial_fact", "direction": "nvidia_financials", "issuer": "NVIDIA Corporation",
        "counterparty": None, "conditions": [], "contradictions": [], "circularity": "none", "dedup_group": None, "counted": False,
        "unknowns": [], "confidence": "primary_verbatim", "timing": {"start": None, "end": None, "text": None},
        "review": {"by": "owner from critic-sec omission", "verdict": "ADDED", "finding": None}}
def add(**kw):
    r = copy.deepcopy(base); r.update(kw); assert r["id"] not in recs; recs[r["id"]] = r; order.append(r["id"])
P = "Q2 FY2027 (ended 2026-07-26)"
add(id="nvda-10q-q2fy27-h1-prepaid-other-assets-outflow", fiscal_period="H1 FY2027 (ended 2026-07-26)",
    source=src10q("Condensed Consolidated Statements of Cash Flows", "Prepaid expenses and other assets | (6,480) | 946"),
    amount={"value": -6480, "currency": "USD", "units": "millions", "basis": "cash_flow"},
    unknowns=["the split between supply prepayments and other items is not disclosed"])
add(id="nvda-10q-q2fy27-h1-receivables-outflow", fiscal_period="H1 FY2027 (ended 2026-07-26)",
    source=src10q("Condensed Consolidated Statements of Cash Flows", "Accounts receivable | (24,590) | (4,743) | Inventories | (10,204) | (4,880)"),
    amount={"value": -24590, "currency": "USD", "units": "millions", "basis": "cash_flow"},
    unknowns=["inventory build in the same half was (10,204)"])
add(id="nvda-10q-q2fy27-h1-equity-purchases", stage="nvidia_investment", record_kind="commitment", direction="financing",
    circularity="unknown", fiscal_period="H1 FY2027 (ended 2026-07-26)",
    source=src10q("Condensed Consolidated Statements of Cash Flows", "Proceeds from sales of equity securities | 7,241 | 70 | Purchases of equity securities | (42,404) | (1,245)"),
    amount={"value": 42404, "currency": "USD", "units": "millions", "basis": "cash_flow"},
    unknowns=["investees not named; cash already spent, so not part of the forward commitment stack"])
add(id="nvda-10q-q2fy27-public-warrants-received", fiscal_period=P, circularity="nvidia_investment_in_customer",
    source=src10q("Note 8 - Public Company Warrants", "In the second quarter of fiscal year 2027, we received warrants to purchase shares of publicly-traded common stock with terms of three to five years."),
    amount={"value": 824, "currency": "USD", "units": "millions", "basis": "balance_sheet"},
    unknowns=["issuers not named in the 10-Q; value as stated by the critic from the same note"], confidence="primary_paraphrase")
F13 = {"url": "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000065/information_table.xml", "doc_type": "other",
       "accession": "0001045810-26-000065", "section": "13F-HR information table (as of 2026-06-30)",
       "published_at": "2026-08-14T16:19:53-04:00", "retrieved_at": "2026-11-13T00:39:08Z",
       "retrieved_sha256": "82b6070ef760ccf7944af329e4dd1c79cf36bed29e3d345ba2dcf769e0e0c1c3"}
for name, val, sh, cp, circ in [("intel", 29989261126, 214776632, "Intel Corporation", "none"),
                                ("spacex", 20975594582, 122764805, "Space Exploration Technologies Corp.", "unknown"),
                                ("coreweave", 4699617158, 47213353, "CoreWeave, Inc.", "nvidia_investment_in_customer")]:
    s = dict(F13); s["quote"] = f"<value>{val}</value> ... <sshPrnamt>{sh}</sshPrnamt>"
    add(id=f"nvda-13f-20260630-{name}", record_kind="commitment", stage="nvidia_investment", direction="financing", counterparty=cp,
        circularity=circ, fiscal_period="as of 2026-06-30", source=s,
        amount={"value": val, "currency": "USD", "units": None, "basis": "nvidia_investment"},
        unknowns=["13F market value in US dollars (units: dollars); funded holding, inside NVIDIA's aggregate equity investments"],
        dedup_group="named-investments-in-aggregate", confidence="primary_paraphrase")
s = {"url": "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000062/primary_doc.xml", "doc_type": "other",
     "accession": "0001045810-26-000062", "section": "Schedule 13G cover page",
     "quote": "<sch:eventDateRequiresFilingThisStatement>07/13/2026 ... <sch:classPercent>9.3",
     "published_at": "2026-07-20T17:00:07-04:00", "retrieved_at": "2026-11-13T00:39:09Z",
     "retrieved_sha256": "ad12837640dcac1f564ddceeb3b357950f4a0af2ee22ac6493f56dc200682a54"}
add(id="nvda-13g-20260713-nebius-9.3pct", record_kind="commitment", stage="nvidia_investment", direction="financing",
    counterparty="Nebius Group N.V.", circularity="nvidia_investment_in_customer", fiscal_period="event 2026-07-13", source=s,
    amount={"value": 9.3, "currency": None, "units": "percent", "basis": "nvidia_investment"},
    unknowns=["22,256,412 Class A shares per the critic's reading of the cover page; dollar value not stated"],
    dedup_group="named-investments-in-aggregate")
# v1 (Fable review B3): the Q2 cash-flow fall also reflects tax timing
add(id="nvda-10q-q2fy27-federal-tax-payments-timing", fiscal_period="Q2 FY2027 (ended 2026-07-26)",
    source=src10q("MD&A - Liquidity", "We made two federal income tax payments in the second quarter of fiscal year 2027, as compared with no estimated tax payments in the first quarter of fiscal year 2027."),
    amount={"value": None, "currency": None, "units": None, "basis": "cash_flow"},
    unknowns=["the dollar amount of the two payments is not stated in this sentence; Q1 FY27 cash flow was flattered and Q2 depressed by this timing"])
cfo = copy.deepcopy(recs["nvda-cfo-q2fy27-dso"]["source"])
cfo["section"] = "CFO commentary - cash flow"
cfo["quote"] = "the sequential decrease was driven by higher working capital adjustments and cash taxes."
add(id="nvda-cfo-q2fy27-ocf-decrease-drivers", fiscal_period="Q2 FY2027 (ended 2026-07-26)", source=cfo,
    amount={"value": None, "currency": None, "units": None, "basis": "cash_flow"},
    unknowns=["no split between working capital and cash taxes is given"])
# supply commitments: correct the 'primarily memory' wording
r = recs["nvda-10q-q2fy27-supply-commitments-text"]
r["source"]["quote"] = "These supply commitments are for our data center infrastructure systems, primarily memory and manufacturing facilities, to produce our products for long-term demand across current and future product architectures."
note("nvda-10q-q2fy27-supply-commitments", "the 10-Q says 'primarily memory and manufacturing facilities'; only the CFO says the increase was primarily memory; buckets 92/87/88/6/5/1 (rest of FY27 .. FY32+); agreements 'may be cancelable, rescheduled, or adjustable ... prior to placing firm orders'")
with open(S / "sec-records-reviewed.jsonl", "w", encoding="utf-8") as f:
    for i in order:
        f.write(json.dumps(recs[i], ensure_ascii=False) + "\n")
print(len(order), "reviewed SEC records")
