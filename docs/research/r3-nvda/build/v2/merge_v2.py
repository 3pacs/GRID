"""v2: rebuild the R3 ledger from v1's inputs plus the fixes from the #1150 re-review.

Run after merge_v1.py has written the v1 ledger. Reads that ledger, applies the v2
changes, and writes canonical JSON Lines. Every time it sets comes from a file on
disk, never typed in.
"""
import copy, datetime, glob, hashlib, json, os, re, sys
from pathlib import Path
S = Path(sys.argv[1]); V1 = Path(sys.argv[2]); OUT = Path(sys.argv[3])
recs = [json.loads(l) for l in V1.read_text(encoding="utf-8").splitlines() if l.strip()]
idx = {r["id"]: r for r in recs}

def mtime(f):
    return datetime.datetime.fromtimestamp(os.path.getmtime(f), datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# 1. retrieval time = earliest real download of exactly the hashed bytes
first = {}
for d in ("critic-cp-dl", "critic-dl", "dl-cp", "dl-sec", "dl-8k", "health-labs-dl", "health-neo-dl", "health-ora-dl"):
    for f in glob.glob(str(S / d / "**" / "*"), recursive=True):
        if os.path.isfile(f):
            h = hashlib.sha256(open(f, "rb").read()).hexdigest()
            t = mtime(f)
            if h not in first or t < first[h]:
                first[h] = t
changed = 0
for r in recs:
    s = r["source"]; h = s["retrieved_sha256"]
    if h and h in first and s["retrieved_at"] != first[h]:
        s["retrieved_at"] = first[h]; changed += 1
        r["unknowns"].append("[v2] retrieved_at = earliest download time of exactly these hashed bytes in the research area")
    elif not h and r["confidence"] == "secondary_only" and s["retrieved_at"] is not None:
        s["retrieved_at"] = None
        r["unknowns"].append("[v2] no bytes kept, so no verifiable retrieval time (left null)")
print("retrieval times re-derived:", changed)

# 2. firmness: supply is only partly cancellable; two firm lines are maxima
idx["nvda-10q-q2fy27-supply-commitments"]["firmness"] = "partly_cancellable"
idx["nvda-10q-q2fy27-supply-commitments"]["unknowns"].append(
    "[v2] 10-Q: 'in certain instances, these agreements may be cancelable, rescheduled, or adjustable ... Changes to these agreements may result in additional costs'; risk factors: 'we may be unable to reduce these commitments'. The cancellable share is not disclosed")
idx["nvda-10q-q2fy27-ai-cloud-agreements"]["unknowns"].append("[v2] firm as a contractual maximum; it decreases as capacity is used by third-party customers")
idx["nvda-10q-q2fy27-dc-leases-third-party"]["unknowns"].append("[v2] firm as a contractual maximum; the leases are expected to be reassigned")

# 3. owner records: the critic only noted the filing was missing; it did not check these records
for i in ("nvda-8k-20261112-ceo-resignation", "nvda-8k-20261112-ex991-commitments-continue"):
    idx[i]["review"] = {"by": "owner; critic-sec OMISSION-11 confirmed the event from the main 8-K but did not check this record; the #1150 review matched its quote and hash at source",
                        "verdict": "NOT_CHECKED", "finding": None}

# 4. new records
def T(f):
    t = open(f, encoding="utf-8", errors="replace").read()
    return re.sub(r"\s+", " ", t)
q2txt = T(S / "critic-txt" / "000104581026000075__nvda-20260726.txt")
q = "If future demand, our needs or plans differ from our expectations, we may be unable to reduce these commitments."
assert q in q2txt
r = copy.deepcopy(idx["nvda-10q-q2fy27-federal-tax-payments-timing"])
r.update(id="nvda-10q-q2fy27-risk-unable-to-reduce-commitments", fiscal_period="Q2 FY2027 (ended 2026-07-26)", unknowns=["risk factor; qualifies the supply commitments' cancellability"],
         review={"by": "owner (quote verified against the critic's re-fetched 10-Q text)", "verdict": "NOT_CHECKED", "finding": None})
r["source"]["section"] = "Risk factors"; r["source"]["quote"] = q
recs.append(r)
pres = S / "health-ora-txt" / "sbg-earnings-presentation_q2fy2026_01_en.txt"
qs = "The $3.7B loss is completely unrelated to PORTS-Pike"
assert qs in T(pres)
base = idx["health-softbank-ltv-sep2026"]
r = copy.deepcopy(base)
r.update(id="health-softbank-sb7-loss-unrelated-to-portspike", counted=False, dedup_group=None,
         amount={"value": 3.7, "currency": "USD", "units": "billions", "basis": "unknown"},
         unknowns=["slide 17 of the earnings presentation. The financial report says the equipment was for AI data center projects 'including the PORTS-Pike Technology Campus', and a separate slide says $5.6B of remaining advances are to be recouped from the PORTS-Pike power generation project; the loss itself is stated as unrelated"],
         review={"by": "owner (quote verified against the health worker's download); the #1150 review matched it at source", "verdict": "NOT_CHECKED", "finding": None})
r["source"]["section"] = "Earnings presentation, slide 17"; r["source"]["quote"] = qs
recs.append(r)

recs.sort(key=lambda x: x["id"])
OUT.write_text("".join(json.dumps(x, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n" for x in recs), encoding="utf-8")
print(len(recs), "records ->", OUT)
