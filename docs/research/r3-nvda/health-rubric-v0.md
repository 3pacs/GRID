# R3 counterparty health check: rubric (written 2026-11-13, BEFORE any health data was gathered)

Question: are the companies NVIDIA has liabilities, guarantees, buy-backs, receivables or stakes with healthy enough that the R3 **base case** stands?

Base case (`docs/research/r3-nvda/scenarios-v0.json`):
- Q3 FY27 revenue at the $108.0B guide.
- Then +6% / +4% / +3% quarter on quarter.
- FCF margin 40%.
- 28× forward FCF.

The base case assumes:
1. customers keep buying at roughly the guided pace for 4 quarters;
2. receivables are collected, so FCF margin recovers from 22% toward 40%;
3. no material cash call on NVIDIA's guarantees or capacity buy-backs in the next 4 quarters.

## NVIDIA's exposure by counterparty (from the ledger)

| Counterparty | NVIDIA exposure |
|---|---|
| OpenAI | Up to $105B guarantee cap on PORTS-Pike leases (takes effect from FY2029; triggered by OpenAI default). 10 GW LOI, non-binding. Reported ~$30B equity stake (secondary). End customer behind Oracle, CoreWeave, Microsoft and AWS capacity. Probably the unnamed "AI research and deployment company" revenue contributor. |
| SB Energy (SoftBank Group) | Lessor and developer at PORTS-Pike; $1.5B NVIDIA investment. SoftBank is also a major OpenAI funder. |
| CoreWeave | NVIDIA capacity buy-back order ($6.3B initial, to 2032). 11.5% stake ($4.7B at 06-30). Large direct GPU buyer (receivables). |
| Nscale | $1.2B NVIDIA cloud-services agreement. NVIDIA lease guarantee up to $860M. About $2.9B of NVIDIA investments. In IPO registration (S-1/A). |
| Nebius | 9.3% stake; $2B pre-funded warrants; GPU buyer. |
| Oracle | Large GPU buyer. RPO $664B, much of it OpenAI. Says "vendor financing, customer-provided hardware and customer prepayments" will reduce its upfront cash needs. |
| Anthropic | Up to $10B NVIDIA investment; buys NVIDIA capacity through Azure and Nscale. |
| xAI / SpaceX ("SpaceXAI") | $21.0B SpaceX stake at 06-30; xAI investment amount undisclosed; deploys NVIDIA systems. |
| Intel | $30.0B stake (supplier and partner, not a customer): mark-to-market only. |
| Microsoft, Meta, Alphabet, Amazon | Largest customers. One direct customer was 16% of Q2 revenue. Extended payment terms to "investment-grade" customers. |
| Unnamed AI clouds | $36B AI-cloud buy-back agreements, $3.5B lease guarantees, $20B third-party leases. Identities are not disclosed. Judge them through the named neoclouds. |

## Metrics per counterparty

Take each metric from primary sources where they exist (SEC filings, company IR or newsroom). Use the latest period public before 2026-11-17 21:00Z.

- **Liquidity:** cash and equivalents, undrawn facilities, and cash burn (operating cash flow minus capex). Runway in months at the latest burn.
- **Debt:** total debt; maturities within the next 12 months; interest expense vs operating income or EBITDA (coverage); covenants; any waiver or breach; credit rating and outlook, if the company publishes it.
- **Obligations vs funding:** purchase and lease commitments and capex guidance vs cash plus committed financing.
- **Revenue quality:** growth; backlog or RPO; customer concentration (especially dependence on OpenAI or Microsoft).
- **Distress signals:** going-concern language, material weakness, impairments, contract terminations, delayed sites, failed or repriced financing, layoffs, auditor changes.
- **Access to capital in 2026:** rounds or debt raised, size, terms and pricing where stated.

## Grades

| Grade | Meaning |
|---|---|
| GREEN | Runway ≥ 24 months, or self-funding. No 12-month maturity wall. No distress signal. |
| AMBER | Runway 12–24 months, or dependent on new external financing to meet stated commitments. Or high concentration on one customer whose own grade is AMBER/RED. Or one distress signal. |
| RED | Runway < 12 months. Or going-concern language, a covenant breach, a failed financing, or a material contract termination that hits NVIDIA-related capacity. |
| UNKNOWN | No primary data, e.g. a private company with no filings. Must say what is missing. |

## Decision rule for the base case

Fixed now, before results.

- **Base case STANDS** if:
  - no counterparty with a 4-quarter cash or revenue exposure is RED; and
  - the counterparties NVIDIA most depends on for revenue (the hyperscalers) are GREEN; and
  - the AMBER group's combined exposure is not large enough to move base-case FCF margin from 40% to below 30%.

  Use as a yardstick: base-case FCF is about $186B over 4 quarters. 10 percentage points of FCF margin is about $46B.
- **Base case is DOWNGRADED** (report which assumption fails) if any 4-quarter-exposure counterparty is RED, or AMBER exposure plausibly exceeds about $46B of FCF over 4 quarters. Such exposure means delayed collections, buy-back payments or guarantee calls.
- **UNKNOWN does not count as GREEN.** If the largest exposure (OpenAI) is UNKNOWN, report the base case as "not supported by evidence" for that part. Do not call it either STANDS or DOWNGRADED.
- **Exposures that begin after the 4-quarter window** (the PORTS-Pike guarantees from FY2029) do not change the 4-quarter base case. They are reported separately as long-dated tail risk.

## Output (per agent)

1. **Health facts as JSON Lines**, following `RECORD_SCHEMA.md` exactly:
   - `record_kind` "financial_fact", `stage` "financial_fact", `direction` "financing";
   - `issuer` = the company; `counterparty` null;
   - basis balance_sheet / cash_flow / unknown.

   Verbatim quotes, sha256 of raw downloads, published_at = SEC acceptance time or release time. Ids are prefixed `health-<company>-`.
2. **One assessment JSON object per company:** {company, grade, metrics: {runway_months, cash_bn, debt_bn, maturities_12m_bn, interest_coverage, ...} (null if not stated), distress_signals: [...], nvidia_exposure_note, evidence_ids: [...], missing: [...], reasoning}. The grade follows the rubric above; say which rule decided it.
