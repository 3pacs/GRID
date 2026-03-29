"""
GRID Intelligence -- Big Pharma Power Network.

Structured intelligence report covering the top 10 pharma companies:
CEO profiles, drug franchises, patent cliffs, lobbying, pricing
controversies, insider trading patterns, offshore IP structures,
and PBM relationships.

Generated: 2026-03-28
"""

PHARMA_POWER_NETWORK = {
    "meta": {
        "report_id": "PHARMA-POWER-NET-2026-03-28",
        "generated": "2026-03-28",
        "confidence_schema": "confirmed | derived | estimated | rumored | inferred",
        "data_sources": [
            "SEC EDGAR (Form 4, 13F, proxy filings)",
            "OpenSecrets.org (lobbying, campaign finance)",
            "FDA (pipeline, approvals)",
            "Company 10-K / earnings releases",
            "Senate Finance Committee investigations",
            "ICIJ (offshore leaks)",
            "FTC (PBM enforcement actions)",
            "I-MAK (patent thicket analysis)",
            "CMS (Medicare negotiation data)",
        ],
    },

    # ================================================================
    # 1. ELI LILLY (LLY) -- GLP-1 Dominance
    # ================================================================
    "LLY": {
        "company": "Eli Lilly and Company",
        "ticker": "LLY",
        "hq": "Indianapolis, Indiana",
        "ceo": {
            "name": "David A. Ricks",
            "title": "Chairman, President & CEO",
            "tenure_start": 2017,
            "compensation_2025_total": 36_700_000,
            "compensation_2024_total": 29_200_000,
            "compensation_notes": "20%+ YoY increase tied to GLP-1 revenue explosion; 67% equity-based",
            "confidence": "confirmed",
        },
        "top_drugs": [
            {
                "name": "Mounjaro (tirzepatide)",
                "indication": "Type 2 diabetes",
                "revenue_2025": 30_000_000_000,
                "revenue_2025_q4": 7_400_000_000,
                "patent_expiry_us": "2036 (base compound); thicket extends to 2040+",
                "confidence": "confirmed (revenue); estimated (patent thicket end)",
            },
            {
                "name": "Zepbound (tirzepatide)",
                "indication": "Obesity / weight management",
                "revenue_2025": 14_000_000_000,
                "revenue_2025_q4": 4_300_000_000,
                "patent_expiry_us": "Same compound as Mounjaro -- shared patent estate",
                "confidence": "confirmed (revenue); confirmed (patent linkage)",
            },
            {
                "name": "Verzenio (abemaciclib)",
                "indication": "Breast cancer (CDK4/6 inhibitor)",
                "revenue_2025_est": 4_500_000_000,
                "patent_expiry_us": "2035",
                "confidence": "estimated (revenue); confirmed (patent)",
            },
        ],
        "total_revenue_2025": 65_200_000_000,
        "revenue_guidance_2026": {"low": 80_000_000_000, "high": 83_000_000_000},
        "fda_pipeline_phase3": [
            {
                "drug": "Orforglipron",
                "indication": "Oral GLP-1 for obesity/T2D",
                "expected_approval": "2026 H2",
                "confidence": "confirmed (Phase 3 complete); estimated (approval timing)",
            },
            {
                "drug": "Retatrutide",
                "indication": "Triple-agonist (GIP/GLP-1/glucagon) for obesity",
                "expected_approval": "2027",
                "confidence": "confirmed (Phase 3 ongoing); estimated (approval)",
            },
            {
                "drug": "Donanemab (Kisunla)",
                "indication": "Alzheimer's disease",
                "expected_approval": "Approved July 2024",
                "confidence": "confirmed",
            },
        ],
        "lobbying": {
            "spend_2024": 11_300_000,
            "spend_2025_est": 13_000_000,
            "top_recipients": [
                "PhRMA trade group ($4M+ to American Action Network)",
                "Trump 2025 Inaugural Committee ($500K-$1M)",
                "Senate Finance Committee members (bipartisan)",
            ],
            "confidence": "confirmed (inaugural); estimated (annual spend); derived (recipients)",
        },
        "drug_pricing_controversies": [
            {
                "issue": "MFN pricing agreement with Trump admin Nov 2025",
                "detail": "Agreed to price Mounjaro/Zepbound at $245/month for Medicare; retail remains ~$1000+/month for uninsured",
                "confidence": "confirmed",
            },
            {
                "issue": "GLP-1 manufacturing cost vs retail price",
                "detail": "Estimated manufacturing cost ~$5/month vs list price ~$1000/month; 200x markup",
                "confidence": "estimated (manufacturing cost from Yale study); confirmed (list price)",
            },
        ],
        "insider_trading_12mo": {
            "ceo_activity": "Ricks purchased 1,632 shares Aug 2025 (bullish signal); exercised 31,932 RSUs Feb 2026, disposed 14,296 shares at $1,037/share for tax",
            "pattern": "Net buyer -- unusual for pharma CEO. Signals extreme confidence in GLP-1 trajectory",
            "confidence": "confirmed (Form 4 filings)",
        },
        "offshore_ip": {
            "ireland": "Eli Lilly Holdings Ltd (Cork) -- holds significant IP for international sales",
            "structure": "Standard Irish sandwich with Dutch conduit until 2020 BEPS reforms; now direct Irish holding",
            "effective_tax_rate_2025": "14.5% (vs 21% US statutory)",
            "confidence": "derived (from 10-K geographic revenue breakdown); estimated (ETR)",
        },
        "pbm_connections": {
            "express_scripts": "Preferred formulary placement for Mounjaro; rebate structure undisclosed",
            "cvs_caremark": "Zepbound added to CVS formulary in 2024 after competitive exclusion initially",
            "optumrx": "Preferred GLP-1 on OptumRx formulary; significant rebate likely >30%",
            "confidence": "confirmed (formulary placement); estimated (rebate levels)",
        },
    },

    # ================================================================
    # 2. NOVO NORDISK (NVO) -- Ozempic / Wegovy Empire
    # ================================================================
    "NVO": {
        "company": "Novo Nordisk A/S",
        "ticker": "NVO",
        "hq": "Bagsvaerd, Denmark",
        "ceo": {
            "name": "Maziar Mike Doustdar",
            "title": "President & CEO",
            "tenure_start": "August 2025",
            "predecessor": "Lars Fruergaard Jorgensen (2017-Aug 2025)",
            "predecessor_exit_payment": 123_600_000,  # DKK
            "predecessor_exit_payment_usd": 17_300_000,
            "compensation_2025_est": "Not yet disclosed (first partial year)",
            "jorgensen_compensation_2024": 57_100_000,  # DKK ~$7.97M
            "notes": "First non-Danish CEO in 100+ years; appointed amid stock price collapse and GLP-1 competition pressure",
            "confidence": "confirmed (appointment, predecessor pay); estimated (exit payment USD conversion)",
        },
        "danish_sovereign_connections": {
            "novo_nordisk_foundation": {
                "type": "Independent enterprise foundation (NOT government sovereign wealth fund)",
                "controlling_stake": "Majority voting control via Novo Holdings A/S",
                "net_worth_2026": 220_000_000_000,
                "status": "Largest charitable foundation in the world",
                "investment_arm": "Novo Holdings -- manages $120B+ in life science investments",
                "confidence": "confirmed",
            },
            "danish_gdp_impact": {
                "detail": "Novo Nordisk alone represents ~2% of Denmark's GDP; Foundation's endowment exceeds Danish sovereign reserves",
                "geopolitical_risk": "Company health is a matter of Danish national interest -- implicit state backing",
                "confidence": "derived (GDP calculation); inferred (state backing)",
            },
            "norway_sovereign_wealth_fund": {
                "detail": "Norges Bank Investment Management (NBIM) is a significant NVO shareholder; publicly criticized Novo board shakeup in Nov 2025",
                "confidence": "confirmed",
            },
        },
        "top_drugs": [
            {
                "name": "Ozempic (semaglutide injection)",
                "indication": "Type 2 diabetes",
                "revenue_2025": 20_000_000_000,  # ~127B DKK
                "patent_expiry_us": "2032",
                "patent_expiry_eu": "2031",
                "patent_expiry_canada": "2026 (lost protection due to unpaid fee)",
                "patent_thicket": "I-MAK identified 100+ patent filings on semaglutide",
                "confidence": "confirmed (revenue, patent dates); confirmed (Canada loss)",
            },
            {
                "name": "Wegovy (semaglutide injection, higher dose)",
                "indication": "Obesity / weight management / CV risk reduction",
                "revenue_2025": 13_000_000_000,  # ~79.1B DKK
                "patent_expiry_us": "2032 (linked to Ozempic compound patents)",
                "notes": "Wegovy HD (higher dose) approved by FDA for enhanced weight loss",
                "confidence": "confirmed",
            },
            {
                "name": "Rybelsus (oral semaglutide)",
                "indication": "Type 2 diabetes (oral formulation)",
                "revenue_2025_est": 4_500_000_000,
                "patent_expiry_us": "2031-2032",
                "confidence": "estimated (revenue); confirmed (patent range)",
            },
        ],
        "total_revenue_2025_est": 52_000_000_000,
        "revenue_guidance_2026": "Negative -- projected 5-13% sales decline",
        "fda_pipeline_phase3": [
            {
                "drug": "CagriSema (cagrilintide + semaglutide)",
                "indication": "Obesity -- next-gen combination",
                "expected_approval": "2026 (FDA accepted filing)",
                "notes": "Phase 3 REDEFINE trials showed 22-25% weight loss",
                "confidence": "confirmed",
            },
            {
                "drug": "Amycretin",
                "indication": "Oral GLP-1/amylin dual agonist for obesity",
                "expected_approval": "2027-2028",
                "confidence": "confirmed (Phase 3 initiated); estimated (approval)",
            },
        ],
        "lobbying": {
            "spend_2024_est": 8_500_000,
            "top_recipients": [
                "PhRMA (as foreign member)",
                "Trump 2025 Inaugural Committee",
                "Senate HELP Committee members",
            ],
            "confidence": "estimated (spend); confirmed (inaugural donation reported)",
        },
        "drug_pricing_controversies": [
            {
                "issue": "MFN pricing deal Nov 2025",
                "detail": "Agreed to $245/month for Ozempic/Wegovy via Medicare; $350/month via TrumpRx platform launching 2026",
                "confidence": "confirmed",
            },
            {
                "issue": "Insulin pricing",
                "detail": "Committed to $35/month cap on NovoLog and Tresiba for Medicare",
                "confidence": "confirmed",
            },
            {
                "issue": "Global price disparity",
                "detail": "Ozempic costs ~$100/month in Denmark, $900+/month in US pre-negotiation",
                "confidence": "confirmed",
            },
        ],
        "insider_trading_12mo": {
            "pattern": "CEO transition period -- Jorgensen exit package triggered large share dispositions; Doustdar accumulating through grants",
            "confidence": "derived (from public filings and press)",
        },
        "offshore_ip": {
            "denmark": "Headquartered in Denmark (12.5-22% corporate tax)",
            "structure": "Danish foundation structure provides tax-efficient reinvestment; no classic Irish sandwich needed",
            "notes": "Foundation structure is itself a form of sovereign-adjacent tax optimization -- profits never leave foundation ecosystem",
            "confidence": "confirmed (structure); inferred (tax optimization intent)",
        },
        "pbm_connections": {
            "express_scripts": "Ozempic maintained preferred status; aggressive rebating vs Mounjaro",
            "cvs_caremark": "Wegovy preferred over Zepbound on CVS formulary for 2025",
            "optumrx": "Ozempic/Wegovy on formulary but facing Lilly competitive pressure",
            "notes": "PBM formulary wars between NVO and LLY are the most intense in pharma -- estimated rebates 30-50%",
            "confidence": "confirmed (formulary status); estimated (rebate range)",
        },
    },

    # ================================================================
    # 3. PFIZER (PFE) -- Post-COVID Rebuild
    # ================================================================
    "PFE": {
        "company": "Pfizer Inc.",
        "ticker": "PFE",
        "hq": "New York, New York",
        "ceo": {
            "name": "Albert Bourla",
            "title": "Chairman & CEO",
            "tenure_start": 2019,
            "compensation_2025_total": 27_600_000,
            "compensation_breakdown": {
                "salary": 1_800_000,
                "stock_awards": 9_400_000,
                "options": 9_000_000,
                "annual_incentive": 5_400_000,
                "other": 1_900_000,
            },
            "notes": "Second-largest pay package since becoming CEO; driven by Metsera acquisition and MFN deal",
            "confidence": "confirmed",
        },
        "top_drugs": [
            {
                "name": "Eliquis (apixaban)",
                "indication": "Blood thinner (anticoagulant)",
                "revenue_2025_est": 7_000_000_000,
                "patent_expiry_us": "2026 (generics expected)",
                "notes": "Co-marketed with Bristol-Myers Squibb; single largest revenue source",
                "confidence": "confirmed (patent); estimated (2025 revenue)",
            },
            {
                "name": "Prevnar 20 (pneumococcal vaccine)",
                "indication": "Pneumococcal disease prevention",
                "revenue_2025_est": 6_500_000_000,
                "patent_expiry_us": "2033",
                "confidence": "estimated",
            },
            {
                "name": "Ibrance (palbociclib)",
                "indication": "Breast cancer (CDK4/6 inhibitor)",
                "revenue_2025_est": 4_800_000_000,
                "patent_expiry_us": "March 2027 (extended from 2023)",
                "confidence": "confirmed (patent extension); estimated (revenue)",
            },
        ],
        "total_revenue_2025": 62_500_000_000,
        "patent_cliff": {
            "detail": "7 franchises losing protection 2025-2028; $17-18B annual revenue at risk",
            "key_expirations": {
                "Xeljanz": 2025,
                "Prevnar 13": 2026,
                "Eliquis": 2026,
                "Ibrance": 2027,
                "Xtandi": 2027,
            },
            "confidence": "confirmed",
        },
        "fda_pipeline_phase3": [
            {
                "drug": "Danuglipron",
                "indication": "Oral GLP-1 for obesity",
                "expected_approval": "2027 (if Phase 3 succeeds)",
                "notes": "Pfizer acquired Metsera for $10B to bolster obesity pipeline after prior GLP-1 failures",
                "confidence": "confirmed (acquisition); estimated (approval)",
            },
            {
                "drug": "Abrysvo (expanded indications)",
                "indication": "RSV vaccine -- broader age ranges",
                "expected_approval": "2026",
                "confidence": "confirmed",
            },
            {
                "drug": "Atirmociclib",
                "indication": "CDK4 inhibitor (next-gen Ibrance)",
                "expected_approval": "2027",
                "confidence": "estimated",
            },
        ],
        "lobbying": {
            "spend_2024": 13_300_000,
            "spend_2025_est": 15_000_000,
            "pac_contributions_2024": 2_800_000,
            "pac_split": {"democrats": 1_600_000, "republicans": 1_100_000},
            "trump_inaugural_2025": "500K-1M",
            "confidence": "confirmed (PAC data, inaugural); estimated (2025 spend)",
        },
        "drug_pricing_controversies": [
            {
                "issue": "COVID vaccine pricing",
                "detail": "Shifted from $20/dose government contract to $110-130/dose commercial pricing in 2023",
                "confidence": "confirmed",
            },
            {
                "issue": "Eliquis Medicare negotiation",
                "detail": "One of first 10 drugs selected for IRA Medicare negotiation; negotiated price effective Jan 2026",
                "confidence": "confirmed",
            },
        ],
        "insider_trading_12mo": {
            "pattern": "Multiple executives selling in small tranches; SVP Jennifer Damico notable March 2025 sale; 9 transactions totaling ~$600K; mostly equity award exercises",
            "signal": "Neutral -- routine 10b5-1 plan activity, no cluster buying",
            "confidence": "confirmed (Form 4 filings)",
        },
        "offshore_ip": {
            "ireland": "Pfizer Ireland Pharmaceuticals (Cork) -- major IP holding entity",
            "netherlands": "CP Pharmaceuticals BV -- Dutch entity provides blanket guarantee for Irish subs, exempting them from Irish filing requirements",
            "senate_investigation": "Senate Finance Committee (Wyden) called it potentially 'largest tax-dodging scheme' in pharma history",
            "tax_impact": "Pfizer claimed negative US taxes while earning billions; part of group that made $429B in profits but paid near-zero US tax since 2018",
            "delaware_conduits": "Two Delaware holding companies serve as conduits for IP income from Irish entities",
            "confidence": "confirmed (Senate investigation, corporate filings)",
        },
        "pbm_connections": {
            "express_scripts": "Eliquis preferred on ES formulary; Paxlovid facing removal pressure",
            "cvs_caremark": "Prevnar franchise maintained; COVID products declining",
            "optumrx": "Standard formulary placement",
            "notes": "Pfizer less dependent on PBM formulary wars than GLP-1 makers",
            "confidence": "confirmed (formulary data); derived (dependency assessment)",
        },
    },

    # ================================================================
    # 4. JOHNSON & JOHNSON (JNJ) -- Talc Litigation & Consumer Split
    # ================================================================
    "JNJ": {
        "company": "Johnson & Johnson",
        "ticker": "JNJ",
        "hq": "New Brunswick, New Jersey",
        "ceo": {
            "name": "Joaquin Duato",
            "title": "Chairman & CEO",
            "tenure_start": 2022,
            "compensation_2025_total": 32_800_000,
            "compensation_2024_total": 24_300_000,
            "pay_ratio": "360:1 CEO-to-median-worker",
            "notes": "35% pay increase YoY; compensation committee excluded $7B talc settlement reversal from incentive calculations to avoid 'windfall'",
            "confidence": "confirmed",
        },
        "top_drugs": [
            {
                "name": "Darzalex (daratumumab)",
                "indication": "Multiple myeloma",
                "revenue_2025_est": 12_500_000_000,
                "revenue_2024": 11_700_000_000,
                "patent_expiry_us": "2029-2031 (thicket)",
                "notes": "Now J&J's #1 seller, surpassing Stelara",
                "confidence": "confirmed (2024 revenue); estimated (2025, patent range)",
            },
            {
                "name": "Stelara (ustekinumab)",
                "indication": "Psoriasis, Crohn's, UC",
                "revenue_2025_est": 5_000_000_000,
                "patent_expiry_us": "2023 (biosimilar competition began 2025)",
                "notes": "Rapid revenue decline as biosimilars enter; was $10B+ peak franchise",
                "confidence": "confirmed (patent cliff); estimated (declining revenue)",
            },
            {
                "name": "Tremfya (guselkumab)",
                "indication": "Psoriasis, psoriatic arthritis",
                "revenue_2025_est": 4_200_000_000,
                "patent_expiry_us": "2034",
                "notes": "Positioned as Stelara successor; subcutaneous formulation",
                "confidence": "estimated",
            },
        ],
        "consumer_business_split": {
            "entity": "Kenvue (KVUE)",
            "split_date": "August 2023",
            "detail": "J&J spun off consumer health (Tylenol, Band-Aid, Listerine, etc.) into Kenvue; J&J retains pharma + MedTech",
            "strategic_motive": "Separate talc liability from pharma cash flows; shield pharma business from consumer litigation",
            "confidence": "confirmed (split); inferred (strategic motive)",
        },
        "talc_litigation": {
            "status": "Ongoing -- 3rd bankruptcy attempt rejected April 2025",
            "largest_single_verdict": {
                "amount": 1_560_000_000,
                "plaintiff": "Cherie Craft (mesothelioma)",
                "date": "December 22, 2025",
                "court": "Baltimore City",
            },
            "other_notable_verdict": {
                "amount": 966_000_000,
                "plaintiff": "Mae Moore family (mesothelioma, deceased)",
                "date": "October 2025",
                "court": "Los Angeles",
            },
            "total_liability_estimated": "25-40B depending on remaining cases",
            "strategy": "J&J announced Jan 2026 it will return to tort system to 'defeat meritless claims' individually after bankruptcy strategy failed",
            "confidence": "confirmed (verdicts, bankruptcy rejection); estimated (total liability range)",
        },
        "fda_pipeline_phase3": [
            {
                "drug": "Nipocalimab",
                "indication": "Generalized myasthenia gravis, hemolytic disease",
                "expected_approval": "2026",
                "confidence": "confirmed (Phase 3); estimated (approval timing)",
            },
            {
                "drug": "RYBREVANT (amivantamab) + lazertinib",
                "indication": "NSCLC (first-line)",
                "expected_approval": "Approved 2024 (expanded indications 2026)",
                "confidence": "confirmed",
            },
        ],
        "lobbying": {
            "spend_2024": 9_800_000,
            "spend_2025_est": 11_000_000,
            "trump_inaugural_2025": "500K-1M",
            "focus": "Tort reform, FDA modernization, patent reform",
            "confidence": "estimated (spend); confirmed (inaugural)",
        },
        "drug_pricing_controversies": [
            {
                "issue": "Stelara Medicare negotiation",
                "detail": "Stelara selected as one of first 10 drugs for IRA Medicare price negotiation; negotiated price effective Jan 2026",
                "confidence": "confirmed",
            },
        ],
        "insider_trading_12mo": {
            "pattern": "Routine 10b5-1 plan sales; no unusual cluster activity",
            "confidence": "derived (from public filings)",
        },
        "offshore_ip": {
            "ireland": "Janssen Sciences Ireland UC -- primary EU IP holding entity",
            "total_irish_affiliate_assets": 102_000_000_000,
            "structure": "Multi-entity Irish structure with Swiss tax residency for certain subs",
            "senate_investigation": "Included in Wyden Senate Finance Committee pharma tax probe",
            "confidence": "confirmed (Investigate Europe data, Senate investigation)",
        },
        "pbm_connections": {
            "express_scripts": "Darzalex specialty pharmacy distribution; Stelara losing preferred status to biosimilars",
            "cvs_caremark": "2025 formulary excluded Stelara in favor of biosimilar options",
            "optumrx": "Specialty drug distribution partnerships",
            "confidence": "confirmed (formulary exclusions); derived (distribution)",
        },
    },

    # ================================================================
    # 5. MERCK (MRK) -- Keytruda Franchise at Risk
    # ================================================================
    "MRK": {
        "company": "Merck & Co., Inc.",
        "ticker": "MRK",
        "hq": "Rahway, New Jersey",
        "ceo": {
            "name": "Robert M. Davis",
            "title": "Chairman, President & CEO",
            "tenure_start": 2021,
            "compensation_2024_total": 23_200_000,
            "compensation_2025_est": 25_000_000,
            "compensation_trajectory": "Rapid: $13.7M (2021) -> $18.6M (2022) -> $20.3M (2023) -> $23.2M (2024)",
            "confidence": "confirmed (2024); estimated (2025)",
        },
        "top_drugs": [
            {
                "name": "Keytruda (pembrolizumab)",
                "indication": "Cancer immunotherapy (PD-1 inhibitor)",
                "revenue_2025": 31_700_000_000,
                "pct_of_total_revenue": 0.49,
                "patent_expiry_us": "December 2028 (original); Keytruda Qlex (subQ) patents extend to 2041",
                "notes": "Davis calls LOE 'more of a hill than a cliff' due to Qlex patent extension; expected $35B peak in 2028",
                "subcutaneous_approved": "September 2025 (Keytruda Qlex)",
                "confidence": "confirmed",
            },
            {
                "name": "Gardasil/Gardasil 9 (HPV vaccine)",
                "indication": "HPV prevention / cervical cancer",
                "revenue_2025": 5_200_000_000,
                "yoy_change": "-39% (China demand collapse)",
                "patent_expiry_us": "2028",
                "confidence": "confirmed",
            },
            {
                "name": "Januvia/Janumet (sitagliptin)",
                "indication": "Type 2 diabetes (DPP-4 inhibitor)",
                "revenue_2025_est": 2_800_000_000,
                "patent_expiry_us": "2026 (generics imminent)",
                "notes": "Legacy franchise being replaced by GLP-1s; higher US net pricing supporting declining volume",
                "confidence": "confirmed (patent); estimated (revenue)",
            },
        ],
        "total_revenue_2025": 65_000_000_000,
        "keytruda_cliff_strategy": {
            "cost_cutting": "$3B initiative announced to prepare for LOE",
            "post_keytruda_opportunity": "$70B+ in commercial opportunities by mid-2030s per CEO",
            "winrevair_ramp": "Winrevair (sotatercept) for PAH reached $1.4B in first full year",
            "acquisitions": "Acquired Verona Pharma (respiratory) mid-2025",
            "confidence": "confirmed",
        },
        "fda_pipeline_phase3": [
            {
                "drug": "Keytruda Qlex (subcutaneous)",
                "indication": "Same as Keytruda -- new formulation extending exclusivity",
                "expected_approval": "Approved September 2025",
                "patent_protection": "Through 2041",
                "confidence": "confirmed",
            },
            {
                "drug": "Winrevair (expanded indications)",
                "indication": "PAH -- broader patient population",
                "expected_approval": "2026-2027",
                "confidence": "estimated",
            },
            {
                "drug": "MK-1684 (TEAD inhibitor)",
                "indication": "Mesothelioma, solid tumors",
                "expected_approval": "2027",
                "confidence": "estimated",
            },
        ],
        "lobbying": {
            "spend_2024_est": 11_500_000,
            "pac_contributions_2024": 2_000_000,
            "pac_split": {"democrats": 1_500_000, "republicans": 728_000},
            "trump_inaugural_2025": "500K-1M",
            "focus": "IRA repeal/modification, Keytruda orphan drug exclusion, patent reform",
            "confidence": "confirmed (PAC data); estimated (lobbying spend)",
        },
        "drug_pricing_controversies": [
            {
                "issue": "Keytruda orphan drug exclusion from Medicare negotiation",
                "detail": "OBBA broadened orphan drug exclusion may shield Keytruda from IRA negotiation; CBO estimates $8.8B in higher Medicare spending as result",
                "confidence": "confirmed",
            },
            {
                "issue": "Januvia pricing",
                "detail": "Januvia selected for Medicare negotiation; negotiated price effective Jan 2026",
                "confidence": "confirmed",
            },
        ],
        "insider_trading_12mo": {
            "pattern": "Routine executive sales via 10b5-1 plans; no unusual cluster activity detected",
            "confidence": "derived",
        },
        "offshore_ip": {
            "ireland": "MSD International Holdings -- Irish subsidiaries with Swiss tax residency; at least $44B in assets",
            "structure": "Classic 'Double Irish with Dutch Sandwich' structure (legacy, partially reformed post-BEPS)",
            "senate_investigation": "Subject of Wyden Senate Finance Committee pharma tax investigation 2023-2024",
            "tax_impact": "Claimed negative US taxes while earning billions in profits",
            "confidence": "confirmed (Senate investigation, Investigate Europe)",
        },
        "pbm_connections": {
            "express_scripts": "Keytruda specialty pharmacy distribution; Januvia losing formulary status to GLP-1s",
            "cvs_caremark": "Standard oncology distribution",
            "optumrx": "Specialty tier placement for Keytruda",
            "notes": "Oncology drugs less sensitive to PBM formulary negotiations than chronic disease drugs",
            "confidence": "derived",
        },
    },

    # ================================================================
    # 6. ABBVIE (ABBV) -- Post-Humira Reinvention
    # ================================================================
    "ABBV": {
        "company": "AbbVie Inc.",
        "ticker": "ABBV",
        "hq": "North Chicago, Illinois",
        "ceo": {
            "name": "Robert A. Michael",
            "title": "Chairman & CEO",
            "tenure_start": "July 2024",
            "predecessor": "Richard Gonzalez (2013-2024, 11 years)",
            "compensation_2025_total": 32_500_000,
            "compensation_2024_total": 18_500_000,
            "compensation_breakdown_2025": {
                "salary": 1_700_000,
                "stock_and_options": 16_500_000,
                "cash_bonus": 5_200_000,
            },
            "notes": "75% pay increase in second year; Gonzalez received $25.7M in final year",
            "confidence": "confirmed",
        },
        "top_drugs": [
            {
                "name": "Skyrizi (risankizumab)",
                "indication": "Psoriasis, Crohn's, UC",
                "revenue_2025_est": 14_000_000_000,
                "patent_expiry_us": "2035",
                "notes": "Primary Humira replacement; IL-23 inhibitor",
                "confidence": "estimated (revenue); confirmed (patent)",
            },
            {
                "name": "Rinvoq (upadacitinib)",
                "indication": "Rheumatoid arthritis, atopic dermatitis, UC, Crohn's",
                "revenue_2025_est": 8_500_000_000,
                "patent_expiry_us": "2036",
                "notes": "JAK inhibitor; carries FDA black box warning",
                "confidence": "estimated (revenue); confirmed (patent)",
            },
            {
                "name": "Botox (onabotulinumtoxinA)",
                "indication": "Aesthetics, migraine, overactive bladder",
                "revenue_2025_est": 5_800_000_000,
                "patent_expiry_us": "Biologic -- complex manufacturing barrier to biosimilars",
                "notes": "Acquired via $63B Allergan purchase (2019-2020)",
                "confidence": "estimated",
            },
        ],
        "humira_cliff": {
            "peak_revenue_2022": 21_200_000_000,
            "revenue_2025_est": 5_000_000_000,
            "biosimilar_entrants": "10+ biosimilars (Amgevita, Hadlima, Hyrimoz, etc.)",
            "patent_thicket": "AbbVie filed 100+ patents on Humira; 'patent thicket' strategy delayed biosimilars by ~6 years in US vs EU",
            "confidence": "confirmed (peak, biosimilar count); estimated (2025 revenue)",
        },
        "total_revenue_2025": 61_100_000_000,
        "fda_pipeline_phase3": [
            {
                "drug": "Emraclidine",
                "indication": "Schizophrenia (muscarinic agonist)",
                "expected_approval": "2027",
                "notes": "Acquired via Cerevel Therapeutics ($8.7B deal)",
                "confidence": "confirmed (Phase 3); estimated (approval)",
            },
            {
                "drug": "ABBV-400 (telisotuzumab vedotin)",
                "indication": "NSCLC",
                "expected_approval": "2026-2027",
                "confidence": "estimated",
            },
        ],
        "lobbying": {
            "spend_2024": 4_530_000,
            "spend_2025_est": 5_000_000,
            "focus": "Patent reform opposition, IRA modifications, 340B reform",
            "notes": "AbbVie historically aggressive on patent strategy lobbying",
            "confidence": "confirmed (2024 spend); estimated (2025)",
        },
        "drug_pricing_controversies": [
            {
                "issue": "Humira patent thicket",
                "detail": "AbbVie used 100+ patents to block biosimilar competition for 20+ years; US patients paid $60K+/year while EU patients had biosimilar access years earlier",
                "confidence": "confirmed (I-MAK analysis, congressional hearings)",
            },
            {
                "issue": "Gonzalez legacy",
                "detail": "Former CEO Gonzalez earned $400M+ during tenure while Humira price doubled; described as 'divisive legacy' in pharma press",
                "confidence": "confirmed (compensation data); derived (legacy assessment)",
            },
        ],
        "insider_trading_12mo": {
            "pattern": "Michael accumulating via equity grants; Gonzalez large dispositions upon retirement",
            "confidence": "derived",
        },
        "offshore_ip": {
            "ireland": "AbbVie Operations Ireland -- $308B in Irish affiliate assets (largest of any pharma)",
            "structure": "IP licensing from Irish subs to global operations; effective tax rate well below US statutory rate",
            "bermuda": "Additional structures in Bermuda and Luxembourg",
            "senate_investigation": "Target of Wyden investigation; lawmakers accused AbbVie of 'tax-dodging'",
            "confidence": "confirmed (Investigate Europe, Senate Finance Committee)",
        },
        "pbm_connections": {
            "express_scripts": "Humira excluded from 2025 formulary in favor of biosimilars; Skyrizi preferred",
            "cvs_caremark": "Rinvoq competing for preferred JAK inhibitor status",
            "optumrx": "Skyrizi/Rinvoq gaining preferred placement as Humira biosimilars proliferate",
            "notes": "AbbVie historically used rebate walls to maintain Humira formulary exclusivity; strategy unwinding",
            "confidence": "confirmed (formulary exclusions); derived (rebate strategy)",
        },
    },

    # ================================================================
    # 7. BRISTOL-MYERS SQUIBB (BMY) -- Opdivo and Patent Cliff
    # ================================================================
    "BMY": {
        "company": "Bristol-Myers Squibb Company",
        "ticker": "BMY",
        "hq": "Princeton, New Jersey",
        "ceo": {
            "name": "Christopher Boerner",
            "title": "CEO",
            "tenure_start": "November 2023",
            "compensation_2025_est": 18_790_000,
            "compensation_breakdown": "8.2% salary, 91.8% equity/bonuses",
            "confidence": "confirmed",
        },
        "top_drugs": [
            {
                "name": "Opdivo (nivolumab)",
                "indication": "Cancer immunotherapy (PD-1 inhibitor)",
                "revenue_2025_est": 9_500_000_000,
                "revenue_2026_projected": 10_000_000_000,
                "patent_expiry_us": "2028",
                "patent_expiry_eu": "June 2028",
                "confidence": "confirmed (patent dates); estimated (revenue)",
            },
            {
                "name": "Eliquis (apixaban)",
                "indication": "Blood thinner (co-marketed with Pfizer)",
                "revenue_2025_bmy_share_est": 6_500_000_000,
                "patent_expiry_us": "April 2028 (generics expected)",
                "notes": "BMS receives ~50% of Eliquis profits from Pfizer alliance",
                "confidence": "confirmed (patent); estimated (BMS share)",
            },
            {
                "name": "Revlimid (lenalidomide)",
                "indication": "Multiple myeloma",
                "revenue_2025_est": 3_500_000_000,
                "patent_expiry_us": "2025-2026 (generic entry occurring)",
                "notes": "Declining rapidly; was $12B+ peak franchise",
                "confidence": "confirmed (generic entry); estimated (revenue)",
            },
        ],
        "growth_products": [
            {
                "name": "Camzyos (mavacamten)",
                "indication": "Hypertrophic cardiomyopathy",
                "revenue_2025_est": 1_800_000_000,
                "confidence": "estimated",
            },
            {
                "name": "Reblozyl (luspatercept)",
                "indication": "Anemia (MDS, beta-thalassemia)",
                "revenue_2025_est": 1_500_000_000,
                "confidence": "estimated",
            },
            {
                "name": "Cobenfy (xanomeline-trospium)",
                "indication": "Schizophrenia (muscarinic agonist, new class)",
                "revenue_2025_est": 500_000_000,
                "notes": "First new mechanism for schizophrenia in decades; launched 2024",
                "confidence": "estimated",
            },
        ],
        "patent_cliff": {
            "revenue_at_risk": "~$15B by 2030 from Opdivo + Eliquis LOE",
            "strategy": "11 key brands to drive transition; 28 pivotal studies by end of 2028",
            "confidence": "confirmed (strategy); estimated (revenue at risk)",
        },
        "fda_pipeline_phase3": [
            {
                "drug": "Breyanzi (lisocabtagene maraleucel)",
                "indication": "CAR-T cell therapy -- expanded indications",
                "expected_approval": "2026 (new lines)",
                "confidence": "confirmed",
            },
            {
                "drug": "Opdivo + subcutaneous formulation",
                "indication": "Patent life extension strategy (like Merck's Keytruda Qlex)",
                "expected_approval": "2026-2027",
                "confidence": "estimated",
            },
            {
                "drug": "Cobenfy (expanded indications)",
                "indication": "Alzheimer's psychosis, bipolar",
                "expected_approval": "2027-2028",
                "confidence": "estimated",
            },
        ],
        "lobbying": {
            "spend_2024_est": 8_000_000,
            "focus": "IRA modifications, biosimilar interchangeability standards, patent reform",
            "confidence": "estimated",
        },
        "drug_pricing_controversies": [
            {
                "issue": "Eliquis Medicare negotiation",
                "detail": "Eliquis among first 10 drugs selected for IRA Medicare negotiation; BMS challenged constitutionality (lawsuit ongoing)",
                "confidence": "confirmed",
            },
        ],
        "insider_trading_12mo": {
            "pattern": "New CEO Boerner receiving large equity grants; limited open-market activity",
            "confidence": "derived",
        },
        "offshore_ip": {
            "ireland": "BMS has significant Irish manufacturing and IP holding operations",
            "puerto_rico": "Major manufacturing presence in PR (tax-advantaged under historical Section 936 / current Act 60)",
            "confidence": "confirmed (manufacturing); derived (IP structure)",
        },
        "pbm_connections": {
            "express_scripts": "Eliquis shared preferred status with Pfizer; Opdivo specialty distribution",
            "cvs_caremark": "Standard oncology formulary placement",
            "optumrx": "Eliquis on preferred tier",
            "confidence": "derived",
        },
    },

    # ================================================================
    # 8. AMGEN (AMGN) -- Biosimilars + Obesity Bet
    # ================================================================
    "AMGN": {
        "company": "Amgen Inc.",
        "ticker": "AMGN",
        "hq": "Thousand Oaks, California",
        "ceo": {
            "name": "Robert A. Bradway",
            "title": "Chairman, President & CEO",
            "tenure_start": 2012,
            "compensation_2025_est": 26_000_000,
            "compensation_2024_total": 24_400_000,
            "compensation_breakdown_2024": {
                "salary": 1_870_000,
                "equity": 18_000_000,
                "bonus": 3_840_000,
            },
            "notes": "All-time high compensation; equity bump attributed to Horizon Therapeutics acquisition integration",
            "confidence": "confirmed (2024); estimated (2025)",
        },
        "top_drugs": [
            {
                "name": "Prolia (denosumab)",
                "indication": "Osteoporosis",
                "revenue_2025_est": 4_200_000_000,
                "patent_expiry_us": "2025 (biosimilar competition beginning)",
                "confidence": "confirmed (patent); estimated (revenue)",
            },
            {
                "name": "ENBREL (etanercept)",
                "indication": "Rheumatoid arthritis, psoriasis",
                "revenue_2025_est": 3_200_000_000,
                "patent_expiry_us": "2029 (extended through patent thicket; original expired 2012)",
                "notes": "Amgen used patent thicket to delay biosimilar competition for 17+ years",
                "confidence": "confirmed (patent strategy); estimated (revenue)",
            },
            {
                "name": "Repatha (evolocumab)",
                "indication": "Cardiovascular (PCSK9 inhibitor)",
                "revenue_2025_est": 2_500_000_000,
                "patent_expiry_us": "2030",
                "confidence": "estimated",
            },
        ],
        "biosimilars_portfolio": {
            "total_approved_or_in_development": 11,
            "cumulative_sales": 13_000_000_000,
            "revenue_2025": 3_000_000_000,
            "yoy_growth": "37%",
            "pipeline_biosimilars": [
                "Biosimilar to Opdivo (Phase 3)",
                "Biosimilar to Keytruda (Phase 3)",
                "Biosimilar to Ocrevus (Phase 3)",
            ],
            "confidence": "confirmed",
        },
        "obesity_pipeline": {
            "lead_asset": "MariTide (maridebart cafraglutide)",
            "mechanism": "GLP-1/GIPR antibody -- once-monthly injection",
            "phase3_program": "MARITIME -- studies initiated H1 2025",
            "efficacy_data": "17% average weight loss at 52 weeks; no plateau; 99% of patients lost >5% body weight",
            "competitive_position": "Monthly dosing advantage vs Lilly/Novo weekly; but lower efficacy ceiling so far",
            "expected_approval": "2027-2028",
            "additional_pipeline": "Oral and injectable approaches with incretin and non-incretin mechanisms",
            "confidence": "confirmed (Phase 3 data, program initiation); estimated (approval timeline)",
        },
        "total_revenue_2025": 35_100_000_000,
        "fda_pipeline_phase3": [
            {
                "drug": "MariTide",
                "indication": "Obesity and related conditions",
                "expected_approval": "2027-2028",
                "confidence": "estimated",
            },
            {
                "drug": "IMDELLTRA/IMDYLLTRA (tarlatamab)",
                "indication": "Small cell lung cancer (BiTE antibody)",
                "expected_approval": "Approved 2024; expanded indications 2026",
                "confidence": "confirmed",
            },
            {
                "drug": "TEPEZZA (teprotumumab)",
                "indication": "Thyroid eye disease (acquired via Horizon)",
                "revenue_2025_est": 2_000_000_000,
                "confidence": "estimated",
            },
        ],
        "lobbying": {
            "spend_2024_est": 12_000_000,
            "notes": "Amgen is historically one of the top 3 pharma lobbying spenders alongside Pfizer and Lilly",
            "focus": "Biosimilar competition framework, patent reform, IRA modifications",
            "confidence": "estimated (spend); confirmed (ranking)",
        },
        "drug_pricing_controversies": [
            {
                "issue": "ENBREL pricing",
                "detail": "ENBREL costs $6K+/month in US; biosimilar competition delayed for years through patent thicket strategy",
                "confidence": "confirmed",
            },
            {
                "issue": "TEPEZZA pricing",
                "detail": "TEPEZZA costs ~$300K per course of treatment; only FDA-approved therapy for TED",
                "confidence": "confirmed",
            },
        ],
        "insider_trading_12mo": {
            "pattern": "Bradway routine 10b5-1 sales; no unusual cluster activity",
            "confidence": "derived",
        },
        "offshore_ip": {
            "ireland": "Amgen Technology Ireland -- significant manufacturing and IP presence",
            "puerto_rico": "Major biologics manufacturing facility",
            "singapore": "Regional hub for Asia-Pacific IP",
            "confidence": "confirmed (manufacturing locations); derived (IP routing)",
        },
        "pbm_connections": {
            "express_scripts": "Biosimilar portfolio gaining formulary placement as brand drugs lose exclusivity",
            "cvs_caremark": "ENBREL facing biosimilar substitution pressure",
            "optumrx": "Repatha competing with Regeneron's Praluent for preferred PCSK9 status",
            "confidence": "derived",
        },
    },

    # ================================================================
    # 9. REGENERON (REGN) -- Dupixent Machine
    # ================================================================
    "REGN": {
        "company": "Regeneron Pharmaceuticals, Inc.",
        "ticker": "REGN",
        "hq": "Tarrytown, New York",
        "ceo": {
            "name": "Leonard S. Schleifer, M.D., Ph.D.",
            "title": "President, CEO & Co-Founder",
            "tenure_start": 1988,
            "tenure_years": 38,
            "compensation_2024_total": 6_823_034,
            "compensation_notes": "Schleifer's cash comp appears modest but he owns 3.98% of REGN ($2.47B+); exercised 203K options in 2024 realizing $117M",
            "historic_comp": "Previously topped $40M in years with large option grants; compensation is highly variable year-to-year",
            "investor_pushback": "Investors urged ouster of compensation committee director over executive pay structure",
            "confidence": "confirmed",
        },
        "top_drugs": [
            {
                "name": "Dupixent (dupilumab)",
                "indication": "Atopic dermatitis, asthma, COPD, CSU, eosinophilic esophagitis, BPNS, bullous pemphigoid",
                "revenue_2025_q4": 4_900_000_000,
                "revenue_2025_est": 16_500_000_000,
                "yoy_growth": "32%",
                "active_patients": "1.4M+ worldwide",
                "patent_expiry_us": "2031 (base); thicket extends to 2039",
                "partner": "Sanofi (50/50 profit share until development balance paid off, expected Q3 2026)",
                "pipeline_in_product": "6+ approved indications; CSU (EU Nov 2025), bullous pemphigoid, allergic fungal rhinosinusitis pending",
                "confidence": "confirmed",
            },
            {
                "name": "EYLEA HD / EYLEA (aflibercept)",
                "indication": "Wet AMD, diabetic macular edema, retinal vein occlusion",
                "revenue_2025_q4_combined": 1_500_000_000,
                "eylea_hd_pct_of_combined": 0.50,
                "patent_expiry_us_eylea": "2027 (biosimilars delayed but expected)",
                "patent_expiry_us_eylea_hd": "2034+",
                "transition_strategy": "Converting EYLEA patients to EYLEA HD (higher dose, less frequent injections)",
                "revenue_2026_outlook": "EYLEA declines ~20% but EYLEA HD grows double digits",
                "confidence": "confirmed",
            },
            {
                "name": "Libtayo (cemiplimab)",
                "indication": "Non-melanoma skin cancers, NSCLC, basal cell carcinoma",
                "revenue_2025_est": 1_200_000_000,
                "patent_expiry_us": "2035",
                "notes": "First and only immunotherapy for adjuvant CSCC; expanding beyond niche skin cancer indications",
                "confidence": "estimated (revenue); confirmed (approvals)",
            },
        ],
        "praluent_status": {
            "name": "Praluent (alirocumab)",
            "indication": "Cardiovascular (PCSK9 inhibitor)",
            "revenue_2025_est": 400_000_000,
            "notes": "Underperformed vs Amgen's Repatha; formulary placement wars with PBMs limited uptake",
            "partner": "Sanofi",
            "confidence": "estimated",
        },
        "total_revenue_2025_est": 15_000_000_000,
        "fda_pipeline_phase3": [
            {
                "drug": "Dupixent (allergic fungal rhinosinusitis)",
                "indication": "New indication expansion",
                "pdufa_date": "February 2026",
                "confidence": "confirmed",
            },
            {
                "drug": "Fianlimab (anti-LAG-3)",
                "indication": "Melanoma (combo with Libtayo)",
                "expected_approval": "2026",
                "confidence": "confirmed (Phase 3); estimated (approval)",
            },
            {
                "drug": "Itepekimab",
                "indication": "Moderate-to-severe asthma (IL-33 inhibitor)",
                "expected_approval": "2027",
                "confidence": "estimated",
            },
        ],
        "lobbying": {
            "spend_2024_est": 5_500_000,
            "focus": "Biosimilar competition framework, ophthalmic drug regulation, specialty pharmacy reform",
            "confidence": "estimated",
        },
        "drug_pricing_controversies": [
            {
                "issue": "Dupixent pricing",
                "detail": "Dupixent costs ~$36K/year ($3K/month) in the US; expanding indications increase total spending while per-patient costs remain high",
                "confidence": "confirmed",
            },
            {
                "issue": "EYLEA pricing defense",
                "detail": "Regeneron fought biosimilar entry through patent litigation; delayed competition by 2+ years",
                "confidence": "confirmed",
            },
        ],
        "insider_trading_12mo": {
            "ceo_activity": "Schleifer exercised 203K options in 2024 for $117M; owns $2.47B in REGN stock directly",
            "pattern": "Founder-CEO with massive equity position; periodic large exercises for liquidity, not bearish signal",
            "cso_activity": "George Yancopoulos (CSO/co-founder) also historically large option exercises",
            "confidence": "confirmed (Form 4 filings)",
        },
        "offshore_ip": {
            "structure": "Regeneron is primarily US-based with less offshore IP routing than peers",
            "ireland": "Regeneron Ireland DAC -- manufacturing facility in Limerick",
            "notes": "Relatively clean tax structure compared to legacy pharma; biotech-origin companies tend to have less complex offshore structures",
            "confidence": "derived",
        },
        "pbm_connections": {
            "express_scripts": "Dupixent specialty pharmacy distribution; limited PBM pricing pressure due to lack of direct competitors",
            "cvs_caremark": "Dupixent covered but often requires prior authorization",
            "optumrx": "Praluent lost preferred status to Repatha on some plans; Dupixent maintained",
            "notes": "Dupixent's expanding indication set makes it harder for PBMs to exclude; 'pipeline in a product' strategy creates formulary stickiness",
            "confidence": "derived (PBM dynamics); confirmed (prior auth requirements)",
        },
    },

    # ================================================================
    # 10. VERTEX PHARMACEUTICALS (VRTX) -- CF Monopoly + Pain Entry
    # ================================================================
    "VRTX": {
        "company": "Vertex Pharmaceuticals Incorporated",
        "ticker": "VRTX",
        "hq": "Boston, Massachusetts",
        "ceo": {
            "name": "Reshma Kewalramani, M.D.",
            "title": "President & CEO",
            "tenure_start": 2020,
            "compensation_2025_total": 21_500_000,
            "compensation_trajectory": "$15.2M (2021) -> $15.9M (2022) -> $20.6M (2023) -> $21.5M (2025, +4%)",
            "compensation_breakdown_2025": {
                "performance_bonus": 4_900_000,
            },
            "notes": "One of few female pharma CEOs; rapidly scaled compensation",
            "confidence": "confirmed",
        },
        "top_drugs": [
            {
                "name": "Trikafta/Kaftrio (elexacaftor/tezacaftor/ivacaftor)",
                "indication": "Cystic fibrosis (CFTR modulator triple combo)",
                "revenue_2025_est": 10_500_000_000,
                "revenue_2025_q3": 2_650_000_000,
                "pct_of_total_revenue": 0.86,
                "patent_expiry_us": "2037",
                "notes": "Dominant CF franchise; being transitioned to Alyftrek",
                "confidence": "confirmed",
            },
            {
                "name": "Alyftrek (vanzacaftor/tezacaftor/deutivacaftor)",
                "indication": "Cystic fibrosis (next-gen once-daily triple combo)",
                "revenue_2025_q3": 247_000_000,
                "patent_expiry_us": "2039",
                "notes": "Approved as Trikafta successor; better sweat chloride reduction in Phase 3; patent extends CF franchise by 2 years",
                "confidence": "confirmed",
            },
            {
                "name": "Journavx (suzetrigine)",
                "indication": "Acute pain (non-opioid, NaV1.8 inhibitor)",
                "revenue_2025_est": 200_000_000,
                "approval_date": "January 2025",
                "patent_expiry_us": "2040+",
                "notes": "First new class of pain medicine in 20+ years; non-addictive; landmark approval for opioid crisis era",
                "confidence": "confirmed (approval); estimated (revenue -- early launch)",
            },
        ],
        "total_revenue_2025_est": 12_000_000_000,
        "cf_franchise_concentration_risk": {
            "detail": "86% of revenue from CF therapies serving ~88K patients globally; addressable market ceiling is a structural concern",
            "diversification_efforts": "Journavx (pain), Casgevy (sickle cell / gene therapy with CRISPR), kidney disease pipeline",
            "confidence": "confirmed (concentration); derived (risk assessment)",
        },
        "fda_pipeline_phase3": [
            {
                "drug": "Alyftrek (pediatric expansion)",
                "indication": "CF in children 2-5 years",
                "expected_results": "H1 2026",
                "confidence": "confirmed (enrollment complete)",
            },
            {
                "drug": "Journavx (expanded indications)",
                "indication": "Chronic pain, post-surgical, neuropathic",
                "expected_approval": "2026-2027 (additional indications)",
                "confidence": "estimated",
            },
            {
                "drug": "Casgevy (exagamglogene autotemcel)",
                "indication": "Sickle cell disease, beta-thalassemia (CRISPR gene therapy)",
                "status": "Approved Dec 2023; scaling manufacturing",
                "notes": "First CRISPR-based therapy ever approved; co-developed with CRISPR Therapeutics",
                "confidence": "confirmed",
            },
            {
                "drug": "VX-548 (suzetrigine) for lumbosacral radiculopathy",
                "indication": "Chronic low back pain with radiculopathy",
                "expected_approval": "2027",
                "confidence": "estimated",
            },
            {
                "drug": "Inaxaplin (VX-147)",
                "indication": "APOL1-mediated kidney disease",
                "expected_approval": "2026",
                "notes": "Would be first treatment targeting genetic cause of kidney disease in Black patients",
                "confidence": "confirmed (Phase 3); estimated (approval)",
            },
        ],
        "lobbying": {
            "spend_2024_est": 4_000_000,
            "focus": "Orphan drug pricing protections, gene therapy reimbursement, pain medication scheduling",
            "notes": "Right to Breathe campaign -- 1000+ patients demanded Vertex CEO 'put lives before profits' on CF drug pricing",
            "confidence": "estimated (spend); confirmed (advocacy campaign)",
        },
        "drug_pricing_controversies": [
            {
                "issue": "Trikafta pricing",
                "detail": "Trikafta costs $322K/year ($27K/month) in the US; Vertex argues small patient population justifies premium pricing",
                "confidence": "confirmed",
            },
            {
                "issue": "Casgevy pricing",
                "detail": "Casgevy priced at $2.2M per one-time treatment; gene therapy pricing is a major policy debate",
                "confidence": "confirmed",
            },
            {
                "issue": "Patient advocacy backlash",
                "detail": "Right to Breathe campaign organized 1000+ CF patients demanding price reform; Vertex maintaining pricing",
                "confidence": "confirmed",
            },
        ],
        "insider_trading_12mo": {
            "pattern": "Routine 10b5-1 plan sales by executives; CSO David Altshuler periodic equity dispositions",
            "confidence": "derived",
        },
        "offshore_ip": {
            "structure": "Vertex has less offshore IP complexity than legacy pharma; biotech-origin",
            "ireland": "Limited Irish presence vs peers",
            "notes": "As CF franchise matures, watch for increasing offshore IP migration -- standard pharma playbook",
            "confidence": "derived (current structure); inferred (future risk)",
        },
        "pbm_connections": {
            "express_scripts": "Trikafta/Alyftrek specialty pharmacy; limited PBM leverage due to monopoly position in CF",
            "cvs_caremark": "Covered but specialty tier with high out-of-pocket costs for patients",
            "optumrx": "Specialty distribution; Journavx formulary placement TBD as launch matures",
            "notes": "Vertex's CF monopoly means PBMs have minimal negotiating leverage; Journavx will face more PBM scrutiny as it competes with existing pain meds",
            "confidence": "derived",
        },
    },

    # ================================================================
    # CROSS-CUTTING INTELLIGENCE
    # ================================================================
    "cross_cutting": {
        "pbm_ftc_action": {
            "status": "FTC sued all Big 3 PBMs (Express Scripts, CVS Caremark, OptumRx) in September 2024",
            "express_scripts_settlement": {
                "date": "February 2026",
                "terms": "Landmark restructuring: must delink compensation from rebate negotiation, cannot prefer high-list-price drugs, transparency requirements",
                "patient_savings_estimate": "Up to $7B over 10 years",
                "excess_revenue_alleged": 7_300_000_000,
            },
            "cvs_caremark_status": "Lawsuit ongoing",
            "optumrx_status": "Lawsuit ongoing",
            "pharma_impact": "PBM reform could reduce pharma rebate burden but also reduce formulary exclusivity leverage",
            "confidence": "confirmed",
        },
        "trump_mfn_pricing": {
            "detail": "Nov 2025: Lilly and Novo agreed to MFN-style pricing for GLP-1s at $245/month for Medicare",
            "trumprx_platform": "Government direct-purchase platform launching 2026; $350/month for semaglutide",
            "additional_companies": "9 total drugmakers struck pricing deals by early 2026",
            "ira_negotiation": "First 10 drugs negotiated prices effective Jan 2026; second batch of 15 (including Ozempic/Wegovy) effective 2027",
            "keytruda_exemption": "Orphan drug exclusion may shield Keytruda; CBO estimates $8.8B higher Medicare spending",
            "confidence": "confirmed",
        },
        "industry_lobbying_totals": {
            "spend_2024": 388_000_000,
            "spend_2025": 451_800_000,
            "spend_2025_h1": 227_000_000,
            "record_status": "On pace for all-time record in 2025",
            "phrma_q1_2025": 13_000_000,
            "inaugural_contributions": "PhRMA + individual companies each gave $500K-$1M to Trump 2025 inaugural",
            "phrma_dark_money": "PhRMA donated $4M to American Action Network (House Republican dark money group)",
            "confidence": "confirmed",
        },
        "offshore_tax_avoidance": {
            "senate_investigation": "Senate Finance Committee (Wyden) ongoing investigation since 2023",
            "combined_profits": "AbbVie, J&J, Merck, Pfizer combined $429B in profits since 2018 with near-zero US taxes paid",
            "irish_affiliate_assets": {
                "AbbVie": 308_000_000_000,
                "JNJ": 102_000_000_000,
                "Merck": 44_000_000_000,
                "Pfizer": "Largest -- exact figure undisclosed (Senate calls it potentially 'largest tax-dodging scheme')",
            },
            "netherlands_entities": "~170 pharma entities registered in Netherlands for tax routing",
            "confidence": "confirmed (Senate investigation, Investigate Europe analysis)",
        },
        "patent_cliff_2026_2032": {
            "total_revenue_at_risk": "200B+ across top 10 pharma companies",
            "key_expirations": {
                "2026": ["Eliquis (PFE/BMY)", "Januvia (MRK)", "Stelara (JNJ) biosimilars accelerating"],
                "2027": ["Ibrance (PFE)", "Xtandi (PFE)"],
                "2028": ["Keytruda (MRK)", "Opdivo (BMY)", "Eliquis EU", "Gardasil (MRK)"],
                "2029-2031": ["Darzalex (JNJ)", "ENBREL (AMGN)", "Dupixent base (REGN)"],
                "2032+": ["Ozempic/Wegovy (NVO)", "Skyrizi (ABBV)"],
                "2036-2040": ["Mounjaro/Zepbound (LLY)", "Rinvoq (ABBV)", "Trikafta (VRTX)"],
            },
            "confidence": "confirmed (dates); derived (revenue at risk estimate)",
        },
    },
}
