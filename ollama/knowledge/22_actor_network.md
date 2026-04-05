# Actor Network Intelligence

## Overview
GRID tracks 1.6M+ actors across 24 data sources with 5M+ connections.

## Actor Categories
- **sovereign**: Central banks (Fed, ECB, BOJ, PBOC), governments, SWFs
- **institutional**: Hedge funds, asset managers, PE firms, corporations, dynasties
- **individual**: Insiders, CEOs, billionaires, kingmakers, activists
- **regional**: Politicians, local governments

## Data Sources for Actor Intelligence
- **ICIJ Offshore Leaks**: 1.6M entities from Panama/Paradise/Pandora Papers
- **QuiverQuant Pro**: Congressional trades, insider filings, lobbying, corporate flights, WSB
- **SEC 13F**: Institutional holdings (quarterly, ~50K rows)
- **LittleSis**: Power-mapping database of who-knows-who
- **Wikidata**: Board seats, subsidiaries, ownership structures
- **Etherscan**: Whale wallets, on-chain flows

## Connection Types
- trades_stock: Congress member trades company stock
- insider_at: Company insider (Form 4 filer)
- lobbies: Company lobbies Congress
- icij_exact_match / icij_fuzzy_high / icij_fuzzy_low: Offshore entity match
- business_partner, competes_with, political_alliance, regulates
- central_bank_coordination, sovereign_investor_peer, wealth_management

## Key Findings
- David E. Shaw, David Solomon (GS CEO), David Siegel (Two Sigma) — exact ICIJ matches
- Carl Icahn, Christine Lagarde (ECB), Nelson Peltz — ICIJ fuzzy matches
- All major SWFs (CIC, KIA, QIA, ADIA) — matched to ICIJ offshore entities
- 52 Congress members actively trading stocks (QuiverQuant)
- 8,122 corporate insiders tracked

## Anti-Hallucination Rules
- Every claim must cite a source from the DB
- Confidence labels: confirmed (DB match), derived (cross-reference), inferred (pattern)
- If no evidence exists, the field stays empty — NEVER fabricate
