# GRID Data Sources Catalog
## Bulk Downloadable Financial & Intelligence Datasets
### Compiled 2026-03-29

---

## MUST FIND (All Located)

---

### 1. ICIJ Offshore Leaks Database
- **Download URL:** https://offshoreleaks-data.icij.org/offshoreleaks/csv/full-oldb.LATEST.zip
- **Neo4j dump:** https://offshoreleaks-data.icij.org/offshoreleaks/neo4j/icij-offshoreleaks-4.4.26.dump
- **Size:** ~500MB (CSV ZIP), ~1.5GB (Neo4j dump)
- **Format:** CSV (in ZIP), Neo4j dump
- **Contains:** Entities, officers, intermediaries, addresses, relationships from: Offshore Leaks (2013), Panama Papers (2016), Bahamas Leaks (2016), Paradise Papers (2017), Pandora Papers (2021)
- **Fields:** Node types (Entity, Officer, Intermediary, Address) + all relationships between them. Names, jurisdictions, countries, dates, source investigations
- **Date range:** 1990s-2021
- **Last updated:** Periodically (check site)
- **License:** Open Database License (ODbL) + CC BY-SA. Commercial use allowed with attribution

---

### 2. SEC EDGAR - Full Filing History & Financial Data

#### a) Submissions Bulk (All Filers)
- **Download URL:** https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip
- **Size:** ~2GB
- **Format:** ZIP containing JSON files
- **Contains:** Complete EDGAR filing history for ALL filers. Company name, CIK, SIC code, ticker, exchange, state, filing dates, form types, accession numbers
- **Date range:** 1993-present
- **Last updated:** Nightly (~3AM ET)
- **License:** Public domain (US Government)

#### b) Company Facts / XBRL Financial Statements
- **Download URL:** https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip
- **Size:** ~3-4GB
- **Format:** ZIP containing JSON files
- **Contains:** ALL XBRL financial data from 10-K, 10-Q, 8-K filings. Revenue, net income, assets, liabilities, cash flows - every reported financial line item in structured format
- **Date range:** 2009-present (when XBRL became mandatory)
- **Last updated:** Nightly
- **License:** Public domain

#### c) Financial Statement Data Sets (Quarterly ZIP)
- **Download URL pattern:** https://www.sec.gov/files/dera/data/financial-statement-data-sets/{YEAR}q{QTR}.zip
- **Example:** https://www.sec.gov/files/dera/data/financial-statement-data-sets/2024q1.zip
- **Archive:** https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets-archive
- **Size:** ~200-400MB per quarter
- **Format:** ZIP containing TSV files (sub.txt, num.txt, tag.txt, pre.txt)
- **Contains:** Numeric data from face financials of ALL financial statements filed with SEC. Structured quarterly data: company info, financial tags, numeric values, presentation info
- **Date range:** 2009-present (quarterly files)
- **Last updated:** Quarterly
- **License:** Public domain

#### d) Company Tickers Mapping
- **Download URL:** https://www.sec.gov/files/company_tickers.json
- **Exchange mapping:** https://www.sec.gov/files/company_tickers_exchange.json
- **Size:** ~2MB
- **Format:** JSON
- **Contains:** CIK-to-ticker-to-company-name mappings for all public companies
- **License:** Public domain

#### e) Full Index (All Filing References)
- **URL:** https://www.sec.gov/Archives/edgar/full-index/
- **Pattern:** https://www.sec.gov/Archives/edgar/full-index/{YEAR}/QTR{N}/
- **Contains:** company.idx, crawler.idx, form.idx, master.idx - indexes of every filing
- **Date range:** 1993-present
- **License:** Public domain

**NOTE:** SEC requires User-Agent header with company name + email. Max 10 req/sec.

---

### 3. FRED (Federal Reserve Economic Data)

- **API bulk approach:** https://fred.stlouisfed.org/docs/api/fred/ (free API key required)
- **All series CSV (Ivo Welch mirror):** https://www.ivo-welch.info/professional/fredcsv.html
- **Size:** 840,000+ time series from 118 sources
- **Format:** CSV per series (via API), or bulk download scripts
- **Contains:** GDP, unemployment, inflation, interest rates, exchange rates, housing, manufacturing - every macro indicator. Each series has: date, value, frequency, seasonal adjustment
- **Date range:** Varies per series (some from 1800s)
- **Last updated:** Continuously
- **License:** Public domain (US Government data)

**Best approach:** Use FRED API with bulk retrieval by release. API key is free: https://fred.stlouisfed.org/docs/api/api_key.html

---

### 4. Congressional Stock Trading

#### a) Senate Stock Watcher (Timothy Carambat)
- **All transactions JSON:** https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json
- **GitHub repo:** https://github.com/timothycarambat/senate-stock-watcher-data
- **Size:** ~50MB
- **Format:** JSON (CSV also available in repo)
- **Contains:** Senator name, transaction date, ticker, asset type, amount range, transaction type (purchase/sale), filing date
- **Date range:** 2012-present
- **Last updated:** Daily (automated scraping of efdsearch.senate.gov)
- **License:** Public domain data (STOCK Act disclosures)

#### b) House Stock Watcher
- **All transactions JSON:** https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json
- **File index:** https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/filemap.xml
- **Size:** ~30MB
- **Format:** JSON
- **Contains:** Representative name, district, transaction date, ticker, amount range, type
- **Date range:** 2016-present
- **Last updated:** Regular automated updates
- **License:** Public domain data

#### c) Kaggle - Congressional Trading
- **URL:** https://www.kaggle.com/datasets/shabbarank/congressional-trading-inception-to-march-23
- **Format:** CSV
- **Contains:** Both House and Senate trades consolidated

#### d) Harvard Dataverse - Senate Stock Trades
- **URL:** https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/XPDSYQ
- **Date range:** 2012-2021
- **Format:** CSV
- **License:** CC0

---

### 5. Corporate Insider Trading (SEC Form 4 Bulk)

#### a) Harvard Dataverse / Layline Dataset (BEST SOURCE)
- **URL:** https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/VH6GVH
- **Kaggle mirror:** https://www.kaggle.com/datasets/layline/insidertrading
- **Size:** Multi-GB (all years)
- **Format:** 6 CSV files per release (Submissions, Reporting Owners, Non-derivative, Derivative, Footnotes, Signatures)
- **Contains:** ALL Form 4 insider trading filings. Insider name, title, company, CIK, transaction date, shares, price, ownership type (direct/indirect), transaction codes (P=purchase, S=sale, etc.)
- **Date range:** 2003-present
- **Last updated:** Daily
- **License:** Free/open

#### b) SEC EDGAR Direct (Form 4 index)
- **URL pattern:** https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=4&dateb=&owner=include&count=40&search_text=&action=getcompany
- **Full index:** Filter form.idx files for type "4" from full-index

#### c) Kaggle - SEC Form 4 Filings
- **URL:** https://www.kaggle.com/datasets/secfilingapi/sec-form-4-filings
- **URL:** https://www.kaggle.com/datasets/sandorabad/insider-trading-sec-form-4-i

---

### 6. Lobbying Disclosures (Senate LDA)

#### a) Senate Lobbying Database Download
- **Main page:** https://www.senate.gov/legislative/Public_Disclosure/database_download.htm
- **REST API (current):** https://lda.senate.gov/api/
- **New portal (replaces old after 06/30/2026):** https://lda.senate.gov/
- **GitHub archive:** https://github.com/wgetsnaps/senate-lda-activity
- **Format:** XML (historical ZIPs), JSON via API
- **Contains:** LD-1 (registrations), LD-2 (quarterly activity), LD-203 (contributions). Registrant, client, lobbying issues, specific bills, lobbyist names, amounts
- **Date range:** 1999-present
- **License:** Public domain

#### b) House Lobbying Disclosures
- **URL:** https://lobbyingdisclosure.house.gov/
- **Format:** XML
- **License:** Public domain

---

### 7. Campaign Finance (FEC Bulk Data)

- **Browse page:** https://www.fec.gov/data/browse-data/?tab=bulk-data
- **Base URL:** https://www.fec.gov/files/bulk-downloads/

#### Key files (2024 cycle shown, pattern repeats for all cycles 1980-2026):
| File | URL | Contains |
|------|-----|----------|
| Individual Contributions | https://www.fec.gov/files/bulk-downloads/2024/indiv24.zip | Every individual donation >$200. Name, city, state, ZIP, employer, occupation, amount, date, recipient committee |
| Candidate Master | https://www.fec.gov/files/bulk-downloads/2024/cn24.zip | All candidates: name, party, office, state, district, status |
| Committee Master | https://www.fec.gov/files/bulk-downloads/2024/cm24.zip | All PACs/committees: name, type, party, treasurer |
| Candidate-Committee Links | https://www.fec.gov/files/bulk-downloads/2024/ccl24.zip | Links between candidates and their committees |
| PAC-to-Candidate | https://www.fec.gov/files/bulk-downloads/2024/pas224.zip | All PAC contributions to candidates |
| Committee-to-Committee | https://www.fec.gov/files/bulk-downloads/2024/oth24.zip | Transfers between committees |
| All Candidates Summary | https://www.fec.gov/files/bulk-downloads/2024/weball24.zip | Summary financial data for all candidates |
| Independent Expenditures | https://www.fec.gov/files/bulk-downloads/2024/independent_expenditure_2024.csv | IE spending for/against candidates |

- **Size:** indiv24.zip is typically 2-4GB. Other files 10-100MB each
- **Format:** ZIP containing pipe-delimited text files (except IE which is CSV)
- **Date range:** 1980-2026 (every 2-year cycle)
- **Last updated:** Daily to weekly
- **License:** Public domain

---

## HIGH VALUE

---

### 8. S&P 500 Historical Fundamentals

#### a) SEC companyfacts.zip (see item 2b above - BEST source)
- All XBRL financial data for all public companies including S&P 500

#### b) HuggingFace - SEC Annual Reports (Full Text)
- **URL:** https://huggingface.co/datasets/PleIAs/SEC
- **Size:** 245,211 10-K filings, 7.2 billion words total
- **Format:** Parquet
- **Date range:** 1993-2024
- **License:** Check dataset card

#### c) Kaggle - S&P 500 Financial Data
- **URL:** https://www.kaggle.com/datasets/ilyaryabov/financial-performance-of-companies-from-sp500
- **Contains:** 72 financial indicators for S&P 500 companies
- **URL:** https://www.kaggle.com/datasets/pierrelouisdanieau/financial-data-sp500-companies
- **URL:** https://www.kaggle.com/datasets/paytonfisher/sp-500-companies-with-financial-information

---

### 9. Global Company Ownership / Subsidiary Data

#### a) UK Companies House - Basic Company Data (FREE)
- **Download page:** https://download.companieshouse.gov.uk/en_output.html
- **Size:** ~1GB (CSV ZIP)
- **Format:** CSV in ZIP
- **Contains:** All live UK companies: name, number, address, SIC codes, incorporation date, status
- **Last updated:** Monthly
- **License:** Open Government Licence

#### b) UK Companies House - Persons with Significant Control (PSC)
- **Download page:** https://download.companieshouse.gov.uk/en_pscdata.html
- **URL pattern:** http://download.companieshouse.gov.uk/persons-with-significant-control-snapshot-YYYY-MM-DD.zip
- **Size:** ~500MB-1GB
- **Format:** JSON in ZIP
- **Contains:** Beneficial ownership data: PSC name, nationality, DOB (month/year), nature of control (shares/votes percentage bands), notified date
- **Last updated:** Daily (before 10AM GMT)
- **License:** Open Government Licence

#### c) UK Companies House - Accounts Data
- **Download page:** https://download.companieshouse.gov.uk/en_accountsdata.html
- **Format:** XBRL/iXBRL in ZIP
- **Contains:** Filed accounts for all UK companies

#### d) Wikidata Corporate Dump
- **URL:** https://dumps.wikimedia.org/wikidatawiki/entities/
- **Size:** ~100GB+ (full dump)
- **Format:** JSON, RDF (TTL, NT)
- **Contains:** Corporate entities, subsidiaries (P355), parent organizations (P749), ownership, board members, headquarters, founding dates
- **Last updated:** Every few days
- **License:** CC0

---

### 10. Historical Crypto Data

#### a) CryptoDataDownload (BEST free source)
- **URL:** https://www.cryptodatadownload.com/data/
- **Size:** Varies per exchange/pair
- **Format:** CSV
- **Contains:** OHLCV candle data from 20+ exchanges (Binance, Bitstamp, Coinbase, Gemini, etc.) for 1000+ cryptocurrencies. Minute, hourly, daily granularity
- **Date range:** 2013-present (varies by exchange)
- **Last updated:** Regular
- **License:** Free for personal/research use

#### b) Binance Public Data
- **URL:** https://data.binance.vision/
- **Format:** CSV in ZIP
- **Contains:** All Binance spot/futures historical trades, klines, aggregated trades
- **License:** Free

---

### 11. Patent Data (USPTO)

#### a) PatentsView Data Download Tables
- **Granted patents:** https://patentsview.org/download/data-download-tables
- **Pre-grant applications:** https://patentsview.org/download/pg-download-tables
- **Annualized CSV:** https://patentsview.org/data/annualized
- **Size:** Multi-GB (full database)
- **Format:** Tab-delimited text files, CSV for annualized
- **Contains:** Patent number, title, abstract, claims, assignee, inventor, CPC/IPC classification, citations, examiner, filing/grant dates
- **Date range:** Granted patents since 1976, applications since 2001
- **Last updated:** Quarterly (migrating to data.uspto.gov March 2026)
- **License:** Public domain

#### b) USPTO Open Data Portal
- **URL:** https://data.uspto.gov/bulkdata/datasets
- **Contains:** Raw patent grant XML, application XML, trademark data
- **License:** Public domain

---

### 12. International Trade Data

#### a) World Bank WITS (free subset of UN Comtrade)
- **URL:** https://wits.worldbank.org/
- **Contains:** Trade flows by country/product (HS codes)
- **License:** Free for non-commercial

#### b) UN Comtrade
- **URL:** https://comtrade.un.org/
- **Note:** Bulk download is PREMIUM only. Free tier limited to 100 req/hour via API
- **Contains:** Bilateral trade data for all countries, all HS commodity codes

---

## INTELLIGENCE

---

### 13. Sanctions Lists

#### a) OFAC SDN List (US)
- **SDN CSV:** https://www.treasury.gov/ofac/downloads/sdn.csv (redirects to sanctionslistservice.ofac.treas.gov)
- **SDN addresses:** https://www.treasury.gov/ofac/downloads/add.csv
- **SDN alternate names:** https://www.treasury.gov/ofac/downloads/alt.csv
- **SDN comments:** https://www.treasury.gov/ofac/downloads/sdn_comments.csv
- **SDN XML:** https://www.treasury.gov/ofac/downloads/sdn.xml
- **Advanced XML:** https://www.treasury.gov/ofac/downloads/sdn_advanced.xml
- **Consolidated (non-SDN):** https://www.treasury.gov/ofac/downloads/consolidated/cons_prim.csv
- **Size:** ~5-10MB total
- **Format:** CSV, XML
- **Contains:** Names, aliases, addresses, DOB, nationality, passport numbers, ID numbers for sanctioned individuals and entities
- **Last updated:** Within hours of new designations (verified working 2026-03-27)
- **License:** Public domain

#### b) EU Consolidated Sanctions List
- **URL:** https://data.europa.eu/data/datasets/consolidated-list-of-persons-groups-and-entities-subject-to-eu-financial-sanctions
- **Format:** CSV, XML, PDF
- **Contains:** All EU-sanctioned persons, groups, entities
- **Last updated:** Daily
- **License:** Open (EU data portal)

#### c) OpenSanctions (AGGREGATED - BEST SOURCE)
- **Default dataset (all sanctions + PEPs):** https://data.opensanctions.org/datasets/latest/default/entities.ftm.json
- **Simplified CSV:** https://data.opensanctions.org/datasets/latest/default/targets.simple.csv
- **Sanctions only:** https://data.opensanctions.org/datasets/latest/sanctions/targets.simple.csv
- **PEPs only:** https://data.opensanctions.org/datasets/latest/peps/targets.simple.csv
- **Size:** ~200-500MB (full default dataset)
- **Format:** JSON (FtM format) and simplified CSV
- **Contains:** 329 sources aggregated: sanctions, PEPs, crime, corporate entities. Names, aliases, DOB, nationality, positions, sanctions programs, relationships
- **Date range:** Current (historical versions available)
- **Last updated:** Daily
- **License:** Free for non-commercial. Commercial requires license ($)

---

### 14. Politically Exposed Persons (PEPs)

#### a) OpenSanctions PEP Dataset (see 13c above)
- **Direct CSV:** https://data.opensanctions.org/datasets/latest/peps/targets.simple.csv
- **Contains:** Cabinet members, parliamentarians, senior officials, military leaders, state-owned enterprise managers, judges, from 100+ countries

#### b) EveryPolitician (archived)
- **URL:** https://everypolitician.org/
- **Format:** CSV, JSON
- **Contains:** Politicians worldwide (archived project)

---

### 15. Beneficial Ownership Registries

#### a) UK PSC Register (see item 9b above)
- Best free beneficial ownership registry in the world

#### b) Open Ownership BODS Data
- **URL:** https://bods-data.openownership.org/
- **Contains:** Beneficial Ownership Data Standard formatted data from UK PSC and other sources

---

### 16. Court Records / Litigation

#### a) CourtListener / RECAP Archive (Free Law Project)
- **Bulk data page:** https://www.courtlistener.com/help/api/bulk-data/
- **RECAP search:** https://www.courtlistener.com/recap/
- **Size:** Multi-TB archive (selective download)
- **Format:** JSON (via API), bulk files generated quarterly
- **Contains:** Federal court dockets, opinions, oral arguments from PACER. Case name, number, parties, judges, filing dates, documents
- **Date range:** Varies by court
- **Last updated:** Quarterly bulk regeneration (last day of Mar/Jun/Sep/Dec)
- **License:** Free for public use (501(c)(3) non-profit)

#### b) Federal Judicial Center - Integrated Database
- **URL:** https://www.fjc.gov/research/idb
- **Size:** Large (entire federal court system)
- **Format:** CSV / downloadable tables
- **Contains:** Case-level data (not documents) for civil, criminal, bankruptcy, and appellate cases. Case type, jurisdiction, disposition, dates, judge
- **Date range:** Civil/Criminal from 1979, Bankruptcy from 2008
- **Last updated:** Regular
- **License:** Public domain (US Government)

---

### 17. International Economic / Financial Data

#### a) World Bank - World Development Indicators
- **CSV bulk:** https://databank.worldbank.org/data/download/WDI_CSV.zip (redirects to https://databankfiles.worldbank.org/public/ddpext_download/WDI_CSV.zip)
- **Excel bulk:** https://databank.worldbank.org/data/download/WDI_excel.zip
- **Size:** ~283MB (CSV ZIP)
- **Format:** CSV
- **Contains:** 1,600+ indicators for 217 economies: GDP, population, health, education, trade, environment, governance
- **Date range:** 1960-present
- **Last updated:** Regularly
- **License:** CC BY 4.0

#### b) IMF World Economic Outlook
- **URL:** https://www.imf.org/en/publications/weo/weo-database/2025/april
- **Data portal:** https://data.imf.org/
- **Format:** XLS (actually CSV), SDMX
- **Contains:** GDP, inflation, unemployment, fiscal balances, current account, for 190+ countries
- **Date range:** 1980-2030 (forecasts included)
- **License:** Free with attribution

#### c) BIS Statistics (Bank for International Settlements)
- **Bulk download portal:** https://data.bis.org/bulkdownload
- **Format:** CSV (zipped), SDMX
- **Contains:** Cross-border banking, derivatives, credit, debt securities, property prices, exchange rates, effective exchange rates
- **Date range:** Varies (some from 1960s)
- **License:** Free with terms compliance

#### d) ECB Statistical Data Warehouse
- **Bulk download help:** https://data.ecb.europa.eu/help/bulk-download
- **Browse datasets:** https://data.ecb.europa.eu/data/datasets
- **Format:** CSV, SDMX
- **Contains:** Euro area monetary aggregates, interest rates, exchange rates, banking statistics, securities, government finance
- **License:** Free with ECB terms

---

### 18. House Financial Disclosures

- **Download page:** https://disclosures-clerk.house.gov/FinancialDisclosure
- **Member data XML:** https://clerk.house.gov/xml/lists/MemberData.xml
- **Format:** XML, PDF
- **Contains:** Financial disclosure reports, periodic transaction reports for House members
- **License:** Public domain (with use restrictions - not for commercial credit/solicitation purposes)

---

### 19. HuggingFace Financial Datasets

| Dataset | URL | Size | Contents |
|---------|-----|------|----------|
| PleIAs/SEC | https://huggingface.co/datasets/PleIAs/SEC | ~50GB+ | 245K 10-K filings full text, 1993-2024 |
| JanosAudran/financial-reports-sec | https://huggingface.co/datasets/JanosAudran/financial-reports-sec | Large | 10-K filings broken into sections, 1993-2020 |
| eloukas/edgar-corpus | https://huggingface.co/datasets/eloukas/edgar-corpus | Large | Annual reports 1993-2020 |
| DenyTranDFW/SEC-Financial-Statements | https://huggingface.co/datasets/DenyTranDFW/SEC-Financial-Statements-And-Notes-Dataset | Medium | Structured financial statements |

---

### 20. Kaggle Stock Market Datasets

| Dataset | URL | Contents |
|---------|-----|----------|
| Huge Stock Market Dataset | https://www.kaggle.com/datasets/borismarjanovic/price-volume-data-for-all-us-stocks-etfs | OHLCV for all US stocks/ETFs |
| 9000+ Tickers Full History | https://www.kaggle.com/datasets/jakewright/9000-tickers-of-stock-market-data-full-history | 9K+ tickers, full history |
| Stock Market Data (NASDAQ/NYSE/S&P) | https://www.kaggle.com/datasets/paultimothymooney/stock-market-data | Major exchange data |

---

## DOWNLOAD PRIORITY SCRIPT

For immediate bulk loading into GRID, here's the recommended download order:

```bash
# Create data directory
mkdir -p ~/dev/GRID/data/bulk && cd ~/dev/GRID/data/bulk

# 1. ICIJ Offshore Leaks (~500MB) - HIGHEST VALUE FOR INTELLIGENCE
curl -L -o icij-offshore-leaks.zip "https://offshoreleaks-data.icij.org/offshoreleaks/csv/full-oldb.LATEST.zip"

# 2. OFAC Sanctions (~10MB) - INSTANT INTELLIGENCE
curl -o sdn.csv "https://www.treasury.gov/ofac/downloads/sdn.csv"
curl -o sdn_add.csv "https://www.treasury.gov/ofac/downloads/add.csv"
curl -o sdn_alt.csv "https://www.treasury.gov/ofac/downloads/alt.csv"
curl -o cons_prim.csv "https://www.treasury.gov/ofac/downloads/consolidated/cons_prim.csv"

# 3. OpenSanctions Default (sanctions + PEPs) (~200MB)
curl -o opensanctions-default.json "https://data.opensanctions.org/datasets/latest/default/entities.ftm.json"
curl -o opensanctions-default.csv "https://data.opensanctions.org/datasets/latest/default/targets.simple.csv"
curl -o opensanctions-peps.csv "https://data.opensanctions.org/datasets/latest/peps/targets.simple.csv"

# 4. SEC EDGAR submissions (~2GB) - ALL COMPANIES, ALL FILINGS
curl -H "User-Agent: GRID Platform admin@example.com" -o submissions.zip "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"

# 5. SEC EDGAR companyfacts (~4GB) - ALL FINANCIAL DATA
curl -H "User-Agent: GRID Platform admin@example.com" -o companyfacts.zip "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"

# 6. Congressional Stock Trading (~50MB each)
curl -o senate_trades.json "https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json"
curl -o house_trades.json "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json"

# 7. FEC Campaign Finance - 2024 cycle (~3GB for indiv)
curl -o indiv24.zip "https://www.fec.gov/files/bulk-downloads/2024/indiv24.zip"
curl -o cm24.zip "https://www.fec.gov/files/bulk-downloads/2024/cm24.zip"
curl -o cn24.zip "https://www.fec.gov/files/bulk-downloads/2024/cn24.zip"
curl -o pas224.zip "https://www.fec.gov/files/bulk-downloads/2024/pas224.zip"
curl -o weball24.zip "https://www.fec.gov/files/bulk-downloads/2024/weball24.zip"

# 8. World Bank WDI (~283MB)
curl -L -o WDI_CSV.zip "https://databank.worldbank.org/data/download/WDI_CSV.zip"

# 9. UK Companies House - Companies + PSC (~1.5GB total)
# Check https://download.companieshouse.gov.uk/en_output.html for latest filename
# Check https://download.companieshouse.gov.uk/en_pscdata.html for latest PSC

# 10. SEC Financial Statement Data Sets (latest quarter)
curl -H "User-Agent: GRID Platform admin@example.com" -o fsds_2025q4.zip "https://www.sec.gov/files/dera/data/financial-statement-data-sets/2025q4.zip"
```

---

## NOTES

1. **SEC rate limits:** 10 requests/second max. Always include User-Agent header with company name + email
2. **OpenSanctions:** Free for non-commercial. Commercial license required for GRID monetization
3. **FEC data:** Files back to 1980. Get all cycles for longitudinal analysis
4. **FRED:** No single bulk file. Best approach is API with bulk release retrieval (free key)
5. **UN Comtrade:** Bulk download is premium only. Use World Bank WITS for free trade data
6. **BoardEx (corporate directors):** No free bulk alternative exists. SEC proxy filings are the free workaround
7. **Historical options data:** No free bulk source found. Best commercial: CBOE DataShop, OptionMetrics via WRDS
8. **Shipping/container data:** No free bulk source found. Best free: UN Comtrade (trade volumes), AIS ship tracking data on Marine Traffic
