"""Comprehensive S&P 500 Market Universe — every GICS sector, industry, and major company.

Replaces the limited SECTOR_MAP in sector_map.py for anything that needs
broad market coverage. Covers all 11 GICS sectors with their industries
and top publicly traded companies.

Usage:
    from analysis.market_universe import get_universe, get_sector, get_peers, search_company
"""

from __future__ import annotations

MARKET_UNIVERSE: dict[str, dict] = {
    # -------------------------------------------------------------------------
    # 1. TECHNOLOGY (XLK)
    # -------------------------------------------------------------------------
    "Technology": {
        "etf": "XLK",
        "industries": {
            "Semiconductors": {
                "etf": "SMH",
                "companies": [
                    {"ticker": "NVDA", "name": "NVIDIA", "market_cap": 3_000_000_000_000, "weight": 0.25},
                    {"ticker": "AVGO", "name": "Broadcom", "market_cap": 800_000_000_000, "weight": 0.12},
                    {"ticker": "AMD", "name": "AMD", "market_cap": 200_000_000_000, "weight": 0.08},
                    {"ticker": "INTC", "name": "Intel", "market_cap": 100_000_000_000, "weight": 0.05},
                    {"ticker": "TXN", "name": "Texas Instruments"},
                    {"ticker": "QCOM", "name": "Qualcomm"},
                    {"ticker": "MU", "name": "Micron"},
                    {"ticker": "AMAT", "name": "Applied Materials"},
                    {"ticker": "LRCX", "name": "Lam Research"},
                    {"ticker": "KLAC", "name": "KLA"},
                    {"ticker": "MRVL", "name": "Marvell"},
                    {"ticker": "ON", "name": "ON Semiconductor"},
                    {"ticker": "ADI", "name": "Analog Devices"},
                    {"ticker": "NXPI", "name": "NXP Semiconductors"},
                    {"ticker": "MCHP", "name": "Microchip Technology"},
                    {"ticker": "TSM", "name": "TSMC"},
                    {"ticker": "ASML", "name": "ASML"},
                    {"ticker": "MPWR", "name": "Monolithic Power Systems"},
                    {"ticker": "SWKS", "name": "Skyworks Solutions"},
                    {"ticker": "QRVO", "name": "Qorvo"},
                    {"ticker": "GFS", "name": "GlobalFoundries"},
                    {"ticker": "ENTG", "name": "Entegris"},
                    {"ticker": "AMKR", "name": "Amkor Technology"},
                ],
            },
            "Software": {
                "etf": "IGV",
                "companies": [
                    {"ticker": "MSFT", "name": "Microsoft", "market_cap": 3_100_000_000_000, "weight": 0.30},
                    {"ticker": "ORCL", "name": "Oracle", "market_cap": 350_000_000_000, "weight": 0.08},
                    {"ticker": "CRM", "name": "Salesforce", "market_cap": 260_000_000_000, "weight": 0.06},
                    {"ticker": "ADBE", "name": "Adobe", "market_cap": 220_000_000_000, "weight": 0.05},
                    {"ticker": "NOW", "name": "ServiceNow", "market_cap": 180_000_000_000, "weight": 0.04},
                    {"ticker": "INTU", "name": "Intuit", "market_cap": 170_000_000_000, "weight": 0.04},
                    {"ticker": "PANW", "name": "Palo Alto Networks"},
                    {"ticker": "SNPS", "name": "Synopsys"},
                    {"ticker": "CDNS", "name": "Cadence Design Systems"},
                    {"ticker": "CRWD", "name": "CrowdStrike"},
                    {"ticker": "FTNT", "name": "Fortinet"},
                    {"ticker": "WDAY", "name": "Workday"},
                    {"ticker": "PLTR", "name": "Palantir"},
                    {"ticker": "TEAM", "name": "Atlassian"},
                    {"ticker": "HUBS", "name": "HubSpot"},
                    {"ticker": "DDOG", "name": "Datadog"},
                    {"ticker": "ZS", "name": "Zscaler"},
                    {"ticker": "ANSS", "name": "ANSYS"},
                    {"ticker": "TTWO", "name": "Take-Two Interactive"},
                    {"ticker": "EA", "name": "Electronic Arts"},
                    {"ticker": "FICO", "name": "Fair Isaac"},
                    {"ticker": "TYL", "name": "Tyler Technologies"},
                    {"ticker": "MNDY", "name": "Monday.com"},
                    {"ticker": "NET", "name": "Cloudflare"},
                    {"ticker": "GEN", "name": "Gen Digital"},
                    {"ticker": "ROP", "name": "Roper Technologies"},
                    {"ticker": "MANH", "name": "Manhattan Associates"},
                    {"ticker": "MSCI", "name": "MSCI"},
                    {"ticker": "PAYC", "name": "Paycom"},
                    {"ticker": "PCTY", "name": "Paylocity"},
                ],
            },
            "Cloud / Internet": {
                "etf": "SKYY",
                "companies": [
                    {"ticker": "AMZN", "name": "Amazon", "market_cap": 2_000_000_000_000, "weight": 0.25},
                    {"ticker": "GOOGL", "name": "Alphabet", "market_cap": 2_100_000_000_000, "weight": 0.25},
                    {"ticker": "META", "name": "Meta Platforms", "market_cap": 1_500_000_000_000, "weight": 0.18},
                    {"ticker": "NFLX", "name": "Netflix", "market_cap": 300_000_000_000, "weight": 0.06},
                    {"ticker": "SNOW", "name": "Snowflake"},
                    {"ticker": "MDB", "name": "MongoDB"},
                    {"ticker": "SHOP", "name": "Shopify"},
                    {"ticker": "UBER", "name": "Uber"},
                    {"ticker": "ABNB", "name": "Airbnb"},
                    {"ticker": "DASH", "name": "DoorDash"},
                    {"ticker": "BKNG", "name": "Booking Holdings"},
                    {"ticker": "PINS", "name": "Pinterest"},
                    {"ticker": "SNAP", "name": "Snap"},
                    {"ticker": "SPOT", "name": "Spotify"},
                    {"ticker": "TWLO", "name": "Twilio"},
                    {"ticker": "ZM", "name": "Zoom Video"},
                    {"ticker": "AKAM", "name": "Akamai Technologies"},
                    {"ticker": "GDDY", "name": "GoDaddy"},
                    {"ticker": "ESTC", "name": "Elastic"},
                ],
            },
            "IT Services / Hardware": {
                "etf": None,
                "companies": [
                    {"ticker": "AAPL", "name": "Apple", "market_cap": 3_400_000_000_000, "weight": 0.35},
                    {"ticker": "IBM", "name": "IBM", "market_cap": 200_000_000_000, "weight": 0.05},
                    {"ticker": "ACN", "name": "Accenture", "market_cap": 200_000_000_000, "weight": 0.05},
                    {"ticker": "CSCO", "name": "Cisco", "market_cap": 220_000_000_000, "weight": 0.05},
                    {"ticker": "HPQ", "name": "HP Inc."},
                    {"ticker": "HPE", "name": "Hewlett Packard Enterprise"},
                    {"ticker": "DELL", "name": "Dell Technologies"},
                    {"ticker": "ANET", "name": "Arista Networks"},
                    {"ticker": "KEYS", "name": "Keysight Technologies"},
                    {"ticker": "IT", "name": "Gartner"},
                    {"ticker": "CTSH", "name": "Cognizant"},
                    {"ticker": "EPAM", "name": "EPAM Systems"},
                    {"ticker": "GDDY", "name": "GoDaddy"},
                    {"ticker": "WIT", "name": "Wipro"},
                    {"ticker": "CDW", "name": "CDW"},
                    {"ticker": "JNPR", "name": "Juniper Networks"},
                    {"ticker": "NTAP", "name": "NetApp"},
                    {"ticker": "STX", "name": "Seagate Technology"},
                    {"ticker": "WDC", "name": "Western Digital"},
                    {"ticker": "ZBRA", "name": "Zebra Technologies"},
                    {"ticker": "TDY", "name": "Teledyne Technologies"},
                    {"ticker": "GLW", "name": "Corning"},
                    {"ticker": "TEL", "name": "TE Connectivity"},
                    {"ticker": "APH", "name": "Amphenol"},
                ],
            },
            "Payments / Fintech": {
                "etf": "IPAY",
                "companies": [
                    {"ticker": "V", "name": "Visa", "market_cap": 580_000_000_000, "weight": 0.25},
                    {"ticker": "MA", "name": "Mastercard", "market_cap": 430_000_000_000, "weight": 0.20},
                    {"ticker": "PYPL", "name": "PayPal", "market_cap": 70_000_000_000, "weight": 0.06},
                    {"ticker": "AXP", "name": "American Express", "market_cap": 180_000_000_000, "weight": 0.10},
                    {"ticker": "SQ", "name": "Block (Square)"},
                    {"ticker": "FIS", "name": "Fidelity National Information Services"},
                    {"ticker": "FISV", "name": "Fiserv"},
                    {"ticker": "GPN", "name": "Global Payments"},
                    {"ticker": "ADYEN", "name": "Adyen"},
                    {"ticker": "COIN", "name": "Coinbase"},
                    {"ticker": "AFRM", "name": "Affirm"},
                    {"ticker": "WEX", "name": "WEX"},
                    {"ticker": "FOUR", "name": "Shift4 Payments"},
                    {"ticker": "TOST", "name": "Toast"},
                ],
            },
        },
    },
    # -------------------------------------------------------------------------
    # 2. HEALTHCARE (XLV)
    # -------------------------------------------------------------------------
    "Healthcare": {
        "etf": "XLV",
        "industries": {
            "Pharmaceuticals": {
                "etf": None,
                "companies": [
                    {"ticker": "LLY", "name": "Eli Lilly", "market_cap": 750_000_000_000, "weight": 0.20},
                    {"ticker": "JNJ", "name": "Johnson & Johnson", "market_cap": 380_000_000_000, "weight": 0.10},
                    {"ticker": "MRK", "name": "Merck", "market_cap": 300_000_000_000, "weight": 0.08},
                    {"ticker": "ABBV", "name": "AbbVie", "market_cap": 310_000_000_000, "weight": 0.08},
                    {"ticker": "PFE", "name": "Pfizer", "market_cap": 150_000_000_000, "weight": 0.04},
                    {"ticker": "BMY", "name": "Bristol-Myers Squibb"},
                    {"ticker": "NVO", "name": "Novo Nordisk"},
                    {"ticker": "AZN", "name": "AstraZeneca"},
                    {"ticker": "SNY", "name": "Sanofi"},
                    {"ticker": "GSK", "name": "GSK"},
                    {"ticker": "ZTS", "name": "Zoetis"},
                    {"ticker": "VRTX", "name": "Vertex Pharmaceuticals"},
                    {"ticker": "REGN", "name": "Regeneron"},
                    {"ticker": "MRNA", "name": "Moderna"},
                    {"ticker": "BIIB", "name": "Biogen"},
                    {"ticker": "GILD", "name": "Gilead Sciences"},
                    {"ticker": "TAK", "name": "Takeda"},
                    {"ticker": "CTLT", "name": "Catalent"},
                ],
            },
            "Biotech": {
                "etf": "XBI",
                "companies": [
                    {"ticker": "AMGN", "name": "Amgen", "market_cap": 150_000_000_000, "weight": 0.15},
                    {"ticker": "VRTX", "name": "Vertex Pharmaceuticals", "market_cap": 120_000_000_000, "weight": 0.12},
                    {"ticker": "REGN", "name": "Regeneron", "market_cap": 110_000_000_000, "weight": 0.10},
                    {"ticker": "GILD", "name": "Gilead Sciences", "market_cap": 100_000_000_000, "weight": 0.10},
                    {"ticker": "MRNA", "name": "Moderna"},
                    {"ticker": "BIIB", "name": "Biogen"},
                    {"ticker": "ILMN", "name": "Illumina"},
                    {"ticker": "ALNY", "name": "Alnylam Pharmaceuticals"},
                    {"ticker": "SGEN", "name": "Seagen"},
                    {"ticker": "BMRN", "name": "BioMarin"},
                    {"ticker": "INCY", "name": "Incyte"},
                    {"ticker": "EXAS", "name": "Exact Sciences"},
                    {"ticker": "TECH", "name": "Bio-Techne"},
                    {"ticker": "UTHR", "name": "United Therapeutics"},
                ],
            },
            "Medical Devices": {
                "etf": "IHI",
                "companies": [
                    {"ticker": "ABT", "name": "Abbott Laboratories", "market_cap": 200_000_000_000, "weight": 0.15},
                    {"ticker": "MDT", "name": "Medtronic", "market_cap": 110_000_000_000, "weight": 0.10},
                    {"ticker": "SYK", "name": "Stryker", "market_cap": 130_000_000_000, "weight": 0.10},
                    {"ticker": "BSX", "name": "Boston Scientific", "market_cap": 120_000_000_000, "weight": 0.10},
                    {"ticker": "ISRG", "name": "Intuitive Surgical", "market_cap": 160_000_000_000, "weight": 0.12},
                    {"ticker": "EW", "name": "Edwards Lifesciences"},
                    {"ticker": "ZBH", "name": "Zimmer Biomet"},
                    {"ticker": "BAX", "name": "Baxter International"},
                    {"ticker": "BDX", "name": "Becton Dickinson"},
                    {"ticker": "HOLX", "name": "Hologic"},
                    {"ticker": "ALGN", "name": "Align Technology"},
                    {"ticker": "DXCM", "name": "DexCom"},
                    {"ticker": "PODD", "name": "Insulet"},
                    {"ticker": "IDXX", "name": "IDEXX Laboratories"},
                    {"ticker": "WAT", "name": "Waters"},
                    {"ticker": "TFX", "name": "Teleflex"},
                ],
            },
            "Managed Care / Health Services": {
                "etf": None,
                "companies": [
                    {"ticker": "UNH", "name": "UnitedHealth Group", "market_cap": 450_000_000_000, "weight": 0.25},
                    {"ticker": "ELV", "name": "Elevance Health", "market_cap": 110_000_000_000, "weight": 0.08},
                    {"ticker": "CI", "name": "Cigna Group", "market_cap": 95_000_000_000, "weight": 0.06},
                    {"ticker": "HUM", "name": "Humana", "market_cap": 45_000_000_000, "weight": 0.03},
                    {"ticker": "CNC", "name": "Centene"},
                    {"ticker": "MOH", "name": "Molina Healthcare"},
                    {"ticker": "HCA", "name": "HCA Healthcare"},
                    {"ticker": "CVS", "name": "CVS Health"},
                    {"ticker": "MCK", "name": "McKesson"},
                    {"ticker": "CAH", "name": "Cardinal Health"},
                    {"ticker": "ABC", "name": "AmerisourceBergen"},
                    {"ticker": "COR", "name": "Cencora"},
                ],
            },
            "Life Science Tools": {
                "etf": None,
                "companies": [
                    {"ticker": "TMO", "name": "Thermo Fisher Scientific", "market_cap": 200_000_000_000, "weight": 0.20},
                    {"ticker": "DHR", "name": "Danaher", "market_cap": 180_000_000_000, "weight": 0.18},
                    {"ticker": "A", "name": "Agilent Technologies"},
                    {"ticker": "IQV", "name": "IQVIA"},
                    {"ticker": "MTD", "name": "Mettler-Toledo"},
                    {"ticker": "RVTY", "name": "Revvity"},
                    {"ticker": "BIO", "name": "Bio-Rad Laboratories"},
                    {"ticker": "CRL", "name": "Charles River Laboratories"},
                    {"ticker": "WST", "name": "West Pharmaceutical Services"},
                    {"ticker": "RGEN", "name": "Repligen"},
                ],
            },
        },
    },
    # -------------------------------------------------------------------------
    # 3. FINANCIALS (XLF)
    # -------------------------------------------------------------------------
    "Financials": {
        "etf": "XLF",
        "industries": {
            "Banks": {
                "etf": "KBE",
                "companies": [
                    {"ticker": "JPM", "name": "JPMorgan Chase", "market_cap": 600_000_000_000, "weight": 0.20},
                    {"ticker": "BAC", "name": "Bank of America", "market_cap": 300_000_000_000, "weight": 0.10},
                    {"ticker": "WFC", "name": "Wells Fargo", "market_cap": 200_000_000_000, "weight": 0.07},
                    {"ticker": "C", "name": "Citigroup", "market_cap": 100_000_000_000, "weight": 0.04},
                    {"ticker": "GS", "name": "Goldman Sachs", "market_cap": 150_000_000_000, "weight": 0.05},
                    {"ticker": "MS", "name": "Morgan Stanley", "market_cap": 150_000_000_000, "weight": 0.05},
                    {"ticker": "USB", "name": "U.S. Bancorp"},
                    {"ticker": "PNC", "name": "PNC Financial"},
                    {"ticker": "TFC", "name": "Truist Financial"},
                    {"ticker": "SCHW", "name": "Charles Schwab"},
                    {"ticker": "BK", "name": "Bank of New York Mellon"},
                    {"ticker": "STT", "name": "State Street"},
                    {"ticker": "FITB", "name": "Fifth Third Bancorp"},
                    {"ticker": "RF", "name": "Regions Financial"},
                    {"ticker": "HBAN", "name": "Huntington Bancshares"},
                    {"ticker": "KEY", "name": "KeyCorp"},
                    {"ticker": "CFG", "name": "Citizens Financial"},
                    {"ticker": "MTB", "name": "M&T Bank"},
                    {"ticker": "SIVB", "name": "SVB Financial Group"},
                    {"ticker": "NTRS", "name": "Northern Trust"},
                    {"ticker": "ZION", "name": "Zions Bancorp"},
                    {"ticker": "CMA", "name": "Comerica"},
                ],
            },
            "Insurance": {
                "etf": "KIE",
                "companies": [
                    {"ticker": "BRK-B", "name": "Berkshire Hathaway", "market_cap": 900_000_000_000, "weight": 0.30},
                    {"ticker": "PGR", "name": "Progressive", "market_cap": 130_000_000_000, "weight": 0.06},
                    {"ticker": "CB", "name": "Chubb", "market_cap": 110_000_000_000, "weight": 0.05},
                    {"ticker": "MMC", "name": "Marsh & McLennan", "market_cap": 100_000_000_000, "weight": 0.05},
                    {"ticker": "AON", "name": "Aon"},
                    {"ticker": "MET", "name": "MetLife"},
                    {"ticker": "PRU", "name": "Prudential Financial"},
                    {"ticker": "AIG", "name": "American International Group"},
                    {"ticker": "AFL", "name": "Aflac"},
                    {"ticker": "TRV", "name": "Travelers"},
                    {"ticker": "ALL", "name": "Allstate"},
                    {"ticker": "CINF", "name": "Cincinnati Financial"},
                    {"ticker": "GL", "name": "Globe Life"},
                    {"ticker": "HIG", "name": "Hartford Financial"},
                    {"ticker": "LNC", "name": "Lincoln National"},
                    {"ticker": "L", "name": "Loews"},
                    {"ticker": "WRB", "name": "W.R. Berkley"},
                    {"ticker": "RNR", "name": "RenaissanceRe"},
                    {"ticker": "BRO", "name": "Brown & Brown"},
                    {"ticker": "AJG", "name": "Arthur J. Gallagher"},
                ],
            },
            "Asset Management": {
                "etf": None,
                "companies": [
                    {"ticker": "BLK", "name": "BlackRock", "market_cap": 130_000_000_000, "weight": 0.15},
                    {"ticker": "BX", "name": "Blackstone", "market_cap": 160_000_000_000, "weight": 0.15},
                    {"ticker": "KKR", "name": "KKR", "market_cap": 100_000_000_000, "weight": 0.10},
                    {"ticker": "APO", "name": "Apollo Global Management", "market_cap": 80_000_000_000, "weight": 0.08},
                    {"ticker": "TROW", "name": "T. Rowe Price"},
                    {"ticker": "IVZ", "name": "Invesco"},
                    {"ticker": "BEN", "name": "Franklin Resources"},
                    {"ticker": "AMG", "name": "Affiliated Managers Group"},
                    {"ticker": "ARES", "name": "Ares Management"},
                    {"ticker": "CG", "name": "Carlyle Group"},
                    {"ticker": "OWL", "name": "Blue Owl Capital"},
                ],
            },
            "Capital Markets / Exchanges": {
                "etf": None,
                "companies": [
                    {"ticker": "SPGI", "name": "S&P Global", "market_cap": 150_000_000_000, "weight": 0.12},
                    {"ticker": "ICE", "name": "Intercontinental Exchange", "market_cap": 80_000_000_000, "weight": 0.06},
                    {"ticker": "CME", "name": "CME Group", "market_cap": 80_000_000_000, "weight": 0.06},
                    {"ticker": "MCO", "name": "Moody's", "market_cap": 80_000_000_000, "weight": 0.06},
                    {"ticker": "NDAQ", "name": "Nasdaq"},
                    {"ticker": "CBOE", "name": "Cboe Global Markets"},
                    {"ticker": "MKTX", "name": "MarketAxess"},
                    {"ticker": "LPLA", "name": "LPL Financial"},
                    {"ticker": "RJF", "name": "Raymond James Financial"},
                    {"ticker": "IBKR", "name": "Interactive Brokers"},
                    {"ticker": "HOOD", "name": "Robinhood"},
                    {"ticker": "FDS", "name": "FactSet Research"},
                ],
            },
            "Consumer Finance": {
                "etf": None,
                "companies": [
                    {"ticker": "AXP", "name": "American Express", "market_cap": 180_000_000_000, "weight": 0.15},
                    {"ticker": "COF", "name": "Capital One", "market_cap": 55_000_000_000, "weight": 0.05},
                    {"ticker": "DFS", "name": "Discover Financial", "market_cap": 35_000_000_000, "weight": 0.03},
                    {"ticker": "SYF", "name": "Synchrony Financial"},
                    {"ticker": "ALLY", "name": "Ally Financial"},
                    {"ticker": "SOFI", "name": "SoFi Technologies"},
                    {"ticker": "LC", "name": "LendingClub"},
                    {"ticker": "CACC", "name": "Credit Acceptance"},
                    {"ticker": "OMF", "name": "OneMain Financial"},
                    {"ticker": "NAVI", "name": "Navient"},
                ],
            },
        },
    },
    # -------------------------------------------------------------------------
    # 4. ENERGY (XLE)
    # -------------------------------------------------------------------------
    "Energy": {
        "etf": "XLE",
        "industries": {
            "Integrated Oil & Gas": {
                "etf": None,
                "companies": [
                    {"ticker": "XOM", "name": "Exxon Mobil", "market_cap": 450_000_000_000, "weight": 0.25},
                    {"ticker": "CVX", "name": "Chevron", "market_cap": 290_000_000_000, "weight": 0.15},
                    {"ticker": "SHEL", "name": "Shell"},
                    {"ticker": "BP", "name": "BP"},
                    {"ticker": "TTE", "name": "TotalEnergies"},
                    {"ticker": "ENB", "name": "Enbridge"},
                    {"ticker": "SU", "name": "Suncor Energy"},
                    {"ticker": "CNQ", "name": "Canadian Natural Resources"},
                    {"ticker": "E", "name": "Eni"},
                    {"ticker": "EQNR", "name": "Equinor"},
                ],
            },
            "Exploration & Production": {
                "etf": "XOP",
                "companies": [
                    {"ticker": "COP", "name": "ConocoPhillips", "market_cap": 130_000_000_000, "weight": 0.15},
                    {"ticker": "EOG", "name": "EOG Resources", "market_cap": 70_000_000_000, "weight": 0.08},
                    {"ticker": "PXD", "name": "Pioneer Natural Resources"},
                    {"ticker": "DVN", "name": "Devon Energy"},
                    {"ticker": "FANG", "name": "Diamondback Energy"},
                    {"ticker": "MRO", "name": "Marathon Oil"},
                    {"ticker": "OXY", "name": "Occidental Petroleum"},
                    {"ticker": "HES", "name": "Hess"},
                    {"ticker": "APA", "name": "APA Corporation"},
                    {"ticker": "EQT", "name": "EQT Corporation"},
                    {"ticker": "AR", "name": "Antero Resources"},
                    {"ticker": "RRC", "name": "Range Resources"},
                    {"ticker": "PR", "name": "Permian Resources"},
                    {"ticker": "CTRA", "name": "Coterra Energy"},
                    {"ticker": "MTDR", "name": "Matador Resources"},
                ],
            },
            "Oil Services & Equipment": {
                "etf": "OIH",
                "companies": [
                    {"ticker": "SLB", "name": "Schlumberger (SLB)", "market_cap": 65_000_000_000, "weight": 0.20},
                    {"ticker": "HAL", "name": "Halliburton", "market_cap": 28_000_000_000, "weight": 0.10},
                    {"ticker": "BKR", "name": "Baker Hughes", "market_cap": 35_000_000_000, "weight": 0.10},
                    {"ticker": "NOV", "name": "NOV"},
                    {"ticker": "FTI", "name": "TechnipFMC"},
                    {"ticker": "CHX", "name": "ChampionX"},
                    {"ticker": "WFRD", "name": "Weatherford International"},
                    {"ticker": "LBRT", "name": "Liberty Energy"},
                    {"ticker": "HP", "name": "Helmerich & Payne"},
                    {"ticker": "RIG", "name": "Transocean"},
                ],
            },
            "Midstream / Pipelines": {
                "etf": "AMLP",
                "companies": [
                    {"ticker": "WMB", "name": "Williams Companies", "market_cap": 55_000_000_000, "weight": 0.12},
                    {"ticker": "KMI", "name": "Kinder Morgan", "market_cap": 45_000_000_000, "weight": 0.10},
                    {"ticker": "OKE", "name": "ONEOK", "market_cap": 55_000_000_000, "weight": 0.10},
                    {"ticker": "ET", "name": "Energy Transfer"},
                    {"ticker": "EPD", "name": "Enterprise Products Partners"},
                    {"ticker": "MPLX", "name": "MPLX"},
                    {"ticker": "TRGP", "name": "Targa Resources"},
                    {"ticker": "LNG", "name": "Cheniere Energy"},
                    {"ticker": "DTM", "name": "DT Midstream"},
                    {"ticker": "AM", "name": "Antero Midstream"},
                ],
            },
            "Refining & Marketing": {
                "etf": None,
                "companies": [
                    {"ticker": "MPC", "name": "Marathon Petroleum", "market_cap": 55_000_000_000, "weight": 0.15},
                    {"ticker": "VLO", "name": "Valero Energy", "market_cap": 45_000_000_000, "weight": 0.12},
                    {"ticker": "PSX", "name": "Phillips 66", "market_cap": 50_000_000_000, "weight": 0.12},
                    {"ticker": "DINO", "name": "HF Sinclair"},
                    {"ticker": "PBF", "name": "PBF Energy"},
                    {"ticker": "DK", "name": "Delek US Holdings"},
                    {"ticker": "CVI", "name": "CVR Energy"},
                    {"ticker": "PARR", "name": "Par Pacific"},
                    {"ticker": "CLMT", "name": "Calumet Specialty Products"},
                    {"ticker": "CEIX", "name": "CONSOL Energy"},
                ],
            },
            "Renewable Energy": {
                "etf": "ICLN",
                "companies": [
                    {"ticker": "NEE", "name": "NextEra Energy", "market_cap": 150_000_000_000, "weight": 0.20},
                    {"ticker": "ENPH", "name": "Enphase Energy"},
                    {"ticker": "SEDG", "name": "SolarEdge Technologies"},
                    {"ticker": "FSLR", "name": "First Solar"},
                    {"ticker": "RUN", "name": "Sunrun"},
                    {"ticker": "NOVA", "name": "Sunnova Energy"},
                    {"ticker": "PLUG", "name": "Plug Power"},
                    {"ticker": "BE", "name": "Bloom Energy"},
                    {"ticker": "ARRY", "name": "Array Technologies"},
                    {"ticker": "CSIQ", "name": "Canadian Solar"},
                    {"ticker": "ORA", "name": "Ormat Technologies"},
                ],
            },
        },
    },
    # -------------------------------------------------------------------------
    # 5. CONSUMER DISCRETIONARY (XLY)
    # -------------------------------------------------------------------------
    "Consumer Discretionary": {
        "etf": "XLY",
        "industries": {
            "Auto & EV": {
                "etf": None,
                "companies": [
                    {"ticker": "TSLA", "name": "Tesla", "market_cap": 800_000_000_000, "weight": 0.30},
                    {"ticker": "TM", "name": "Toyota Motor"},
                    {"ticker": "GM", "name": "General Motors"},
                    {"ticker": "F", "name": "Ford Motor"},
                    {"ticker": "RIVN", "name": "Rivian Automotive"},
                    {"ticker": "LCID", "name": "Lucid Group"},
                    {"ticker": "NIO", "name": "NIO"},
                    {"ticker": "XPEV", "name": "XPeng"},
                    {"ticker": "LI", "name": "Li Auto"},
                    {"ticker": "STLA", "name": "Stellantis"},
                    {"ticker": "HMC", "name": "Honda Motor"},
                    {"ticker": "APTV", "name": "Aptiv"},
                    {"ticker": "BWA", "name": "BorgWarner"},
                    {"ticker": "ALV", "name": "Autoliv"},
                    {"ticker": "LEA", "name": "Lear"},
                ],
            },
            "Retail - Discretionary": {
                "etf": "XRT",
                "companies": [
                    {"ticker": "AMZN", "name": "Amazon", "market_cap": 2_000_000_000_000, "weight": 0.35},
                    {"ticker": "HD", "name": "Home Depot", "market_cap": 370_000_000_000, "weight": 0.10},
                    {"ticker": "LOW", "name": "Lowe's", "market_cap": 140_000_000_000, "weight": 0.05},
                    {"ticker": "TJX", "name": "TJX Companies"},
                    {"ticker": "ROST", "name": "Ross Stores"},
                    {"ticker": "BURL", "name": "Burlington Stores"},
                    {"ticker": "TGT", "name": "Target"},
                    {"ticker": "BBY", "name": "Best Buy"},
                    {"ticker": "ORLY", "name": "O'Reilly Automotive"},
                    {"ticker": "AZO", "name": "AutoZone"},
                    {"ticker": "AAP", "name": "Advance Auto Parts"},
                    {"ticker": "ULTA", "name": "Ulta Beauty"},
                    {"ticker": "DG", "name": "Dollar General"},
                    {"ticker": "DLTR", "name": "Dollar Tree"},
                    {"ticker": "FIVE", "name": "Five Below"},
                    {"ticker": "KMX", "name": "CarMax"},
                    {"ticker": "TSCO", "name": "Tractor Supply"},
                    {"ticker": "WSM", "name": "Williams-Sonoma"},
                    {"ticker": "RH", "name": "RH"},
                    {"ticker": "W", "name": "Wayfair"},
                    {"ticker": "ETSY", "name": "Etsy"},
                ],
            },
            "Restaurants & Leisure": {
                "etf": None,
                "companies": [
                    {"ticker": "MCD", "name": "McDonald's", "market_cap": 210_000_000_000, "weight": 0.15},
                    {"ticker": "SBUX", "name": "Starbucks", "market_cap": 110_000_000_000, "weight": 0.08},
                    {"ticker": "CMG", "name": "Chipotle Mexican Grill"},
                    {"ticker": "YUM", "name": "Yum! Brands"},
                    {"ticker": "DRI", "name": "Darden Restaurants"},
                    {"ticker": "WING", "name": "Wingstop"},
                    {"ticker": "CAVA", "name": "CAVA Group"},
                    {"ticker": "DPZ", "name": "Domino's Pizza"},
                    {"ticker": "QSR", "name": "Restaurant Brands International"},
                    {"ticker": "DINE", "name": "Dine Brands"},
                    {"ticker": "EAT", "name": "Brinker International"},
                    {"ticker": "TXRH", "name": "Texas Roadhouse"},
                ],
            },
            "Hotels, Travel & Gaming": {
                "etf": None,
                "companies": [
                    {"ticker": "BKNG", "name": "Booking Holdings", "market_cap": 150_000_000_000, "weight": 0.15},
                    {"ticker": "MAR", "name": "Marriott International"},
                    {"ticker": "HLT", "name": "Hilton Worldwide"},
                    {"ticker": "H", "name": "Hyatt Hotels"},
                    {"ticker": "WYNN", "name": "Wynn Resorts"},
                    {"ticker": "LVS", "name": "Las Vegas Sands"},
                    {"ticker": "MGM", "name": "MGM Resorts"},
                    {"ticker": "CZR", "name": "Caesars Entertainment"},
                    {"ticker": "ABNB", "name": "Airbnb"},
                    {"ticker": "EXPE", "name": "Expedia Group"},
                    {"ticker": "RCL", "name": "Royal Caribbean"},
                    {"ticker": "CCL", "name": "Carnival"},
                    {"ticker": "NCLH", "name": "Norwegian Cruise Line"},
                    {"ticker": "DKNG", "name": "DraftKings"},
                    {"ticker": "FLUT", "name": "Flutter Entertainment"},
                ],
            },
            "Apparel & Luxury": {
                "etf": None,
                "companies": [
                    {"ticker": "NKE", "name": "Nike", "market_cap": 120_000_000_000, "weight": 0.10},
                    {"ticker": "LULU", "name": "Lululemon Athletica"},
                    {"ticker": "TPR", "name": "Tapestry"},
                    {"ticker": "RL", "name": "Ralph Lauren"},
                    {"ticker": "CPRI", "name": "Capri Holdings"},
                    {"ticker": "PVH", "name": "PVH Corp"},
                    {"ticker": "HBI", "name": "Hanesbrands"},
                    {"ticker": "VFC", "name": "VF Corporation"},
                    {"ticker": "DECK", "name": "Deckers Outdoor"},
                    {"ticker": "CROX", "name": "Crocs"},
                    {"ticker": "SKX", "name": "Skechers"},
                    {"ticker": "UAA", "name": "Under Armour"},
                ],
            },
        },
    },
    # -------------------------------------------------------------------------
    # 6. CONSUMER STAPLES (XLP)
    # -------------------------------------------------------------------------
    "Consumer Staples": {
        "etf": "XLP",
        "industries": {
            "Beverages": {
                "etf": None,
                "companies": [
                    {"ticker": "KO", "name": "Coca-Cola", "market_cap": 260_000_000_000, "weight": 0.20},
                    {"ticker": "PEP", "name": "PepsiCo", "market_cap": 230_000_000_000, "weight": 0.18},
                    {"ticker": "MDLZ", "name": "Mondelez International"},
                    {"ticker": "KDP", "name": "Keurig Dr Pepper"},
                    {"ticker": "STZ", "name": "Constellation Brands"},
                    {"ticker": "BF-B", "name": "Brown-Forman"},
                    {"ticker": "SAM", "name": "Boston Beer"},
                    {"ticker": "MNST", "name": "Monster Beverage"},
                    {"ticker": "CELH", "name": "Celsius Holdings"},
                    {"ticker": "DEO", "name": "Diageo"},
                ],
            },
            "Food Products": {
                "etf": None,
                "companies": [
                    {"ticker": "GIS", "name": "General Mills"},
                    {"ticker": "K", "name": "Kellanova"},
                    {"ticker": "CPB", "name": "Campbell Soup"},
                    {"ticker": "CAG", "name": "Conagra Brands"},
                    {"ticker": "SJM", "name": "J.M. Smucker"},
                    {"ticker": "HSY", "name": "Hershey"},
                    {"ticker": "MKC", "name": "McCormick"},
                    {"ticker": "HRL", "name": "Hormel Foods"},
                    {"ticker": "TSN", "name": "Tyson Foods"},
                    {"ticker": "ADM", "name": "Archer-Daniels-Midland"},
                    {"ticker": "BG", "name": "Bunge Global"},
                    {"ticker": "INGR", "name": "Ingredion"},
                    {"ticker": "DAR", "name": "Darling Ingredients"},
                    {"ticker": "LANC", "name": "Lancaster Colony"},
                ],
            },
            "Household & Personal Products": {
                "etf": None,
                "companies": [
                    {"ticker": "PG", "name": "Procter & Gamble", "market_cap": 380_000_000_000, "weight": 0.25},
                    {"ticker": "CL", "name": "Colgate-Palmolive", "market_cap": 75_000_000_000, "weight": 0.06},
                    {"ticker": "KMB", "name": "Kimberly-Clark"},
                    {"ticker": "CHD", "name": "Church & Dwight"},
                    {"ticker": "CLX", "name": "Clorox"},
                    {"ticker": "EL", "name": "Estee Lauder"},
                    {"ticker": "SWK", "name": "Stanley Black & Decker"},
                    {"ticker": "HELE", "name": "Helen of Troy"},
                    {"ticker": "SPB", "name": "Spectrum Brands"},
                    {"ticker": "COTY", "name": "Coty"},
                    {"ticker": "EPC", "name": "Energizer Holdings"},
                ],
            },
            "Retail - Staples": {
                "etf": None,
                "companies": [
                    {"ticker": "WMT", "name": "Walmart", "market_cap": 550_000_000_000, "weight": 0.30},
                    {"ticker": "COST", "name": "Costco", "market_cap": 350_000_000_000, "weight": 0.22},
                    {"ticker": "KR", "name": "Kroger"},
                    {"ticker": "WBA", "name": "Walgreens Boots Alliance"},
                    {"ticker": "SYY", "name": "Sysco"},
                    {"ticker": "PFGC", "name": "Performance Food Group"},
                    {"ticker": "USFD", "name": "US Foods"},
                    {"ticker": "ACI", "name": "Albertsons"},
                    {"ticker": "BJ", "name": "BJ's Wholesale Club"},
                    {"ticker": "GO", "name": "Grocery Outlet"},
                    {"ticker": "SFM", "name": "Sprouts Farmers Market"},
                ],
            },
            "Tobacco": {
                "etf": None,
                "companies": [
                    {"ticker": "PM", "name": "Philip Morris International", "market_cap": 190_000_000_000, "weight": 0.35},
                    {"ticker": "MO", "name": "Altria Group", "market_cap": 85_000_000_000, "weight": 0.15},
                    {"ticker": "BTI", "name": "British American Tobacco"},
                    {"ticker": "TPB", "name": "Turning Point Brands"},
                ],
            },
        },
    },
    # -------------------------------------------------------------------------
    # 7. INDUSTRIALS (XLI)
    # -------------------------------------------------------------------------
    "Industrials": {
        "etf": "XLI",
        "industries": {
            "Aerospace & Defense": {
                "etf": "ITA",
                "companies": [
                    {"ticker": "RTX", "name": "RTX Corporation", "market_cap": 150_000_000_000, "weight": 0.12},
                    {"ticker": "BA", "name": "Boeing", "market_cap": 130_000_000_000, "weight": 0.10},
                    {"ticker": "LMT", "name": "Lockheed Martin", "market_cap": 120_000_000_000, "weight": 0.10},
                    {"ticker": "GD", "name": "General Dynamics", "market_cap": 80_000_000_000, "weight": 0.06},
                    {"ticker": "NOC", "name": "Northrop Grumman"},
                    {"ticker": "LHX", "name": "L3Harris Technologies"},
                    {"ticker": "GE", "name": "GE Aerospace"},
                    {"ticker": "HWM", "name": "Howmet Aerospace"},
                    {"ticker": "TXT", "name": "Textron"},
                    {"ticker": "HII", "name": "Huntington Ingalls"},
                    {"ticker": "TDG", "name": "TransDigm Group"},
                    {"ticker": "HEI", "name": "HEICO"},
                    {"ticker": "SPR", "name": "Spirit AeroSystems"},
                    {"ticker": "AXON", "name": "Axon Enterprise"},
                    {"ticker": "KTOS", "name": "Kratos Defense"},
                ],
            },
            "Electrical Equipment / Multi-Industry": {
                "etf": None,
                "companies": [
                    {"ticker": "HON", "name": "Honeywell", "market_cap": 130_000_000_000, "weight": 0.08},
                    {"ticker": "CAT", "name": "Caterpillar", "market_cap": 160_000_000_000, "weight": 0.10},
                    {"ticker": "DE", "name": "Deere & Company", "market_cap": 120_000_000_000, "weight": 0.08},
                    {"ticker": "ETN", "name": "Eaton", "market_cap": 120_000_000_000, "weight": 0.08},
                    {"ticker": "EMR", "name": "Emerson Electric"},
                    {"ticker": "ROK", "name": "Rockwell Automation"},
                    {"ticker": "AME", "name": "AMETEK"},
                    {"ticker": "NDSN", "name": "Nordson"},
                    {"ticker": "PH", "name": "Parker-Hannifin"},
                    {"ticker": "ITW", "name": "Illinois Tool Works"},
                    {"ticker": "MMM", "name": "3M"},
                    {"ticker": "GE", "name": "GE Vernova"},
                    {"ticker": "IR", "name": "Ingersoll Rand"},
                    {"ticker": "DOV", "name": "Dover"},
                    {"ticker": "SWK", "name": "Stanley Black & Decker"},
                    {"ticker": "GNRC", "name": "Generac"},
                    {"ticker": "XYL", "name": "Xylem"},
                    {"ticker": "RRX", "name": "Regal Rexnord"},
                ],
            },
            "Transportation": {
                "etf": "IYT",
                "companies": [
                    {"ticker": "UNP", "name": "Union Pacific", "market_cap": 150_000_000_000, "weight": 0.12},
                    {"ticker": "UPS", "name": "United Parcel Service", "market_cap": 120_000_000_000, "weight": 0.10},
                    {"ticker": "FDX", "name": "FedEx"},
                    {"ticker": "CSX", "name": "CSX"},
                    {"ticker": "NSC", "name": "Norfolk Southern"},
                    {"ticker": "DAL", "name": "Delta Air Lines"},
                    {"ticker": "LUV", "name": "Southwest Airlines"},
                    {"ticker": "UAL", "name": "United Airlines"},
                    {"ticker": "AAL", "name": "American Airlines"},
                    {"ticker": "ALK", "name": "Alaska Air Group"},
                    {"ticker": "JBHT", "name": "J.B. Hunt Transport"},
                    {"ticker": "ODFL", "name": "Old Dominion Freight Line"},
                    {"ticker": "XPO", "name": "XPO"},
                    {"ticker": "CHRW", "name": "C.H. Robinson"},
                    {"ticker": "EXPD", "name": "Expeditors International"},
                    {"ticker": "KEX", "name": "Kirby"},
                    {"ticker": "MATX", "name": "Matson"},
                    {"ticker": "SNDR", "name": "Schneider National"},
                ],
            },
            "Building & Construction": {
                "etf": "ITB",
                "companies": [
                    {"ticker": "SHW", "name": "Sherwin-Williams", "market_cap": 85_000_000_000, "weight": 0.08},
                    {"ticker": "JCI", "name": "Johnson Controls"},
                    {"ticker": "CARR", "name": "Carrier Global"},
                    {"ticker": "TT", "name": "Trane Technologies"},
                    {"ticker": "LII", "name": "Lennox International"},
                    {"ticker": "VMC", "name": "Vulcan Materials"},
                    {"ticker": "MLM", "name": "Martin Marietta Materials"},
                    {"ticker": "MAS", "name": "Masco"},
                    {"ticker": "FAST", "name": "Fastenal"},
                    {"ticker": "PWR", "name": "Quanta Services"},
                    {"ticker": "WSC", "name": "WillScot Mobile Mini"},
                    {"ticker": "DHI", "name": "D.R. Horton"},
                    {"ticker": "LEN", "name": "Lennar"},
                    {"ticker": "PHM", "name": "PulteGroup"},
                    {"ticker": "NVR", "name": "NVR"},
                    {"ticker": "TOL", "name": "Toll Brothers"},
                    {"ticker": "MTH", "name": "Meritage Homes"},
                    {"ticker": "KBH", "name": "KB Home"},
                ],
            },
            "Waste Management / Environmental Services": {
                "etf": None,
                "companies": [
                    {"ticker": "WM", "name": "Waste Management", "market_cap": 80_000_000_000, "weight": 0.15},
                    {"ticker": "RSG", "name": "Republic Services", "market_cap": 60_000_000_000, "weight": 0.10},
                    {"ticker": "WCN", "name": "Waste Connections"},
                    {"ticker": "SRCL", "name": "Stericycle"},
                    {"ticker": "CLH", "name": "Clean Harbors"},
                    {"ticker": "GFL", "name": "GFL Environmental"},
                    {"ticker": "CWST", "name": "Casella Waste Systems"},
                    {"ticker": "ECOL", "name": "US Ecology"},
                    {"ticker": "CSWI", "name": "CSW Industrials"},
                    {"ticker": "VRSK", "name": "Verisk Analytics"},
                ],
            },
            "Staffing & Business Services": {
                "etf": None,
                "companies": [
                    {"ticker": "ADP", "name": "Automatic Data Processing", "market_cap": 110_000_000_000, "weight": 0.10},
                    {"ticker": "PAYX", "name": "Paychex"},
                    {"ticker": "CTAS", "name": "Cintas"},
                    {"ticker": "RHI", "name": "Robert Half"},
                    {"ticker": "INFO", "name": "IHS Markit"},
                    {"ticker": "BR", "name": "Broadridge Financial Solutions"},
                    {"ticker": "CPRT", "name": "Copart"},
                    {"ticker": "WAB", "name": "Westinghouse Air Brake"},
                    {"ticker": "GWW", "name": "W.W. Grainger"},
                    {"ticker": "RSG", "name": "Rollins"},
                ],
            },
        },
    },
    # -------------------------------------------------------------------------
    # 8. MATERIALS (XLB)
    # -------------------------------------------------------------------------
    "Materials": {
        "etf": "XLB",
        "industries": {
            "Chemicals": {
                "etf": None,
                "companies": [
                    {"ticker": "LIN", "name": "Linde", "market_cap": 220_000_000_000, "weight": 0.20},
                    {"ticker": "APD", "name": "Air Products", "market_cap": 60_000_000_000, "weight": 0.06},
                    {"ticker": "SHW", "name": "Sherwin-Williams", "market_cap": 85_000_000_000, "weight": 0.08},
                    {"ticker": "DD", "name": "DuPont de Nemours"},
                    {"ticker": "DOW", "name": "Dow"},
                    {"ticker": "ECL", "name": "Ecolab"},
                    {"ticker": "PPG", "name": "PPG Industries"},
                    {"ticker": "CE", "name": "Celanese"},
                    {"ticker": "EMN", "name": "Eastman Chemical"},
                    {"ticker": "ALB", "name": "Albemarle"},
                    {"ticker": "CTVA", "name": "Corteva"},
                    {"ticker": "FMC", "name": "FMC Corporation"},
                    {"ticker": "IFF", "name": "International Flavors & Fragrances"},
                    {"ticker": "RPM", "name": "RPM International"},
                    {"ticker": "CF", "name": "CF Industries"},
                    {"ticker": "MOS", "name": "Mosaic"},
                    {"ticker": "NTR", "name": "Nutrien"},
                    {"ticker": "AXTA", "name": "Axalta Coating Systems"},
                ],
            },
            "Metals & Mining": {
                "etf": "XME",
                "companies": [
                    {"ticker": "FCX", "name": "Freeport-McMoRan", "market_cap": 60_000_000_000, "weight": 0.15},
                    {"ticker": "NEM", "name": "Newmont", "market_cap": 50_000_000_000, "weight": 0.10},
                    {"ticker": "NUE", "name": "Nucor", "market_cap": 40_000_000_000, "weight": 0.08},
                    {"ticker": "STLD", "name": "Steel Dynamics"},
                    {"ticker": "RS", "name": "Reliance Steel"},
                    {"ticker": "CLF", "name": "Cleveland-Cliffs"},
                    {"ticker": "X", "name": "United States Steel"},
                    {"ticker": "AA", "name": "Alcoa"},
                    {"ticker": "GOLD", "name": "Barrick Gold"},
                    {"ticker": "AEM", "name": "Agnico Eagle Mines"},
                    {"ticker": "WPM", "name": "Wheaton Precious Metals"},
                    {"ticker": "FNV", "name": "Franco-Nevada"},
                    {"ticker": "RGLD", "name": "Royal Gold"},
                    {"ticker": "MP", "name": "MP Materials"},
                    {"ticker": "TECK", "name": "Teck Resources"},
                    {"ticker": "RIO", "name": "Rio Tinto"},
                    {"ticker": "BHP", "name": "BHP Group"},
                    {"ticker": "VALE", "name": "Vale"},
                ],
            },
            "Packaging & Containers": {
                "etf": None,
                "companies": [
                    {"ticker": "BALL", "name": "Ball Corporation"},
                    {"ticker": "PKG", "name": "Packaging Corporation of America"},
                    {"ticker": "IP", "name": "International Paper"},
                    {"ticker": "WRK", "name": "WestRock"},
                    {"ticker": "SEE", "name": "Sealed Air"},
                    {"ticker": "BLL", "name": "Ball"},
                    {"ticker": "ATR", "name": "AptarGroup"},
                    {"ticker": "AVY", "name": "Avery Dennison"},
                    {"ticker": "SON", "name": "Sonoco Products"},
                    {"ticker": "GPK", "name": "Graphic Packaging"},
                    {"ticker": "BERY", "name": "Berry Global"},
                ],
            },
        },
    },
    # -------------------------------------------------------------------------
    # 9. UTILITIES (XLU)
    # -------------------------------------------------------------------------
    "Utilities": {
        "etf": "XLU",
        "industries": {
            "Electric Utilities": {
                "etf": None,
                "companies": [
                    {"ticker": "NEE", "name": "NextEra Energy", "market_cap": 150_000_000_000, "weight": 0.20},
                    {"ticker": "SO", "name": "Southern Company", "market_cap": 85_000_000_000, "weight": 0.10},
                    {"ticker": "DUK", "name": "Duke Energy", "market_cap": 80_000_000_000, "weight": 0.10},
                    {"ticker": "D", "name": "Dominion Energy"},
                    {"ticker": "AEP", "name": "American Electric Power"},
                    {"ticker": "EXC", "name": "Exelon"},
                    {"ticker": "SRE", "name": "Sempra"},
                    {"ticker": "XEL", "name": "Xcel Energy"},
                    {"ticker": "ED", "name": "Consolidated Edison"},
                    {"ticker": "PEG", "name": "Public Service Enterprise Group"},
                    {"ticker": "WEC", "name": "WEC Energy Group"},
                    {"ticker": "ES", "name": "Eversource Energy"},
                    {"ticker": "PPL", "name": "PPL Corporation"},
                    {"ticker": "FE", "name": "FirstEnergy"},
                    {"ticker": "ETR", "name": "Entergy"},
                    {"ticker": "AEE", "name": "Ameren"},
                    {"ticker": "CMS", "name": "CMS Energy"},
                    {"ticker": "DTE", "name": "DTE Energy"},
                    {"ticker": "LNT", "name": "Alliant Energy"},
                    {"ticker": "EVRG", "name": "Evergy"},
                    {"ticker": "PNW", "name": "Pinnacle West Capital"},
                    {"ticker": "ATO", "name": "Atmos Energy"},
                    {"ticker": "NI", "name": "NiSource"},
                    {"ticker": "CEG", "name": "Constellation Energy"},
                    {"ticker": "VST", "name": "Vistra"},
                    {"ticker": "NRG", "name": "NRG Energy"},
                    {"ticker": "PCOR", "name": "Procore Technologies"},
                ],
            },
            "Gas Utilities": {
                "etf": None,
                "companies": [
                    {"ticker": "ATO", "name": "Atmos Energy"},
                    {"ticker": "NI", "name": "NiSource"},
                    {"ticker": "OGS", "name": "ONE Gas"},
                    {"ticker": "SR", "name": "Spire"},
                    {"ticker": "SWX", "name": "Southwest Gas"},
                    {"ticker": "NFG", "name": "National Fuel Gas"},
                    {"ticker": "NJR", "name": "New Jersey Resources"},
                    {"ticker": "UGI", "name": "UGI Corporation"},
                    {"ticker": "MDU", "name": "MDU Resources"},
                    {"ticker": "RGCO", "name": "RGC Resources"},
                ],
            },
            "Water Utilities": {
                "etf": None,
                "companies": [
                    {"ticker": "AWK", "name": "American Water Works", "market_cap": 27_000_000_000, "weight": 0.30},
                    {"ticker": "WTR", "name": "Essential Utilities"},
                    {"ticker": "WTRG", "name": "Essential Utilities"},
                    {"ticker": "SJW", "name": "SJW Group"},
                    {"ticker": "YORW", "name": "York Water"},
                    {"ticker": "CWT", "name": "California Water Service"},
                    {"ticker": "AWR", "name": "American States Water"},
                    {"ticker": "MSEX", "name": "Middlesex Water"},
                    {"ticker": "ARTNA", "name": "Artesian Resources"},
                    {"ticker": "ARIS", "name": "Aris Water Solutions"},
                ],
            },
            "Independent Power / Renewables": {
                "etf": None,
                "companies": [
                    {"ticker": "CEG", "name": "Constellation Energy", "market_cap": 70_000_000_000, "weight": 0.15},
                    {"ticker": "VST", "name": "Vistra", "market_cap": 40_000_000_000, "weight": 0.08},
                    {"ticker": "NRG", "name": "NRG Energy"},
                    {"ticker": "AES", "name": "AES Corporation"},
                    {"ticker": "BEP", "name": "Brookfield Renewable Partners"},
                    {"ticker": "CWEN", "name": "Clearway Energy"},
                    {"ticker": "OGE", "name": "OGE Energy"},
                    {"ticker": "PNM", "name": "PNM Resources"},
                    {"ticker": "TAL", "name": "Talen Energy"},
                    {"ticker": "ENPH", "name": "Enphase Energy"},
                ],
            },
        },
    },
    # -------------------------------------------------------------------------
    # 10. REAL ESTATE (XLRE)
    # -------------------------------------------------------------------------
    "Real Estate": {
        "etf": "XLRE",
        "industries": {
            "REITs - Data Centers & Towers": {
                "etf": None,
                "companies": [
                    {"ticker": "AMT", "name": "American Tower", "market_cap": 95_000_000_000, "weight": 0.15},
                    {"ticker": "CCI", "name": "Crown Castle", "market_cap": 45_000_000_000, "weight": 0.07},
                    {"ticker": "EQIX", "name": "Equinix", "market_cap": 80_000_000_000, "weight": 0.12},
                    {"ticker": "DLR", "name": "Digital Realty", "market_cap": 50_000_000_000, "weight": 0.08},
                    {"ticker": "SBAC", "name": "SBA Communications"},
                    {"ticker": "QTS", "name": "QTS Realty"},
                    {"ticker": "CONE", "name": "CyrusOne"},
                    {"ticker": "UNIT", "name": "Uniti Group"},
                    {"ticker": "IIPR", "name": "Innovative Industrial Properties"},
                    {"ticker": "USDC", "name": "US Data Center REIT"},
                ],
            },
            "REITs - Industrial & Logistics": {
                "etf": None,
                "companies": [
                    {"ticker": "PLD", "name": "Prologis", "market_cap": 110_000_000_000, "weight": 0.20},
                    {"ticker": "STAG", "name": "STAG Industrial"},
                    {"ticker": "FR", "name": "First Industrial Realty"},
                    {"ticker": "REXR", "name": "Rexford Industrial Realty"},
                    {"ticker": "TRNO", "name": "Terreno Realty"},
                    {"ticker": "EGP", "name": "EastGroup Properties"},
                    {"ticker": "LPT", "name": "Liberty Property Trust"},
                    {"ticker": "GTY", "name": "Getty Realty"},
                    {"ticker": "ILPT", "name": "Industrial Logistics Properties Trust"},
                    {"ticker": "COLD", "name": "Americold Realty"},
                ],
            },
            "REITs - Residential": {
                "etf": None,
                "companies": [
                    {"ticker": "INVH", "name": "Invitation Homes"},
                    {"ticker": "EQR", "name": "Equity Residential"},
                    {"ticker": "AVB", "name": "AvalonBay Communities"},
                    {"ticker": "MAA", "name": "Mid-America Apartment"},
                    {"ticker": "UDR", "name": "UDR"},
                    {"ticker": "CPT", "name": "Camden Property Trust"},
                    {"ticker": "ESS", "name": "Essex Property Trust"},
                    {"ticker": "NXRT", "name": "NexPoint Residential"},
                    {"ticker": "AIV", "name": "Apartment Investment & Management"},
                    {"ticker": "SUI", "name": "Sun Communities"},
                    {"ticker": "ELS", "name": "Equity LifeStyle Properties"},
                ],
            },
            "REITs - Retail": {
                "etf": None,
                "companies": [
                    {"ticker": "SPG", "name": "Simon Property Group", "market_cap": 55_000_000_000, "weight": 0.12},
                    {"ticker": "O", "name": "Realty Income", "market_cap": 50_000_000_000, "weight": 0.10},
                    {"ticker": "NNN", "name": "NNN REIT"},
                    {"ticker": "REG", "name": "Regency Centers"},
                    {"ticker": "KIM", "name": "Kimco Realty"},
                    {"ticker": "FRT", "name": "Federal Realty"},
                    {"ticker": "BRX", "name": "Brixmor Property Group"},
                    {"ticker": "MAC", "name": "Macerich"},
                    {"ticker": "SKT", "name": "Tanger Factory Outlet"},
                    {"ticker": "WPC", "name": "W. P. Carey"},
                    {"ticker": "STOR", "name": "STORE Capital"},
                ],
            },
            "REITs - Office & Specialty": {
                "etf": None,
                "companies": [
                    {"ticker": "WELL", "name": "Welltower", "market_cap": 55_000_000_000, "weight": 0.08},
                    {"ticker": "VTR", "name": "Ventas"},
                    {"ticker": "ARE", "name": "Alexandria Real Estate"},
                    {"ticker": "BXP", "name": "BXP (Boston Properties)"},
                    {"ticker": "SLG", "name": "SL Green Realty"},
                    {"ticker": "VNO", "name": "Vornado Realty Trust"},
                    {"ticker": "HIW", "name": "Highwoods Properties"},
                    {"ticker": "KRC", "name": "Kilroy Realty"},
                    {"ticker": "PSA", "name": "Public Storage"},
                    {"ticker": "EXR", "name": "Extra Space Storage"},
                    {"ticker": "CUBE", "name": "CubeSmart"},
                    {"ticker": "LSI", "name": "Life Storage"},
                    {"ticker": "NSA", "name": "National Storage Affiliates"},
                ],
            },
        },
    },
    # -------------------------------------------------------------------------
    # 11. COMMUNICATION SERVICES (XLC)
    # -------------------------------------------------------------------------
    "Communication Services": {
        "etf": "XLC",
        "industries": {
            "Interactive Media & Services": {
                "etf": None,
                "companies": [
                    {"ticker": "GOOGL", "name": "Alphabet", "market_cap": 2_100_000_000_000, "weight": 0.35},
                    {"ticker": "META", "name": "Meta Platforms", "market_cap": 1_500_000_000_000, "weight": 0.25},
                    {"ticker": "SNAP", "name": "Snap"},
                    {"ticker": "PINS", "name": "Pinterest"},
                    {"ticker": "RDDT", "name": "Reddit"},
                    {"ticker": "MTCH", "name": "Match Group"},
                    {"ticker": "BMBL", "name": "Bumble"},
                    {"ticker": "ZI", "name": "ZoomInfo Technologies"},
                    {"ticker": "YELP", "name": "Yelp"},
                    {"ticker": "IAC", "name": "IAC"},
                ],
            },
            "Entertainment & Streaming": {
                "etf": None,
                "companies": [
                    {"ticker": "NFLX", "name": "Netflix", "market_cap": 300_000_000_000, "weight": 0.20},
                    {"ticker": "DIS", "name": "Walt Disney", "market_cap": 200_000_000_000, "weight": 0.15},
                    {"ticker": "SPOT", "name": "Spotify"},
                    {"ticker": "WBD", "name": "Warner Bros. Discovery"},
                    {"ticker": "PARA", "name": "Paramount Global"},
                    {"ticker": "LYV", "name": "Live Nation Entertainment"},
                    {"ticker": "ROKU", "name": "Roku"},
                    {"ticker": "RBLX", "name": "Roblox"},
                    {"ticker": "EA", "name": "Electronic Arts"},
                    {"ticker": "TTWO", "name": "Take-Two Interactive"},
                    {"ticker": "ATVI", "name": "Activision Blizzard"},
                    {"ticker": "U", "name": "Unity Software"},
                ],
            },
            "Telecom": {
                "etf": None,
                "companies": [
                    {"ticker": "T", "name": "AT&T", "market_cap": 140_000_000_000, "weight": 0.12},
                    {"ticker": "VZ", "name": "Verizon", "market_cap": 170_000_000_000, "weight": 0.14},
                    {"ticker": "TMUS", "name": "T-Mobile US", "market_cap": 230_000_000_000, "weight": 0.18},
                    {"ticker": "CMCSA", "name": "Comcast", "market_cap": 150_000_000_000, "weight": 0.12},
                    {"ticker": "CHTR", "name": "Charter Communications"},
                    {"ticker": "LBRDA", "name": "Liberty Broadband"},
                    {"ticker": "FYBR", "name": "Frontier Communications"},
                    {"ticker": "USM", "name": "US Cellular"},
                    {"ticker": "LUMN", "name": "Lumen Technologies"},
                    {"ticker": "ATUS", "name": "Altice USA"},
                    {"ticker": "CABO", "name": "Cable One"},
                ],
            },
            "Advertising / Marketing": {
                "etf": None,
                "companies": [
                    {"ticker": "OMC", "name": "Omnicom Group"},
                    {"ticker": "IPG", "name": "Interpublic Group"},
                    {"ticker": "WPP", "name": "WPP"},
                    {"ticker": "MGNI", "name": "Magnite"},
                    {"ticker": "TTD", "name": "The Trade Desk"},
                    {"ticker": "DV", "name": "DoubleVerify"},
                    {"ticker": "IAS", "name": "Integral Ad Science"},
                    {"ticker": "APPS", "name": "Digital Turbine"},
                    {"ticker": "CRTO", "name": "Criteo"},
                    {"ticker": "PUBM", "name": "PubMatic"},
                ],
            },
        },
    },
}


# =============================================================================
# Accessor functions
# =============================================================================

def get_universe() -> dict:
    """Return the full market universe map."""
    return MARKET_UNIVERSE


def get_sector(name: str) -> dict | None:
    """Return one sector with all its industries and companies.

    Case-insensitive partial matching: 'tech' matches 'Technology'.
    """
    name_lower = name.lower()
    for sector_name, sector_data in MARKET_UNIVERSE.items():
        if name_lower == sector_name.lower() or name_lower in sector_name.lower():
            return {"name": sector_name, **sector_data}
    return None


def get_industry(name: str) -> dict | None:
    """Return one industry dict (with its companies) by name.

    Searches across all sectors. Case-insensitive partial match.
    """
    name_lower = name.lower()
    for sector_name, sector_data in MARKET_UNIVERSE.items():
        for ind_name, ind_data in sector_data.get("industries", {}).items():
            if name_lower == ind_name.lower() or name_lower in ind_name.lower():
                return {"name": ind_name, "sector": sector_name, **ind_data}
    return None


def get_peers(ticker: str) -> list[dict]:
    """Return all companies in the same industry as the given ticker.

    Excludes the ticker itself from results.
    """
    ticker_upper = ticker.upper()
    for _sector_name, sector_data in MARKET_UNIVERSE.items():
        for _ind_name, ind_data in sector_data.get("industries", {}).items():
            companies = ind_data.get("companies", [])
            tickers_in_industry = [c["ticker"] for c in companies]
            if ticker_upper in tickers_in_industry:
                return [c for c in companies if c["ticker"] != ticker_upper]
    return []


def search_company(query: str) -> list[dict]:
    """Fuzzy search across all companies by ticker or name.

    Returns list of matching companies with sector/industry context.
    """
    query_lower = query.lower()
    results = []
    for sector_name, sector_data in MARKET_UNIVERSE.items():
        for ind_name, ind_data in sector_data.get("industries", {}).items():
            for company in ind_data.get("companies", []):
                ticker_match = query_lower in company["ticker"].lower()
                name_match = query_lower in company["name"].lower()
                if ticker_match or name_match:
                    results.append({
                        **company,
                        "sector": sector_name,
                        "industry": ind_name,
                    })
    return results


def get_all_tickers() -> list[str]:
    """Return a flat, deduplicated, sorted list of all tracked tickers."""
    tickers: set[str] = set()
    for _sector_name, sector_data in MARKET_UNIVERSE.items():
        for _ind_name, ind_data in sector_data.get("industries", {}).items():
            for company in ind_data.get("companies", []):
                tickers.add(company["ticker"])
    return sorted(tickers)
