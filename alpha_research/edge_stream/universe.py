"""Frozen instrument lists the edge stream's policy names (hashed into the policy).

``SP500_20251231``: S&P 500 members on 2025-12-31 from ``config/sp500_membership.json``
(``sp500-membership/1``, base set + dated changes), fixed here so a later refresh of
that file can never change the policy silently. Known on the first day of the EC1
history window (2026-01-02), so choosing it uses nothing from the window. Left out:
share classes with a dot (BF.B, BRK.B; their Polygon and EDGAR symbols differ)
and the single names earlier multi-day searches already used
(AAPL, MSFT, NVDA, META, AMZN, GOOGL, GOOG, TSLA, XOM, CVX; high-Sharpe ensemble v1-v6 and the stream's seven
mega caps), and ES (Eversource: the policy already uses ES for the E-mini future).
``tests/test_edge_stream_events.py`` re-derives the list from the file.
"""

from __future__ import annotations

SP500_AS_OF = "2025-12-31"
SP500_SYMBOL_CLASH = ("ES",)
SP500_PRIOR_USE = ('AAPL', 'MSFT', 'NVDA', 'META', 'AMZN', 'GOOGL', 'GOOG', 'TSLA', 'XOM', 'CVX')

SP500_20251231: tuple[str, ...] = (
    "A", "ABBV", "ABNB", "ABT", "ACGL", "ACN", "ADBE", "ADI", "ADM", "ADP", "ADSK", "AEE", "AEP", "AES", "AFL",
    "AIG", "AIZ", "AJG", "AKAM", "ALB", "ALGN", "ALL", "ALLE", "AMAT", "AMCR", "AMD", "AME", "AMGN", "AMP",
    "AMT", "ANET", "AON", "AOS", "APA", "APD", "APH", "APO", "APP", "APTV", "ARE", "ARES", "ATO", "AVB", "AVGO",
    "AVY", "AWK", "AXON", "AXP", "AZO", "BA", "BAC", "BALL", "BAX", "BBY", "BDX", "BEN", "BG", "BIIB", "BK",
    "BKNG", "BKR", "BLDR", "BLK", "BMY", "BR", "BRO", "BSX", "BX", "BXP", "C", "CAG", "CAH", "CARR", "CAT",
    "CB", "CBOE", "CBRE", "CCI", "CCL", "CDNS", "CDW", "CEG", "CF", "CFG", "CHD", "CHRW", "CHTR", "CI", "CINF",
    "CL", "CLX", "CMCSA", "CME", "CMG", "CMI", "CMS", "CNC", "CNP", "COF", "COIN", "COO", "COP", "COR", "COST",
    "CPAY", "CPB", "CPRT", "CPT", "CRH", "CRL", "CRM", "CRWD", "CSCO", "CSGP", "CSX", "CTAS", "CTRA", "CTSH",
    "CTVA", "CVNA", "CVS", "D", "DAL", "DASH", "DAY", "DD", "DDOG", "DE", "DECK", "DELL", "DG", "DGX", "DHI",
    "DHR", "DIS", "DLR", "DLTR", "DOC", "DOV", "DOW", "DPZ", "DRI", "DTE", "DUK", "DVA", "DVN", "DXCM", "EA",
    "EBAY", "ECL", "ED", "EFX", "EG", "EIX", "EL", "ELV", "EME", "EMR", "EOG", "EPAM", "EQIX", "EQR", "EQT",
    "ERIE", "ESS", "ETN", "ETR", "EVRG", "EW", "EXC", "EXE", "EXPD", "EXPE", "EXR", "F", "FANG", "FAST",
    "FCX", "FDS", "FDX", "FE", "FFIV", "FICO", "FIS", "FISV", "FITB", "FIX", "FOX", "FOXA", "FRT", "FSLR",
    "FTNT", "FTV", "GD", "GDDY", "GE", "GEHC", "GEN", "GEV", "GILD", "GIS", "GL", "GLW", "GM", "GNRC", "GPC",
    "GPN", "GRMN", "GS", "GWW", "HAL", "HAS", "HBAN", "HCA", "HD", "HIG", "HII", "HLT", "HOLX", "HON", "HOOD",
    "HPE", "HPQ", "HRL", "HSIC", "HST", "HSY", "HUBB", "HUM", "HWM", "IBKR", "IBM", "ICE", "IDXX", "IEX", "IFF",
    "INCY", "INTC", "INTU", "INVH", "IP", "IQV", "IR", "IRM", "ISRG", "IT", "ITW", "IVZ", "J", "JBHT", "JBL",
    "JCI", "JKHY", "JNJ", "JPM", "KDP", "KEY", "KEYS", "KHC", "KIM", "KKR", "KLAC", "KMB", "KMI", "KO", "KR",
    "KVUE", "L", "LDOS", "LEN", "LH", "LHX", "LII", "LIN", "LLY", "LMT", "LNT", "LOW", "LRCX", "LULU", "LUV",
    "LVS", "LW", "LYB", "LYV", "MA", "MAA", "MAR", "MAS", "MCD", "MCHP", "MCK", "MCO", "MDLZ", "MDT", "MET",
    "MGM", "MKC", "MLM", "MMC", "MMM", "MNST", "MO", "MOH", "MOS", "MPC", "MPWR", "MRK", "MRNA", "MS", "MSCI",
    "MSI", "MTB", "MTCH", "MTD", "MU", "NCLH", "NDAQ", "NDSN", "NEE", "NEM", "NFLX", "NI", "NKE", "NOC", "NOW",
    "NRG", "NSC", "NTAP", "NTRS", "NUE", "NVR", "NWS", "NWSA", "NXPI", "O", "ODFL", "OKE", "OMC", "ON", "ORCL",
    "ORLY", "OTIS", "OXY", "PANW", "PAYC", "PAYX", "PCAR", "PCG", "PEG", "PEP", "PFE", "PFG", "PG", "PGR", "PH",
    "PHM", "PKG", "PLD", "PLTR", "PM", "PNC", "PNR", "PNW", "PODD", "POOL", "PPG", "PPL", "PRU", "PSA", "PSKY",
    "PSX", "PTC", "PWR", "PYPL", "Q", "QCOM", "RCL", "REG", "REGN", "RF", "RJF", "RL", "RMD", "ROK", "ROL",
    "ROP", "ROST", "RSG", "RTX", "RVTY", "SBAC", "SBUX", "SCHW", "SHW", "SJM", "SLB", "SMCI", "SNA", "SNDK",
    "SNPS", "SO", "SOLV", "SPG", "SPGI", "SRE", "STE", "STLD", "STT", "STX", "STZ", "SW", "SWK", "SWKS", "SYF",
    "SYK", "SYY", "T", "TAP", "TDG", "TDY", "TECH", "TEL", "TER", "TFC", "TGT", "TJX", "TKO", "TMO", "TMUS",
    "TPL", "TPR", "TRGP", "TRMB", "TROW", "TRV", "TSCO", "TSN", "TT", "TTD", "TTWO", "TXN", "TXT", "TYL", "UAL",
    "UBER", "UDR", "UHS", "ULTA", "UNH", "UNP", "UPS", "URI", "USB", "V", "VICI", "VLO", "VLTO", "VMC", "VRSK",
    "VRSN", "VRTX", "VST", "VTR", "VTRS", "VZ", "WAB", "WAT", "WBD", "WDAY", "WDC", "WEC", "WELL", "WFC", "WM",
    "WMB", "WMT", "WRB", "WSM", "WST", "WTW", "WY", "WYNN", "XEL", "XYL", "XYZ", "YUM", "ZBH", "ZBRA", "ZTS",
)
