import requests, json, os
from datetime import datetime, timedelta

def fetch_json(url, params=None, headers=None, timeout=30):
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"_error": str(e)}

def fetch_ofr_fsi(db):
    api_key = os.environ.get("FRED_API_KEY", "")
    if not api_key: return 0
    data = fetch_json("https://api.stlouisfed.org/fred/series/observations",
        {"series_id": "STLFSI2", "api_key": api_key, "file_type": "json", "sort_order": "desc", "limit": "10"})
    if "_error" not in data and data.get("observations"):
        db.execute("INSERT INTO raw_ingest VALUES (?,?,?,?,?)",
            ["ofr_fsi", 1, datetime.utcnow(), datetime.utcnow().date(), json.dumps(data)])
        return len(data["observations"])
    return 0

def fetch_ny_fed_repo(db):
    week_ago = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    today = datetime.utcnow().strftime("%Y-%m-%d")
    data = fetch_json(f"https://markets.newyorkfed.org/api/rp/results/search.json?startDate={week_ago}&endDate={today}")
    ops = data.get("repo", {}).get("operations", []) if "_error" not in data else []
    if ops:
        db.execute("INSERT INTO raw_ingest VALUES (?,?,?,?,?)",
            ["ny_fed_repo", 1, datetime.utcnow(), datetime.utcnow().date(), json.dumps(data)])
        return len(ops)
    return 0

def fetch_treasury_tga(db):
    data = fetch_json("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/deposits_withdrawals_operating_cash",
        {"sort": "-record_date", "page[size]": "10", "filter": "account_type:eq:Federal Reserve Account"})
    if "_error" not in data and data.get("data"):
        db.execute("INSERT INTO raw_ingest VALUES (?,?,?,?,?)",
            ["treasury_tga", 2, datetime.utcnow(), datetime.utcnow().date(), json.dumps(data)])
        return len(data["data"])
    return 0

def fetch_ny_fed_soma(db):
    data = fetch_json("https://markets.newyorkfed.org/api/soma/summary.json")
    if "_error" not in data and data.get("soma"):
        db.execute("INSERT INTO raw_ingest VALUES (?,?,?,?,?)",
            ["ny_fed_soma", 1, datetime.utcnow(), datetime.utcnow().date(), json.dumps(data)])
        return 1
    return 0

def fetch_ny_fed_sofr(db):
    data = fetch_json("https://markets.newyorkfed.org/api/rates/secured/sofr/last/5.json")
    if "_error" not in data:
        db.execute("INSERT INTO raw_ingest VALUES (?,?,?,?,?)",
            ["ny_fed_sofr", 1, datetime.utcnow(), datetime.utcnow().date(), json.dumps(data)])
        return 1
    return 0

def fetch_kalshi(db):
    data = fetch_json("https://api.elections.kalshi.com/trade-api/v2/markets",
        {"limit": "20", "status": "open"})
    if "_error" not in data and data.get("markets"):
        db.execute("INSERT INTO raw_ingest VALUES (?,?,?,?,?)",
            ["kalshi", 3, datetime.utcnow(), datetime.utcnow().date(), json.dumps(data)])
        return len(data["markets"])
    return 0

def fetch_cftc_cot(db):
    week_ago = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%d")
    data = fetch_json("https://publicreporting.cftc.gov/resource/jun7-fc8e.json",
        {"$limit": "50", "$where": f"report_date_as_yyyy_mm_dd > '{week_ago}'"})
    if "_error" not in data and isinstance(data, list) and len(data) > 0:
        db.execute("INSERT INTO raw_ingest VALUES (?,?,?,?,?)",
            ["cftc_cot", 3, datetime.utcnow(), datetime.utcnow().date(), json.dumps(data)])
        return len(data)
    return 0

def fetch_cfpb(db):
    week_ago = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    data = fetch_json("https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/",
        {"date_received_min": week_ago, "size": "25", "sort": "created_date_desc"})
    hits = data.get("hits", {}).get("hits", []) if "_error" not in data else []
    if hits:
        db.execute("INSERT INTO raw_ingest VALUES (?,?,?,?,?)",
            ["cfpb", 4, datetime.utcnow(), datetime.utcnow().date(), json.dumps(data)])
        return len(hits)
    return 0

FRED_FULL = [
    "DFF", "T10Y2Y", "BAMLH0A0HYM2", "DTWEXBGS", "VIXCLS",
    "DGS10", "DGS2", "DGS30", "T10YIE", "DFEDTARU",
    "SOFR", "MORTGAGE30US", "CPIAUCSL", "UNRATE", "PAYEMS",
    "RSAFS", "INDPRO", "M2SL", "WALCL", "RRPONTSYD",
]

def fetch_all_fred(db):
    api_key = os.environ.get("FRED_API_KEY", "")
    if not api_key: return 0
    ok = 0
    for s in FRED_FULL:
        try:
            data = fetch_json("https://api.stlouisfed.org/fred/series/observations",
                {"series_id": s, "api_key": api_key, "file_type": "json", "sort_order": "desc", "limit": "10"})
            if "_error" not in data and data.get("observations"):
                db.execute("INSERT INTO raw_ingest VALUES (?,?,?,?,?)",
                    [f"fred_{s}", 2, datetime.utcnow(), datetime.utcnow().date(), json.dumps(data)])
                ok += 1
        except Exception:
            pass
    return ok

ALL_FETCHERS = [
    ("ofr_fsi", fetch_ofr_fsi),
    ("ny_fed_repo", fetch_ny_fed_repo),
    ("ny_fed_sofr", fetch_ny_fed_sofr),
    ("ny_fed_soma", fetch_ny_fed_soma),
    ("treasury_tga", fetch_treasury_tga),
    ("kalshi", fetch_kalshi),
    ("cftc_cot", fetch_cftc_cot),
    ("cfpb", fetch_cfpb),
]
