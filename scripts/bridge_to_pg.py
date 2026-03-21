import duckdb, psycopg2, json
from datetime import datetime

DUCK = "/data/grid/duckdb/grid.duckdb"
PG = dict(dbname="griddb", user="grid", password="grid2026")

SOURCE_MAP = {
    "fred_DFF": ("FRED", "fed_funds_rate"),
    "fred_T10Y2Y": ("FRED", "yield_curve_10y2y"),
    "fred_BAMLH0A0HYM2": ("FRED", "hy_spread"),
    "fred_DTWEXBGS": ("FRED", "dollar_index"),
    "fred_VIXCLS": ("FRED", "vix"),
    "fred_DGS10": ("FRED", "treasury_10y"),
    "fred_DGS2": ("FRED", "treasury_2y"),
    "fred_DGS30": ("FRED", "treasury_30y"),
    "fred_T10YIE": ("FRED", "breakeven_10y"),
    "fred_SOFR": ("FRED", "sofr"),
    "fred_MORTGAGE30US": ("FRED", "mortgage_30y"),
    "fred_CPIAUCSL": ("FRED", "cpi"),
    "fred_UNRATE": ("FRED", "unemployment"),
    "fred_PAYEMS": ("FRED", "nonfarm_payrolls"),
    "fred_RSAFS": ("FRED", "retail_sales"),
    "fred_INDPRO": ("FRED", "industrial_production"),
    "fred_M2SL": ("FRED", "m2_money_supply"),
    "fred_WALCL": ("FRED", "fed_balance_sheet"),
    "fred_RRPONTSYD": ("FRED", "reverse_repo"),
    "fred_DFEDTARU": ("FRED", "fed_funds_rate"),
}

def run():
    dk = duckdb.connect(DUCK, read_only=True)
    pg = psycopg2.connect(**PG)
    pg.autocommit = True
    cur = pg.cursor()

    # Get source IDs
    cur.execute("SELECT id, name FROM source_catalog")
    src_ids = {r[1]: r[0] for r in cur.fetchall()}

    # Get feature IDs
    cur.execute("SELECT id, name FROM feature_registry")
    feat_ids = {r[1]: r[0] for r in cur.fetchall()}

    inserted = 0
    skipped = 0

    rows = dk.execute("SELECT source, payload, fetched_at FROM raw_ingest ORDER BY fetched_at").fetchall()

    for source, payload_str, fetched_at in rows:
        if source not in SOURCE_MAP:
            continue

        src_name, feat_name = SOURCE_MAP[source]
        src_id = src_ids.get(src_name)
        feat_id = feat_ids.get(feat_name)
        if not src_id or not feat_id:
            continue

        try:
            data = json.loads(payload_str) if isinstance(payload_str, str) else payload_str
            obs = data.get("observations", [])
            for o in obs:
                val = o.get("value", ".")
                if val == ".":
                    continue
                obs_date = o.get("date", "")
                if not obs_date:
                    continue
                try:
                    cur.execute(
                        "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status, pull_timestamp) "
                        "VALUES (%s, %s, %s, %s, 'SUCCESS', %s) "
                        "ON CONFLICT DO NOTHING",
                        (feat_name, src_id, obs_date, float(val), fetched_at)
                    )
                    inserted += 1
                except Exception:
                    skipped += 1
        except Exception as e:
            skipped += 1

    dk.close()
    pg.close()
    print(f"Bridge complete: {inserted} inserted, {skipped} skipped")

if __name__ == "__main__":
    run()
