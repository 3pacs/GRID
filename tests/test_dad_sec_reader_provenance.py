"""Stored SEC admission controls using the actual SQLite reader and Dad route."""

import json

import pytest
from sqlalchemy import create_engine, event, text

from api.dad_sec_fundamentals import read_sec_profile


DURATION_FIELDS = ("revenue", "revenue_contracts", "net_income", "eps_basic", "eps_diluted")
INSTANT_FIELDS = ("total_assets", "stockholders_equity", "long_term_debt")


@pytest.fixture
def engine():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE source_catalog (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO source_catalog VALUES (1, 'SEC_EDGAR_Fundamentals')"))
        conn.execute(text("""CREATE TABLE raw_series (
            series_id TEXT, source_id INTEGER, obs_date TEXT, pull_timestamp TEXT,
            value REAL, raw_payload TEXT, pull_status TEXT)"""))
    yield engine
    engine.dispose()


def seed(engine, field="revenue", value=0, captured="2026-08-02", **metadata):
    payload = {
        "ticker": "AAPL", "cik": "0000320193", "field": field,
        "period_start": "2025-01-01", "period_end": "2025-12-31",
        "filed": "2026-08-01", "form": "10-K",
        "unit": "USD/shares" if field.startswith("eps_") else "USD",
        "accession": "0000320193-26-000011",
        "source_url": "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json",
        **metadata,
    }
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO raw_series VALUES (:sid, 1, :obs, :captured, :value, :payload, 'SUCCESS')"),
                     {"sid": f"sec_filed_fundamentals.AAPL.{field}", "obs": "2025-12-31",
                      "captured": captured, "value": value, "payload": json.dumps(payload)})


@pytest.mark.parametrize("field", DURATION_FIELDS)
@pytest.mark.parametrize("start", [None, "nonsense", "2026-01-01"])
def test_all_duration_concepts_require_valid_bounded_start(engine, field, start):
    seed(engine, field, period_start=start)
    result = read_sec_profile(engine, "AAPL")
    assert result["status"] == "unavailable"
    assert result["fields"] == {} and result["stats"] == []
    assert result["latest_filed_date"] is None and result["latest_pull"] is None


@pytest.mark.parametrize("field", DURATION_FIELDS)
def test_genuine_duration_and_zero_remain_reported(engine, field):
    seed(engine, field, period_start="2025-12-31")  # Inclusive same-day boundary.
    result = read_sec_profile(engine, "AAPL")
    assert result["status"] == "ready"
    item = result["fields"][field]
    assert item["period_start"] == item["period_end"] == "2025-12-31"
    assert item["numeric_value"] == 0 and item["raw_value"].startswith("0 ")


@pytest.mark.parametrize("field", INSTANT_FIELDS)
def test_genuine_instant_assets_equity_and_debt_need_no_start(engine, field):
    seed(engine, field, value=19, period_start=None)
    result = read_sec_profile(engine, "AAPL")
    assert result["status"] == "ready"
    assert result["fields"][field]["period_start"] is None
    assert result["fields"][field]["numeric_value"] == 19


@pytest.mark.parametrize("cik", [None, "0000789019", True, 320193.0, "0", "320193\n"])
def test_missing_wrong_or_malformed_payload_cik_is_unavailable(engine, cik):
    seed(engine, cik=cik)
    assert read_sec_profile(engine, "AAPL")["status"] == "unavailable"


@pytest.mark.parametrize("cik", ["0000320193", "320193", 320193])
def test_valid_numeric_cik_forms_must_match_exact_url_identity(engine, cik):
    seed(engine, cik=cik)
    assert read_sec_profile(engine, "AAPL")["fields"]["revenue"]["numeric_value"] == 0


@pytest.mark.parametrize("url", [
    "https://data.sec.gov/api/xbrl/companyfacts/CIK0000789019.json",
    "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json?alias=AAPL",
    "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json\n",
    "https://data.sec.gov.evil.invalid/api/xbrl/companyfacts/CIK0000320193.json",
    "https://data.sec.gov/api/xbrl/companyfacts/CIK００００３２０１９３.json",
])
def test_companyfacts_url_is_exact_ascii_sec_identity(engine, url):
    seed(engine, source_url=url)
    assert read_sec_profile(engine, "AAPL")["fields"] == {}


def test_actual_dad_route_rejects_newer_bad_facts_without_erasing_good_zero_or_instant(engine, monkeypatch):
    from api.routers import dad

    seed(engine)
    seed(engine, value=999, captured="2026-09-29", period_start=None)
    seed(engine, "eps_basic", value=999, captured="2026-09-29", cik="0000789019")
    seed(engine, "total_assets", value=19, period_start=None)
    statements = []
    event.listen(engine, "before_cursor_execute", lambda _a, _b, sql, *_rest: statements.append(sql))
    monkeypatch.setattr(dad, "_fetch_finviz_snapshot", lambda *_a: pytest.fail("provider invoked"))
    monkeypatch.setattr(dad, "_store_finviz_snapshot", lambda *_a: pytest.fail("writer invoked"))
    result = dad._get_finviz_profile(engine, "AAPL", refresh=True, persist_refresh=True)
    assert set(result["fields"]) == {"revenue", "total_assets"}
    assert result["fields"]["revenue"]["numeric_value"] == 0
    assert result["fields"]["total_assets"]["numeric_value"] == 19
    assert result["latest_pull"] == "2026-08-02"
    assert result["refresh_available"] is False
    assert all(sql.lstrip().upper().startswith("SELECT") for sql in statements)
