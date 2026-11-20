"""Small-cap universe: permanent ids as keys, share-kind filter, resumable fetch, the factory's floor rule by hand."""
import csv
import json

import pandas as pd
import pytest

from analysis import cross_section_factory as X
from analysis import smallcap_universe as U

HEADER = "date,close,high,low,open,volume,adjClose,adjHigh,adjLow,adjOpen,adjVolume,divCash,splitFactor"
A, B, C, D, E = (f"US{n:012d}" for n in (1, 2, 3, 4, 5))


def meta(tmp_path, rows=None):
    rows = rows or [
        {"permaTicker": A, "ticker": "para", "name": "Old Media Co - Class B", "isActive": False, "isADR": False},
        {"permaTicker": B, "ticker": "para", "name": "New Tech Inc - Class A", "isActive": True, "isADR": False},
        {"permaTicker": C, "ticker": "xyz-u", "name": "XYZ Corp - Units (1 Ord Class A & 1/2 War)", "isActive": True, "isADR": False},
        {"permaTicker": D, "ticker": "fgn", "name": "Foreign Holdings", "isActive": True, "isADR": True},
        {"permaTicker": E, "ticker": "shel", "name": "Shell Acquisition Corp", "isActive": True, "isADR": False},
    ]
    p = tmp_path / "meta.json"
    p.write_text(json.dumps(rows))
    return p


def bars(days, close, volume=100_000):
    lines = [HEADER]
    for d, c in zip(days, close if isinstance(close, list) else [close] * len(days), strict=True):
        lines.append(f"{d}T00:00:00.000Z,{c},{c},{c},{c},{volume},{c},{c},{c},{c},{volume},0.0,1.0")
    return ("\n".join(lines) + "\n").encode()


def days(n, start="2020-01-02"):
    return [d.strftime("%Y-%m-%d") for d in pd.bdate_range(start, periods=n)]


@pytest.mark.parametrize("name,ticker,kind", [
    ("Acme Corp", "acme", "common"),
    ("Acme Corp - Class A", "acme", "common"),
    ("Berkshire - Class B", "brk-b", "common"),
    ("XYZ Corp - Units (1 Ord Class A & 1/2 War)", "xyz", "unit"),
    ("Anything", "xyz-u", "unit"),
    ("Anything", "xyz-ws", "warrant"),
    ("XYZ Corp - Warrants (01/01/2030)", "xyzw", "warrant"),
    ("XYZ Corp - Rights", "xyzr", "right"),
    ("Bank - Depositary Shares Pfd", "bkp", "preferred"),
    ("Shell Acquisition Corp", "shel", "spac"),
    ("Some Income Fund", "sif", "fund"),
    (None, "noname", "common"),
    ("United Rights Group", "urg", "common"),
])
def test_classify(name, ticker, kind):
    assert U.classify(name, ticker) == kind


def test_load_companies_keys_on_the_permanent_id_not_the_ticker(tmp_path):
    cs = U.load_companies(meta(tmp_path))
    assert [c.perma for c in cs] == [A, B, C, D, E]
    assert [c.ticker for c in cs[:2]] == ["PARA", "PARA"]          # one ticker, two companies, two ids
    assert [c.included for c in cs] == [True, True, False, False, False]   # unit, receipt and shell are left out


def test_load_companies_refuses_a_repeated_or_malformed_id(tmp_path):
    row = {"permaTicker": A, "ticker": "a", "name": "A", "isActive": True, "isADR": False}
    assert [c.included for c in U.load_companies(meta(tmp_path, [row, {**row, "permaTicker": "AU000000099308"}]))] == [False, True]
    with pytest.raises(U.UniverseError, match="listed twice"):
        U.load_companies(meta(tmp_path, [row, row]))
    with pytest.raises(U.UniverseError, match="malformed"):
        U.load_companies(meta(tmp_path, [{**row, "permaTicker": "PARA"}]))


def test_fetch_asks_by_id_writes_whole_files_and_resumes(tmp_path):
    cs = U.load_companies(meta(tmp_path))
    asked = []
    answers = {A: (200, bars(days(3), 10.0)), B: (500, b"")}

    def get(url):
        asked.append(url)
        return answers[next(p for p in answers if p in url)]

    out = tmp_path / "store"
    tally = U.fetch_prices(cs, out, get=get, sleep=lambda s: None)
    assert tally == {"prices": 1, "failed": 1, "not_included": 3}
    assert all("/tiingo/daily/US" in u and "PARA" not in u.upper().replace("PRICES", "") for u in asked)
    assert (out / "prices" / f"{A}.csv").read_bytes() == answers[A][1]
    assert not list((out / "prices").glob("*.part"))
    assert not (out / "prices" / f"{B}.csv").exists()               # a failure leaves nothing, so it is asked again

    asked.clear()
    answers[B] = (200, b'{"detail": "over your hourly allocation"}')   # a 200 that is not the empty answer
    assert U.fetch_prices(cs, out, get=get, sleep=lambda s: None) == {"already_done": 1, "failed": 1, "not_included": 3}
    assert not (out / "no-prices" / B).exists()                     # never mistaken for "this company has no prices"

    asked.clear()
    answers[B] = (200, b"[]")                                       # Tiingo's answer for an id with no bars
    tally = U.fetch_prices(cs, out, get=get, sleep=lambda s: None)
    assert tally == {"already_done": 1, "no_prices": 1, "not_included": 3}
    assert len(asked) == 1 and B in asked[0]
    assert (out / "no-prices" / B).exists()

    asked.clear()
    assert U.fetch_prices(cs, out, get=get, sleep=lambda s: None) == {"already_done": 2, "not_included": 3}
    assert asked == []
    log = [json.loads(line) for line in (out / "fetch-log.jsonl").read_text().splitlines()]
    assert [(r["perma"], r["outcome"]) for r in log] == [(A, "prices"), (B, "failed"), (B, "failed"), (B, "no_prices")]


def test_fetch_takes_the_header_alone_as_no_prices_but_not_a_cut_header(tmp_path):
    cs = [c for c in U.load_companies(meta(tmp_path)) if c.included]
    answers = {A: (200, HEADER.encode() + b"\n"), B: (200, HEADER.encode()[:-5] + b"\n")}
    out = tmp_path / "store"
    tally = U.fetch_prices(cs, out, get=lambda url: answers[next(p for p in answers if p in url)], sleep=lambda s: None)
    assert tally == {"no_prices": 1, "failed": 1}
    assert (out / "no-prices" / A).exists() and not (out / "no-prices" / B).exists()
    assert not list((out / "prices").iterdir())
    assert HEADER.encode() == U.PRICE_HEADER


def test_fetch_needs_a_key_and_sends_it_in_a_header(monkeypatch):
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    monkeypatch.delenv("TIINGO_KEY", raising=False)
    with pytest.raises(U.UniverseError, match="TIINGO_API_KEY"):
        U.tiingo_get("https://example.invalid/x")
    assert "token" not in U.PRICES_URL.lower()


def test_qualifies_is_the_factory_rule_with_the_floors_as_arguments():
    d = days(40)
    close = [3.0] * 25 + [1.5] * 15          # falls under a $2 floor from the 26th session
    df = pd.read_csv(pd.io.common.BytesIO(bars(d, close, volume=500_000)))
    df["date"] = df["date"].str[:10]
    df = df.set_index("date")
    q = U.qualifies(df, min_price=2.0, min_dollar_volume=1e6)
    assert not q.iloc[:10].any()             # fewer than 10 prior sessions of dollar volume: not judged yet
    assert q.iloc[10:26].all()               # $1.5M a day, prior close $3
    assert not q.iloc[26:].any()             # prior close $1.50
    assert not U.qualifies(df, 2.0, 2e6).any()   # median dollar volume is $1.5M
    assert X.DOLLAR_VOLUME_WINDOW == 21 and X.BAR_TOL == 0.005   # the constants this rule is defined by


def test_build_writes_what_the_factory_reads_and_counts_ended_names(tmp_path):
    m = meta(tmp_path)
    out = tmp_path / "store"
    (out / "prices").mkdir(parents=True)
    long = days(300, "2015-01-02")
    stale = b"".join(f"{d},10.0,10.0,10.0,10.0,0,10.0,10.0,10.0,10.0,0,0.0,1.0\n".encode() for d in long[100:])
    (out / "prices" / f"{A}.csv").write_bytes(bars(long[:100], 10.0) + stale)   # stops trading in 2015, then flat no-volume bars
    (out / "prices" / f"{B}.csv").write_bytes(bars(long, 20.0, volume=200_000))
    rep = U.build(m, out)
    assert rep["included"] == 2 and rep["included_with_prices"] == 2
    assert rep["ended_by_last_bar_year"] == {"2015": 1}
    assert rep["tickers_shared_by_included_companies"] == 1
    assert rep["kinds"] == {"common": 3, "spac": 1, "unit": 1}
    assert rep["price_files_cut"] == 1 and rep["rows_cut_outside_traded_life"] == 200 and rep["panel"] == "panel"

    panel = out / "panel"
    with open(panel / "members.csv", newline="") as fh:                   # read the way the factory reads it
        rows = [(r["date"], [t for t in r["tickers"].split(",") if t]) for r in csv.DictReader(fh)]
    assert rows == [(long[0], [A, B])]
    assert json.loads((panel / "renames.json").read_text()) == {"renames": {}}
    assert (panel / "sectors.csv").read_text() == "Symbol,GICS Sector\n"   # the factory's "no labels" file
    assert (panel / "prices" / f"{A}.csv").read_bytes() == bars(long[:100], 10.0)     # cut at the last traded day
    assert (panel / "prices" / f"{B}.csv").read_bytes() == (out / "prices" / f"{B}.csv").read_bytes()
    assert (out / "prices" / f"{A}.csv").read_bytes().count(b"\n") == 301           # the file as served is kept
    with open(out / "companies.csv", newline="") as fh:
        table = {r["perma"]: r for r in csv.DictReader(fh)}
    assert table[A]["ended"] == "1" and table[A]["last_bar"] == long[99]
    assert table[B]["ended"] == "0" and table[C]["included"] == "0" and table[C]["bars"] == "0"

    assert U.universe_on(out, long[50], 2.0, 1e6) == [A, B]
    assert U.universe_on(out, long[99], 2.0, 1e6) == [A, B]               # its last traded day
    assert U.universe_on(out, long[100], 2.0, 1e6) == [B]                 # gone the next session, not carried as a flat bar
    assert U.universe_on(out, long[150], 2.0, 1e6) == [B]
    counts = U.daily_counts(out, 2.0, 1e6)
    assert counts[long[50]] == 2 and counts[long[150]] == 1 and long[5] not in counts


def test_traded_life_cuts_both_ends_keeps_a_halt_and_refuses_what_it_cannot_read():
    d = days(8)
    flat = "{},5.0,5.0,5.0,5.0,{},5.0,5.0,5.0,5.0,{},0.0,1.0\n"
    vols = [0, 100, 100, 0, 100, 0, 0, 0]                                  # padded before, a halt inside, padded after
    raw = (HEADER + "\n" + "".join(flat.format(f"{x}T00:00:00.000Z", v, v) for x, v in zip(d, vols, strict=True))).encode()
    body, first, last, n, cut = U.traded_life(raw)
    assert (first, last, n, cut) == (d[1], d[4], 3, 4)
    assert body.splitlines()[0] == U.PRICE_HEADER and [ln[:10].decode() for ln in body.splitlines()[1:]] == d[1:5]
    assert U.traded_life((HEADER + "\n" + flat.format(d[0], 0, 0)).encode()) == (b"", "", "", 0, 1)
    with pytest.raises(U.UniverseError, match="header"):
        U.traded_life(b"date,close\n2020-01-02,5\n")
    with pytest.raises(U.UniverseError, match="date order"):
        U.traded_life((HEADER + "\n" + flat.format(d[1], 1, 1) + flat.format(d[0], 1, 1)).encode())


def test_build_drops_a_panel_file_whose_company_no_longer_has_traded_bars_and_counts_need_a_build(tmp_path):
    m = meta(tmp_path)
    out = tmp_path / "store"
    (out / "prices").mkdir(parents=True)
    d = days(40)
    (out / "prices" / f"{A}.csv").write_bytes(bars(d, 10.0))
    (out / "prices" / f"{B}.csv").write_bytes(bars(d, 20.0))
    with pytest.raises(U.UniverseError, match="run build first"):         # the files as served are never ranked
        U.universe_on(out, d[30], 2.0, 1e5)
    U.build(m, out)
    assert sorted(f.name for f in (out / "panel" / "prices").iterdir()) == [f"{A}.csv", f"{B}.csv"]
    (out / "prices" / f"{A}.csv").write_bytes(bars(d, 10.0, volume=0))    # a later pull holds no traded day for it
    rep = U.build(m, out)
    assert sorted(f.name for f in (out / "panel" / "prices").iterdir()) == [f"{B}.csv"]
    assert rep["included_with_prices_never_traded"] == 1
    assert U.universe_on(out, d[30], 2.0, 1e5) == [B]


def test_build_refuses_a_price_file_the_list_does_not_include(tmp_path):
    out = tmp_path / "store"
    (out / "prices").mkdir(parents=True)
    (out / "prices" / f"{D}.csv").write_bytes(bars(days(30), 10.0))       # a depositary receipt
    with pytest.raises(U.UniverseError, match="does not include"):
        U.build(meta(tmp_path), out)
    with pytest.raises(U.UniverseError, match="run fetch first"):
        U.build(meta(tmp_path), tmp_path / "nothing")
