"""Small-cap universe: permanent ids as keys, share-kind filter, a pull bound to its start and vintage, whole-file
checks before anything is written, panel generations that are never edited, and the factory's own universe."""
import csv
import json
import sqlite3

import numpy as np
import pandas as pd
import pytest

from analysis import cross_section_factory as X
from analysis import smallcap_universe as U

HEADER = "date,close,high,low,open,volume,adjClose,adjHigh,adjLow,adjOpen,adjVolume,divCash,splitFactor"
A, B, C, D, E = (f"US{n:012d}" for n in (1, 2, 3, 4, 5))
START, VINTAGE, TODAY = "2009-01-02", "2022-12-30", "2023-01-03"


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


def fetch(cs, out, get, m, **kw):
    return U.fetch_prices(cs, out, get=get, sleep=lambda s: None, today=TODAY,
                          **{"vintage": VINTAGE, "company_list_sha256": U.sha256_file(m), **kw})


def store(tmp_path, files, rows=None):
    """A data directory holding ``files`` (id -> bytes as served), bound to the test pull."""
    m = meta(tmp_path, rows)
    out = tmp_path / "store"
    (out / "prices").mkdir(parents=True)
    for perma, raw in files.items():
        (out / "prices" / f"{perma}.csv").write_bytes(raw)
    U.bind_existing(m, out, start=START, vintage=VINTAGE)
    return m, out


def generation(out):
    return out / "panel" / (out / "panel" / "CURRENT").read_text().strip()


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
    m = meta(tmp_path)
    cs = U.load_companies(m)
    asked = []
    answers = {A: (200, bars(days(3), 10.0)), B: (500, b"")}

    def get(url):
        asked.append(url)
        return answers[next(p for p in answers if p in url)]

    out = tmp_path / "store"
    tally = fetch(cs, out, get, m)
    assert tally == {"prices": 1, "failed": 1, "not_included": 3}
    assert all("/tiingo/daily/US" in u and "PARA" not in u.upper().replace("PRICES", "") for u in asked)
    assert all(f"startDate={START}&endDate={VINTAGE}&" in u for u in asked)   # every file is asked for the same days
    assert (out / "prices" / f"{A}.csv").read_bytes() == answers[A][1]
    assert not list((out / "prices").glob("*.part"))
    assert not (out / "prices" / f"{B}.csv").exists()               # a failure leaves nothing, so it is asked again

    asked.clear()
    answers[B] = (200, b'{"detail": "over your hourly allocation"}')   # a 200 that is not the empty answer
    assert fetch(cs, out, get, m) == {"already_done": 1, "failed": 1, "not_included": 3}
    assert not (out / "no-prices" / B).exists()                     # never mistaken for "this company has no prices"

    asked.clear()
    answers[B] = (200, b"[]")                                       # Tiingo's answer for an id with no bars
    tally = fetch(cs, out, get, m)
    assert tally == {"already_done": 1, "no_prices": 1, "not_included": 3}
    assert len(asked) == 1 and B in asked[0]
    assert (out / "no-prices" / B).exists()

    asked.clear()
    assert fetch(cs, out, get, m) == {"already_done": 2, "not_included": 3}
    assert asked == []
    log = [json.loads(line) for line in (out / "fetch-log.jsonl").read_text().splitlines()]
    assert [(r["perma"], r["outcome"]) for r in log] == [(A, "prices"), (B, "failed"), (B, "failed"), (B, "no_prices")]
    assert "header" in log[2]["refused"]


def test_fetch_takes_the_header_alone_as_no_prices_but_not_a_cut_header(tmp_path):
    m = meta(tmp_path)
    cs = [c for c in U.load_companies(m) if c.included]
    answers = {A: (200, HEADER.encode() + b"\n"), B: (200, HEADER.encode()[:-5] + b"\n")}
    out = tmp_path / "store"
    tally = fetch(cs, out, lambda url: answers[next(p for p in answers if p in url)], m)
    assert tally == {"no_prices": 1, "failed": 1}
    assert (out / "no-prices" / A).exists() and not (out / "no-prices" / B).exists()
    assert not list((out / "prices").iterdir())
    assert HEADER.encode() == U.PRICE_HEADER


GOOD = "2020-01-02,5.0,5.0,5.0,5.0,100,5.0,5.0,5.0,5.0,100,0.0,1.0\n"


@pytest.mark.parametrize("body,why", [
    (b"date,error\n2020-01-02,ticker not found\n", "header"),                           # the stub a review probe served
    ((HEADER + "\n" + GOOD + "2020-01-03,oops\n").encode(), "13 fields"),               # a malformed row after a good one
    ((HEADER + "\n" + GOOD.replace("5.0,5.0,5.0,5.0,100", "5.0,5.0,5.0,five,100")).encode(), "cannot be read"),
    ((HEADER + "\n" + GOOD.replace(",100,", ",nan,", 1)).encode(), "not finite"),
    ((HEADER + "\n" + GOOD.replace(",100,", ",-1,", 1)).encode(), "negative volume"),
    ((HEADER + "\n" + GOOD.replace("2020-01-02", "2020-13-45")).encode(), "cannot be read"),
    ((HEADER + "\n" + GOOD.replace("2020-01-02", "Jan 2 2020")).encode(), "13 fields"),
    ((HEADER + "\n" + GOOD + GOOD).encode(), "repeats a day"),
    ((HEADER + "\n" + GOOD.replace("2020-01-02", "2020-01-03") + GOOD).encode(), "out of date order"),
    ((HEADER + "\n" + GOOD.replace("2020-01-02", "2008-12-31")).encode(), "outside the pull"),   # before the requested start
    ((HEADER + "\n" + GOOD.replace("2020-01-02", "2022-12-31")).encode(), "outside the pull"),   # after the vintage
    ((HEADER + "\n" + GOOD + "\n" + GOOD.replace("2020-01-02", "2020-01-03")).encode(), "13 fields"),   # a blank line inside
    ((HEADER + ",extra\n" + GOOD).encode(), "header"),
    ((HEADER + "\n" + GOOD.replace(",100,", ",,", 1)).encode(), "cannot be read"),                 # a blank volume
    ((HEADER + "\n" + GOOD.replace(",0.0,1.0", ",,1.0")).encode(), "cannot be read"),               # a blank dividend
    ((HEADER + "\n" + GOOD.replace(",0.0,1.0", ",0.0,")).encode(), "cannot be read"),               # a blank split factor
    ((HEADER + "\n" + GOOD.replace("2020-01-02", "")).encode(), "13 fields"),                       # a blank date
    # a review's witnesses: blank only in adjHigh or adjLow, which the factory does not read, at zero and positive volume
    ((HEADER + "\n2016-04-04,20,21,19,20,0,20,,19,20,0,0,1\n").encode(), "blank only in adjHigh or adjLow"),
    ((HEADER + "\n2016-04-04,20,21,19,20,0,20,21,,20,0,0,1\n").encode(), "blank only in adjHigh or adjLow"),
    ((HEADER + "\n2016-04-04,20,21,19,20,100000,20,,19,20,100000,0,1\n").encode(), "blank only in adjHigh or adjLow"),
    ((HEADER + "\n2016-04-04,20,21,19,20,100000,20,21,,20,100000,0,1\n").encode(), "blank only in adjHigh or adjLow"),
    ((HEADER + "\n2016-04-04,20,21,19,20,100000,20,,,20,100000,0,1\n").encode(), "blank only in adjHigh or adjLow"),
])
def test_a_200_that_is_not_a_whole_daily_file_is_never_written_or_taken_as_done(tmp_path, body, why):
    m = meta(tmp_path)
    cs = [c for c in U.load_companies(m) if c.perma == A]
    out = tmp_path / "store"
    asked = []

    def get(url):
        asked.append(url)
        return 200, body

    assert fetch(cs, out, get, m) == {"failed": 1}
    assert not list((out / "prices").iterdir()) and not list((out / "no-prices").iterdir())
    assert fetch(cs, out, get, m) == {"failed": 1} and len(asked) == 2      # not resumable: it is asked again
    log = [json.loads(line) for line in (out / "fetch-log.jsonl").read_text().splitlines()]
    assert all(r["outcome"] == "failed" and why in r["refused"] for r in log)
    with pytest.raises(U.UniverseError, match=why):
        U.price_rows(body, START, VINTAGE)
    with pytest.raises(U.UniverseError, match=why):                         # build checks a file on disk the same way
        U.traded_life(body, START, VINTAGE)


def test_a_blank_price_is_kept_as_served_and_reported_but_other_blank_fields_are_refused(tmp_path):
    # Tiingo serves a blank low on some days without a trade (10 of the 9,576 files pulled 2026-11-20).
    blank_low = "2020-01-03,5.0,5.0,,5.0,0,5.0,5.0,,5.0,0,0.0,1.0\n"
    raw = (HEADER + "\n" + GOOD + blank_low + GOOD.replace("2020-01-02", "2020-01-06")).encode()
    rows = U.price_rows(raw, START, VINTAGE)
    assert [(r[0], r[1], r[3]) for r in rows] == [("2020-01-02", 100.0, False), ("2020-01-03", 0.0, True), ("2020-01-06", 100.0, False)]
    assert rows[1][2] == blank_low.encode()                                 # the line is not rewritten
    body, first, last, n, cut = U.traded_life(raw, START, VINTAGE)
    assert body == raw and (first, last, n, cut) == ("2020-01-02", "2020-01-06", 2, 0)   # a no-trade day inside the life stays
    names = HEADER.split(",")
    for i in (1, 2, 3, 4, 6, 7, 8, 9):                                      # each of the eight prices, blank on its own
        fields = GOOD.strip().split(",")
        fields[i] = ""
        one = (HEADER + "\n" + ",".join(fields) + "\n").encode()
        if names[i] in ("adjHigh", "adjLow"):                               # the factory would not see it: refused
            with pytest.raises(U.UniverseError, match="blank only in adjHigh or adjLow"):
                U.price_rows(one, START, VINTAGE)
        else:
            assert U.price_rows(one, START, VINTAGE)[0][3] is True
    assert [names[1 + i] for i in U._BAR_FIELDS] == ["close", "high", "low", "open", "adjClose", "adjOpen"]
    assert [names[1 + i] for i in U._PRICE_FIELDS] == ["close", "high", "low", "open", "adjClose", "adjHigh", "adjLow", "adjOpen"]
    m = meta(tmp_path)
    cs = [c for c in U.load_companies(m) if c.perma == A]
    out = tmp_path / "store"
    assert fetch(cs, out, lambda url: (200, raw), m) == {"prices": 1}       # written: it is a whole daily file
    assert (out / "prices" / f"{A}.csv").read_bytes() == raw


def test_a_resume_is_bound_to_the_start_the_vintage_and_the_company_list(tmp_path):
    m = meta(tmp_path)
    cs = [c for c in U.load_companies(m) if c.included]
    out = tmp_path / "store"
    served = bars(days(3), 10.0)
    fetch(cs[:1], out, lambda url: (200, served), m)
    state = json.loads((out / "fetch-state.json").read_text())
    assert state == {"schema": "smallcap-fetch/1", "start": START, "vintage": VINTAGE, "company_list_sha256": U.sha256_file(m)}
    never = lambda url: pytest.fail("a refused resume asks nothing")
    with pytest.raises(U.UniverseError, match="holds another pull .start differ"):
        fetch(cs, out, never, m, start="2012-01-03")
    with pytest.raises(U.UniverseError, match="holds another pull .vintage differ"):
        fetch(cs, out, never, m, vintage="2022-12-29")
    other = tmp_path / "other"
    other.mkdir()
    with pytest.raises(U.UniverseError, match="company_list_sha256 differ"):
        fetch(cs, out, never, meta(other, [{"permaTicker": A, "ticker": "a", "name": "A", "isActive": True, "isADR": False}]))
    assert json.loads((out / "fetch-state.json").read_text()) == state     # a refusal changes nothing
    assert fetch(cs, out, lambda url: (200, served), m) == {"already_done": 1, "prices": 1}   # the same pull resumes

    fresh = tmp_path / "fresh"
    for vintage, match in ((TODAY, "finished day"), ("2023-02-01", "finished day"), ("2023-1-2", "must be a day")):
        with pytest.raises(U.UniverseError, match=match):
            fetch(cs, fresh, never, m, vintage=vintage)
    with pytest.raises(U.UniverseError, match="start is after the vintage"):
        fetch(cs, fresh, never, m, start="2022-12-31")
    assert not (fresh / "fetch-state.json").exists()


def test_a_store_without_a_fetch_record_is_refused_until_it_is_bound(tmp_path):
    m = meta(tmp_path)
    cs = [c for c in U.load_companies(m) if c.included]
    out = tmp_path / "store"
    (out / "prices").mkdir(parents=True)
    (out / "prices" / f"{A}.csv").write_bytes(bars(days(30), 10.0))        # pulled before the record existed
    never = lambda url: pytest.fail("nothing is asked")
    with pytest.raises(U.UniverseError, match="no fetch-state.json: record the start and vintage"):
        fetch(cs, out, never, m)
    with pytest.raises(U.UniverseError, match="no fetch-state.json"):
        U.build(m, out)
    with pytest.raises(U.UniverseError, match=rf"{A}.csv: line 2 is dated 2020-01-02, outside the pull"):
        U.bind_existing(m, out, start=START, vintage="2019-12-31")           # the files say otherwise
    assert not (out / "fetch-state.json").exists()
    bound = U.bind_existing(m, out, start=START, vintage=VINTAGE)
    assert bound["price_files_checked"] == 1 and U.fetch_state(out)["vintage"] == VINTAGE
    with pytest.raises(U.UniverseError, match="already has"):
        U.bind_existing(m, out, start=START, vintage=VINTAGE)
    assert fetch(cs, out, lambda url: (200, b"[]"), m) == {"already_done": 1, "no_prices": 1}
    other = tmp_path / "other"
    other.mkdir()
    with pytest.raises(U.UniverseError, match="not the one this store was pulled with"):
        U.build(meta(other, [{"permaTicker": A, "ticker": "a", "name": "A", "isActive": True, "isADR": False}]), out)


def test_fetch_needs_a_key_and_sends_it_in_a_header(monkeypatch):
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    monkeypatch.delenv("TIINGO_KEY", raising=False)
    with pytest.raises(U.UniverseError, match="TIINGO_API_KEY"):
        U.tiingo_get("https://example.invalid/x")
    assert "token" not in U.PRICES_URL.lower()


def test_build_writes_what_the_factory_reads_and_counts_ended_names(tmp_path):
    long = days(300, "2015-01-02")
    stale = b"".join(f"{d},10.0,10.0,10.0,10.0,0,10.0,10.0,10.0,10.0,0,0.0,1.0\n".encode() for d in long[100:])
    m, out = store(tmp_path, {A: bars(long[:100], 10.0) + stale,              # stops trading in 2015, then flat no-volume bars
                              B: bars(long, 20.0, volume=200_000)})
    rep = U.build(m, out)
    panel = generation(out)
    assert rep["included"] == 2 and rep["included_with_prices"] == 2
    assert rep["ended_by_last_bar_year"] == {"2015": 1}
    assert rep["tickers_shared_by_included_companies"] == 1
    assert rep["kinds"] == {"common": 3, "spac": 1, "unit": 1}
    assert rep["price_files_cut"] == 1 and rep["rows_cut_outside_traded_life"] == 200
    assert rep["panel"] == f"panel/{panel.name}" and rep["generation"] == panel.name and rep["generation_already_built"] is False
    assert rep["discovery_first_session"] == "2016-01-04" and rep["fetch"] == {"start": START, "vintage": VINTAGE}

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

    # the export the factory binds: complete, hashed, dated, and carrying the pull it came from
    exp = json.loads((panel / X.EXPORT_MANIFEST).read_text())
    assert exp["schema"] == X.EXPORT_SCHEMA and exp["complete"] is True and exp["generation"] == panel.name
    assert exp["discovery_first_session"] == "2016-01-04"
    assert exp["source"] == {"provider": "tiingo", "endpoint": "daily prices by permanent id", "start": START, "vintage": VINTAGE,
                             "company_list_sha256": U.sha256_file(m), "builder": "smallcap-universe/1",
                             "collection": {"included": 2, "with_prices": 2, "confirmed_no_prices": 0}}
    assert exp["files"] == X.input_files(panel) and set(exp["files"]) == {
        f"prices/{A}.csv", f"prices/{B}.csv", "members.csv", "renames.json", "sectors.csv"}
    assert X.read_export(panel, X.input_files(panel))["generation"] == panel.name

    on = lambda day: U.universe_on(out, day, 2.0, 1e6, min_session_bars=1)
    assert on(long[50]) == [A, B]
    assert on(long[99]) == [A, B]                                         # its last traded day
    assert on(long[100]) == [B]                                           # gone the next session, not carried as a flat bar
    assert on(long[150]) == [B] and on("2015-01-01") == []
    counts = U.daily_counts(out, 2.0, 1e6, min_session_bars=1)
    assert counts[long[50]] == 2 and counts[long[150]] == 1 and counts[long[5]] == 0 and list(counts) == long
    with pytest.raises(U.UniverseError, match="no development sessions"):  # the factory's session rule, at its default
        U.daily_counts(out, 2.0, 1e6)


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


def test_a_generation_is_published_whole_and_never_edited(tmp_path):
    d = days(40)
    m, out = store(tmp_path, {A: bars(d, 10.0), B: bars(d, 20.0)})
    with pytest.raises(U.UniverseError, match="run build first"):         # the files as served are never ranked
        U.universe_on(out, d[30], 2.0, 1e5, min_session_bars=1)
    first = U.build(m, out)
    one = generation(out)
    assert sorted(f.name for f in (one / "prices").iterdir()) == [f"{A}.csv", f"{B}.csv"]
    assert not [p for p in (out / "panel").iterdir() if p.name.startswith(".building")]   # nothing half-built is left
    assert all(not p.stat().st_mode & 0o222 for p in [one, *one.rglob("*")])              # nothing in it is writable
    again = U.build(m, out)                                               # the same inputs are the same generation
    assert again["generation"] == first["generation"] and again["generation_already_built"] is True
    assert [p.name for p in (out / "panel").iterdir() if p.is_dir()] == [one.name]
    before = {p.name: p.read_bytes() for p in [*one.rglob("*.csv"), *one.glob("*.json")]}

    (out / "prices" / f"{A}.csv").write_bytes(bars(d, 10.0, volume=0))    # other inputs: A holds no traded day now
    rep = U.build(m, out)
    two = generation(out)
    assert two.name != one.name and rep["generation"] == two.name and rep["included_with_prices_never_traded"] == 1
    assert sorted(f.name for f in (two / "prices").iterdir()) == [f"{B}.csv"]
    assert {p.name: p.read_bytes() for p in [*one.rglob("*.csv"), *one.glob("*.json")]} == before   # the old one is untouched
    assert X.read_export(one, X.input_files(one))["generation"] == one.name
    assert U.universe_on(out, d[30], 2.0, 1e5, min_session_bars=1) == [B]

    target = two / "prices" / f"{B}.csv"                                  # someone edits a file inside the generation
    (two / "prices").chmod(0o755)
    target.chmod(0o644)
    target.write_bytes(target.read_bytes().replace(b"20.0", b"21.0", 1))
    with pytest.raises(U.UniverseError, match="not a whole export.*1 file"):
        U.universe_on(out, d[30], 2.0, 1e5, min_session_bars=1)
    with pytest.raises(U.UniverseError, match="not a whole export"):
        U.daily_counts(out, 2.0, 1e5, min_session_bars=1)
    (out / "panel" / "CURRENT").write_text("g-0000000000000000\n")
    with pytest.raises(U.UniverseError, match="does not name a built generation"):
        U.universe_on(out, d[30], 2.0, 1e5, min_session_bars=1)


def test_build_refuses_a_price_file_the_list_does_not_include(tmp_path):
    out = tmp_path / "store"
    (out / "prices").mkdir(parents=True)
    (out / "prices" / f"{D}.csv").write_bytes(bars(days(30), 10.0))       # a depositary receipt
    with pytest.raises(U.UniverseError, match="does not include"):
        U.build(meta(tmp_path), out)
    with pytest.raises(U.UniverseError, match="run fetch first"):
        U.build(meta(tmp_path), tmp_path / "nothing")


def many(tmp_path, d, n=25, edits=None):
    """``n`` companies on a random walk near $20, $2M a day; ``edits`` maps a company index to a change of its lines."""
    rng = np.random.default_rng(11)
    ids = [f"US{100 + i:012d}" for i in range(n)]
    rows = [{"permaTicker": perma, "ticker": f"c{i}", "name": f"Company {i}", "isActive": True, "isADR": False}
            for i, perma in enumerate(ids)]
    files = {}
    for i, perma in enumerate(ids):
        close = list(np.round(20.0 * np.cumprod(1 + rng.normal(0, 0.01, len(d))), 4))
        lines = bars(d, close).splitlines(keepends=True)
        files[perma] = b"".join((edits or {}).get(i, lambda x: x)(lines))
    m, out = store(tmp_path, files, rows)
    return m, out, ids


def test_counts_are_the_factory_s_universe_on_a_missing_session_and_an_invalid_adjusted_bar(tmp_path):
    d = days(60, "2016-02-01")
    gap, bad, blank = 30, 40, 45

    def blank_low(lines):
        f = lines[blank + 1].split(b",")
        f[3] = f[8] = b""
        return [*lines[:blank + 1], b",".join(f), *lines[blank + 2:]]

    def zero_adjusted_close(lines):
        f = lines[bad + 1].split(b",")
        f[6] = b"0.0"
        return [*lines[:bad + 1], b",".join(f), *lines[bad + 2:]]

    m, out, ids = many(tmp_path, d, edits={3: lambda lines: [*lines[:gap + 1], *lines[gap + 2:]],   # no bar on session `gap`
                                           4: zero_adjusted_close, 5: blank_low})
    rep = U.build(m, out)
    assert rep["rows_with_a_blank_price"] == 1 and rep["price_files_with_a_blank_price"] == 1
    on = lambda day: U.universe_on(out, day, 2.0, 1e6, min_session_bars=20)
    everyone = sorted(ids)
    assert on(d[gap - 1]) == everyone
    assert on(d[gap]) == sorted(set(ids) - {ids[3]})                      # no bar that day
    # The review's probe. The company's own previous row (session gap-1) closed near $20, so reading adjacent rows of
    # its file would admit it on gap+1. The session before gap+1 has no bar for it, so the factory does not.
    assert on(d[gap + 1]) == sorted(set(ids) - {ids[3]})
    assert on(d[gap + 2]) == everyone
    assert on(d[bad]) == sorted(set(ids) - {ids[4]}) and on(d[bad + 1]) == everyone   # a zero adjusted close is not a bar
    assert on(d[blank]) == sorted(set(ids) - {ids[5]}) and on(d[blank + 1]) == everyone   # nor is a day with a blank low
    assert b",," in (generation(out) / "prices" / f"{ids[5]}.csv").read_bytes()         # the panel keeps the row as served

    panel = generation(out)
    run = X.RunFile.from_dict({"run_id": "parity", "first_session": d[0], "last_session": d[-2], "rankings": ["pooled"],
                               "fractions": [0.2], "min_session_bars": 20, "min_ranked": 20, "min_price": 2, "min_dollar_volume": 1e6,
                               "signals": [{"name": "rev5", "kind": "reversal", "lookback": 5, "horizons": ["close_1d"]}]})
    loaded = X.load_panel(panel, run, X.input_files(panel))               # what a run would rank
    counts = U.daily_counts(out, 2.0, 1e6, min_session_bars=20)
    assert list(counts) == loaded.sessions == d
    assert list(counts.values()) == loaded.base.sum(axis=1).tolist()
    for r, day in enumerate(d):
        assert on(day) == sorted(c for c, x in zip(loaded.cols, loaded.base[r], strict=True) if x)
    assert counts[d[9]] == 0 and counts[d[10]] == 25                      # ten sessions of dollar volume before the first count


def test_the_factory_runs_the_built_export_and_holds_it_to_its_2016_start(tmp_path):
    d = days(150, "2015-11-02")                                           # 2015-11-02 .. 2016-05-27
    m, out, _ = many(tmp_path, d)
    rep = U.build(m, out)
    panel = generation(out)
    con = sqlite3.connect(tmp_path / "state.db")
    con.execute("CREATE TABLE desk_state (data_through TEXT PRIMARY KEY, record_json TEXT)")
    for day in d:
        con.execute("INSERT INTO desk_state VALUES (?, ?)", (day, json.dumps({
            "market": {"phase": "rising, slowing"}, "fed_balance_sheet": {"phase": "flat"},
            "vix": {"third": "calm", "spike_state": "normal"}, "sectors": {}})))
    con.commit()
    con.close()
    body = {"run_id": "xs-smallcap-synthetic", "first_session": "2016-01-04", "last_session": d[-2], "rankings": ["pooled"],
            "fractions": [0.2], "perms": 1000, "min_session_bars": 20, "min_ranked": 20, "cards": 1,
            "min_price": 2, "min_dollar_volume": 1e6, "cost_bp_per_side": 25.0,
            "signals": [{"name": "rev5", "kind": "reversal", "lookback": 5, "horizons": ["close_1d"]}]}
    ledger = X.open_ledger(tmp_path / "l.jsonl", tmp_path / "l.anchors.jsonl", "xs-smallcap-test-ledger")
    before = (tmp_path / "l.jsonl").read_text()
    assert U.DISCOVERY_FIRST == "2016-01-04" and json.loads((panel / X.EXPORT_MANIFEST).read_text())["source"]["collection"] == {
        "included": 25, "with_prices": 25, "confirmed_no_prices": 0}
    early = tmp_path / "early.json"
    early.write_text(json.dumps({**body, "run_id": "xs-early", "first_session": "2015-12-31"}))
    with pytest.raises(X.RunFileError, match="discovery on this export starts 2016-01-04; the run file's window starts 2015-12-31"):
        X.run(early, panel, tmp_path / "state.db", ledger, tmp_path / "out-early")
    assert (tmp_path / "l.jsonl").read_text() == before and not (tmp_path / "out-early").exists()   # nothing was declared

    run_file = tmp_path / "run.json"
    run_file.write_text(json.dumps(body))
    report = X.run(run_file, panel, tmp_path / "state.db", ledger, tmp_path / "out")
    cov = report["coverage"]
    assert cov["panel_export"]["generation"] == rep["generation"] and cov["panel_export"]["discovery_first_session"] == "2016-01-04"
    assert cov["sector_assignment"]["labels"] == "none" and cov["price_files"] == 25 and cov["first"] == d[0]
    assert cov["floors"] == {"min_price": 2, "min_dollar_volume": 1e6, "dollar_volume_window": 21}
    assert report["trial_count"] == 1 and report["tested"] == 1 and report["ranking"][0]["n"] > 90


def test_the_builder_cannot_seal_a_discovery_start_before_2016(tmp_path):
    d = days(150, "2015-01-02")                                           # a store whose bars would support a 2015 window
    m, out, _ = many(tmp_path, d)
    for early in ("2015-01-02", "2015-12-31", "2016-01-03"):
        with pytest.raises(U.UniverseError, match=f"starts no earlier than 2016-01-04; {early} is refused"):
            U.build(m, out, discovery_first=early)
    assert not (out / "panel").exists()                                   # nothing was sealed, so the factory has nothing to read
    assert U.main(["build", "--meta", str(m), "--data-dir", str(out), "--discovery-first", "2015-01-02"]) == 2
    assert not (out / "panel").exists()
    with pytest.raises(U.UniverseError, match="must be a day"):
        U.build(m, out, discovery_first="2016")

    rep = U.build(m, out)                                                 # the default is the floor itself
    panel = generation(out)
    assert rep["discovery_first_session"] == "2016-01-04"
    con = sqlite3.connect(tmp_path / "state.db")
    con.execute("CREATE TABLE desk_state (data_through TEXT PRIMARY KEY, record_json TEXT)")
    con.commit()
    con.close()
    body = {"run_id": "xs-2015-window", "first_session": "2015-03-02", "last_session": d[-2], "rankings": ["pooled"],
            "fractions": [0.2], "perms": 1000, "min_session_bars": 20, "min_ranked": 20, "min_price": 2, "min_dollar_volume": 1e6,
            "signals": [{"name": "rev5", "kind": "reversal", "lookback": 5, "horizons": ["close_1d"]}]}
    run_file = tmp_path / "run.json"
    run_file.write_text(json.dumps(body))
    ledger = X.open_ledger(tmp_path / "l.jsonl", tmp_path / "l.anchors.jsonl", "xs-smallcap-cutoff-ledger")
    before = (tmp_path / "l.jsonl").read_text()
    with pytest.raises(X.RunFileError, match="discovery on this export starts 2016-01-04; the run file's window starts 2015-03-02"):
        X.run(run_file, panel, tmp_path / "state.db", ledger, tmp_path / "out")     # the review's 2015 run, end to end
    assert (tmp_path / "l.jsonl").read_text() == before and not (tmp_path / "out").exists()   # refused before any declaration

    later = tmp_path / "later"
    later.mkdir()
    m2, out2, _ = many(later, d)
    assert U.build(m2, out2, discovery_first="2016-06-01")["discovery_first_session"] == "2016-06-01"   # later is allowed
    assert json.loads((generation(out2) / X.EXPORT_MANIFEST).read_text())["discovery_first_session"] == "2016-06-01"


def test_a_pull_with_a_failed_or_unasked_company_is_never_sealed(tmp_path):
    m = meta(tmp_path)
    cs = U.load_companies(m)
    out = tmp_path / "store"
    answers = {A: (200, bars(days(40), 10.0)), B: (500, b"")}             # the review's probe: A answers, B fails
    get = lambda url: answers[next(p for p in answers if p in url)]
    assert fetch(cs, out, get, m) == {"prices": 1, "failed": 1, "not_included": 3}
    assert U.collection(out, cs) == {"included": 2, "with_prices": 1, "confirmed_no_prices": 0, "unresolved": [B]}
    with pytest.raises(U.UniverseError, match=rf"the pull is not finished: 1 included id\(s\) have neither.*first \['{B}'\]; run fetch again"):
        U.build(m, out)
    assert not (out / "panel").exists() and not (out / "universe-report.json").exists()   # nothing sealed, nothing reported
    assert U.main(["build", "--meta", str(m), "--data-dir", str(out)]) == 2

    interrupted = tmp_path / "interrupted"                                # stopped before B was asked at all
    fetch([c for c in cs if c.perma != B], interrupted, get, m)
    with pytest.raises(U.UniverseError, match="the pull is not finished"):
        U.build(m, interrupted)

    answers[B] = (200, b"[]")                                             # Tiingo confirms it holds no bars for B
    assert fetch(cs, out, get, m) == {"already_done": 1, "no_prices": 1, "not_included": 3}
    rep = U.build(m, out)
    assert rep["included_without_prices"] == 1 and rep["included_unresolved"] == 0 and rep["included_with_prices"] == 1
    exp = json.loads((generation(out) / X.EXPORT_MANIFEST).read_text())
    assert exp["complete"] is True and exp["source"]["collection"] == {"included": 2, "with_prices": 1, "confirmed_no_prices": 1}

    (out / "no-prices" / A).touch()                                       # a mark beside a price file contradicts it
    with pytest.raises(U.UniverseError, match="both a price file and a no-prices mark"):
        U.build(m, out)
    (out / "no-prices" / A).unlink()
    (out / "no-prices" / D).touch()                                       # a mark for a company the list leaves out
    with pytest.raises(U.UniverseError, match="no-prices marks for ids the company list does not include"):
        U.build(m, out)


@pytest.mark.parametrize("volume", [0, 100_000])
def test_a_blank_in_any_price_the_builder_accepts_is_never_a_valid_bar_in_the_factory(tmp_path, volume):
    # One store, one session, six companies: each has exactly one price blank, a different one each. Every price the
    # builder accepts a lone blank in must take the company out of the factory's universe that day. The two prices
    # the factory does not read (adjHigh, adjLow) are refused by the builder instead, tested with the other bodies.
    d = days(60, "2016-02-01")
    day = 44
    names = HEADER.split(",")

    def blank_one(field):
        def edit(lines):
            f = lines[day + 1].rstrip(b"\n").split(b",")
            f[5] = f[10] = str(volume).encode()
            f[names.index(field)] = b""
            return [*lines[:day + 1], b",".join(f) + b"\n", *lines[day + 2:]]
        return edit

    accepted = [names[1 + i] for i in U._BAR_FIELDS]
    m, out, ids = many(tmp_path, d, edits={i: blank_one(field) for i, field in enumerate(accepted)})
    rep = U.build(m, out)
    assert rep["rows_with_a_blank_price"] == 6 and rep["price_files_with_a_blank_price"] == 6
    panel = generation(out)
    u = X.universe(panel, min_session_bars=19, min_price=2.0, min_dollar_volume=1e6, files=X.input_files(panel))
    r = u.sessions.index(d[day])
    for i, field in enumerate(accepted):
        col = u.cols.index(ids[i])
        assert not u.ok.values[r, col] and not u.base[r, col], field           # not a valid bar, not rankable
        assert u.ok.values[r - 1, col] and u.base[r - 1, col], field           # and it was the day before
        served = (out / "prices" / f"{ids[i]}.csv").read_bytes().splitlines()[day + 1]
        assert (panel / "prices" / f"{ids[i]}.csv").read_bytes().splitlines()[day + 1] == served and b",," in served + b","
    untouched = [u.cols.index(c) for c in ids[6:]]
    assert u.ok.values[r, untouched].all() and u.base[r, untouched].all()
    assert U.universe_on(out, d[day], 2.0, 1e6, min_session_bars=19) == sorted(ids[6:])
    run = X.RunFile.from_dict({"run_id": "blank", "first_session": d[0], "last_session": d[-2], "rankings": ["pooled"],
                               "fractions": [0.2], "min_session_bars": 20, "min_ranked": 20, "min_price": 2, "min_dollar_volume": 1e6,
                               "signals": [{"name": "rev5", "kind": "reversal", "lookback": 5, "horizons": ["close_1d"]}]})
    loaded = X.load_panel(panel, run, X.input_files(panel))                 # a run reads the same mask
    assert not loaded.base[loaded.sessions.index(d[day]), [loaded.cols.index(c) for c in ids[:6]]].any()

    # the two prices the factory does not read: a store holding such a row cannot be bound or built at all
    for field in ("adjHigh", "adjLow"):
        root = tmp_path / field
        root.mkdir()
        mm = meta(root, [{"permaTicker": f"US{100 + i:012d}", "ticker": f"c{i}", "name": f"Company {i}", "isActive": True, "isADR": False}
                         for i in range(25)])
        store_dir = root / "store"
        (store_dir / "prices").mkdir(parents=True)
        for perma in ids:
            raw = (out / "prices" / f"{perma}.csv").read_bytes()
            (store_dir / "prices" / f"{perma}.csv").write_bytes(
                b"".join(blank_one(field)(bars(d, 20.0).splitlines(keepends=True))) if perma == ids[7] else raw)
        with pytest.raises(U.UniverseError, match=rf"{ids[7]}.csv: line {day + 2} is blank only in adjHigh or adjLow"):
            U.bind_existing(mm, store_dir, start=START, vintage=VINTAGE)
        (store_dir / "fetch-state.json").write_text((out / "fetch-state.json").read_text())   # even a store bound some other way
        with pytest.raises(U.UniverseError, match="blank only in adjHigh or adjLow"):
            U.build(mm, store_dir)
        assert not (store_dir / "panel").exists()


def test_the_committed_first_run_file_fits_the_built_export():
    from pathlib import Path
    path = Path(__file__).resolve().parents[1] / "docs/research/cross-section-runs/xs-smallcap-published-effects-1-20261120.json"
    body = json.loads(path.read_text())
    run = X.RunFile.from_dict(body)                                         # the factory accepts it as written
    assert run.first_session >= U.DISCOVERY_FIRST and run.last_session <= X.DEV_LAST   # inside the export's discovery days
    assert run.sector_uses() == [] and run.rankings == ("pooled",)          # the store has no sector labels
    assert (run.min_price, run.min_dollar_volume, run.cost_bp_per_side) == (2, 1_000_000, 25.0)
    assert run.trial_count() == 8 and {m["horizon"] for m in run.modules()} == {"close_1d"}
    assert any("S&P 500" in look for look in run.prior_dev_looks) and any("spread estimate" in look for look in run.prior_dev_looks)
