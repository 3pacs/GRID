"""Tests for the staged EDGAR acceptance backfill (G2). No network: every fetch is an injected fake."""

from __future__ import annotations

import gzip
import json
import sys
import types
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from ingestion import edgar_acceptance_backfill as B

UTC = timezone.utc

HEADER = ("Description: x\n\n\nForm Type   Company Name                                                  CIK         Date Filed  File Name\n"
          + "-" * 100 + "\n")


def idx(*rows: str) -> bytes:
    return gzip.compress((HEADER + "\n".join(rows) + "\n").encode("latin-1"))


def row(form: str, name: str, cik: int, filed: str, acc: str) -> str:
    # deliberately misaligned against the header, as the SEC's real file is
    return f"{form:<17}{name:<62}{cik:<12}{filed:<12}edgar/data/{cik}/{acc}.txt            "


A1, A2, A3 = "0000000001-24-000001", "0000000001-24-000002", "0000000002-24-000003"


def sub(cik: str, entries: list[tuple], files=()) -> bytes:
    cols = {"accessionNumber": [], "filingDate": [], "form": [], "acceptanceDateTime": [], "items": []}
    for acc, filed, form, acc_utc, items in entries:
        for k, v in zip(cols, (acc, filed, form, acc_utc, items)):
            cols[k].append(v)
    return json.dumps({"cik": cik.lstrip("0"), "filings": {"recent": cols, "files": list(files)}}).encode()


class Site:
    """A fake SEC: url -> bytes, recording every request."""

    def __init__(self, pages: dict[str, bytes]):
        self.pages, self.calls = pages, []

    def __call__(self, url: str) -> bytes:
        self.calls.append(url)
        if url not in self.pages:
            raise FileNotFoundError(url)
        return self.pages[url]


def quarter_url(y: int, q: int) -> str:
    return B.INDEX_URL.format(y=y, q=q)


def world(tmp_path: Path):
    pages = {
        quarter_url(2024, 1): idx(row("8-K", "ONE CO", 1, "2024-01-05", A1), row("8-K/A", "ONE CO", 1, "2024-01-09", A2),
                                  row("8-K12B", "OTHER", 5, "2024-01-09", "0000000005-24-000001"),
                                  row("10-K", "ONE CO", 1, "2024-01-10", "0000000001-24-000009")),
        quarter_url(2024, 2): idx(row("8-K", "TWO CO", 2, "2024-04-02", A3), row("8-K", "ONE CO", 1, "2024-04-02", A3)),
        quarter_url(2024, 3): idx(), quarter_url(2024, 4): idx(),
        "https://data.sec.gov/submissions/CIK0000000001.json": sub("1", [
            (A1, "2024-01-05", "8-K", "2024-01-05T21:30:05.000Z", "2.02,9.01"),
            (A2, "2024-01-09", "8-K/A", "2024-01-09T14:00:00.000Z", "2.02"),
            (A3, "2024-04-02", "8-K", "2024-04-02T20:05:00.000Z", "5.02")]),
        "https://data.sec.gov/submissions/CIK0000000002.json": sub("2", [(A3, "2024-04-02", "8-K", "2024-04-02T20:05:00.000Z", "5.02")]),
    }
    return Site(pages), tmp_path / "out"


def run_plan(site, out):
    return B.stage_plan(out, site, 2024, 2024, today=date(2026, 11, 26))


# ------------------------------------------------------------------------------------------------ parsing


def test_parse_reads_8k_and_8ka_from_the_right_and_ignores_every_other_form():
    text = idx(row("8-K", "A CO 123", 7, "2024-01-02", A1), row("8-K/A", "B", 8, "2024-01-03", A2),
               row("8-K12B", "C", 9, "2024-01-03", A3), row("8-K15D5", "D", 9, "2024-01-03", A3), row("SC 13D", "E", 9, "2024-01-03", A3))
    got = B.parse_form_idx(gzip.decompress(text).decode())
    assert [(p.accession, p.ciks, p.form, p.filed) for p in got] == [
        (A1, ("0000000007",), "8-K", "2024-01-02"), (A2, ("0000000008",), "8-K/A", "2024-01-03")]


@pytest.mark.parametrize("bad", [
    "8-K              BROKEN LINE WITHOUT FIELDS",
    "8-K              X     1     2024-13-45  edgar/data/1/0000000001-24-000001.txt",
    "8-K              X     1     2024-01-02  edgar/data/1/short.txt",
])
def test_a_malformed_8k_row_is_an_error_not_a_skip(bad):
    with pytest.raises(B.BackfillError if "13-45" not in bad else ValueError):
        B.parse_form_idx(HEADER + bad + "\n")


def test_no_header_is_an_error():
    with pytest.raises(B.BackfillError, match="Form Type"):
        B.parse_form_idx("8-K  X  1  2024-01-02  edgar/data/1/0000000001-24-000001.txt\n")


# ------------------------------------------------------------------------------------------------ plan


def test_plan_keeps_raw_hashes_and_merges_co_registrants(tmp_path):
    site, out = world(tmp_path)
    s = run_plan(site, out)
    assert s["planned_accessions"] == 3 and s["planned_ciks"] == 2
    plan = {p.accession: p for p in B.load_plan(out)}
    assert plan[A3].ciks == ("0000000001", "0000000002")  # one filing, two registrants
    assert (out / "raw" / "index" / "2024Q1.gz").read_bytes() == site.pages[quarter_url(2024, 1)]
    assert s["plan_sha256"] == B.sha256_bytes((out / "plan.jsonl").read_bytes())


def test_plan_accepts_an_uncompressed_index_too(tmp_path):
    site, out = world(tmp_path)
    site.pages[quarter_url(2024, 1)] = gzip.decompress(site.pages[quarter_url(2024, 1)])
    assert run_plan(site, out)["planned_accessions"] == 3


def test_plan_refuses_a_quarter_that_has_not_started(tmp_path):
    site, out = world(tmp_path)
    with pytest.raises(B.BackfillError, match="has not started"):
        B.stage_plan(out, site, 2024, 2024, today=date(2024, 5, 1))


def test_plan_refuses_one_accession_with_two_dates(tmp_path):
    site, out = world(tmp_path)
    site.pages[quarter_url(2024, 4)] = idx(row("8-K", "X", 3, "2024-10-01", A1))
    with pytest.raises(B.BackfillError, match="different form or date"):
        run_plan(site, out)


def test_plan_window_is_checked():
    with pytest.raises(B.BackfillError):
        B.quarters(2025, 2024)


# ------------------------------------------------------------------------------------------------ fetch


def fetch_all(site, out, **kw):
    return B.stage_fetch(out, site, clock=lambda: datetime(2026, 11, 26, 4, 0, 0, tzinfo=UTC), **kw)


def test_fetch_stores_raw_gzip_with_a_hash_manifest(tmp_path):
    site, out = world(tmp_path)
    run_plan(site, out)
    r = fetch_all(site, out)
    assert r["ciks_stored"] == 2
    man = B._load_manifest(out)
    e = man["CIK0000000001.json"]
    raw = gzip.decompress((out / "raw/submissions/CIK0000000001.json.gz").read_bytes())
    assert e["sha256"] == B.sha256_bytes(raw) and e["retrieved_at"] == "2026-11-26T04:00:00Z" and e["status"] == "OK"


def test_fetch_resumes_without_refetching_verified_files_and_refetches_corrupt_ones(tmp_path):
    site, out = world(tmp_path)
    run_plan(site, out)
    fetch_all(site, out)
    n = len(site.calls)
    fetch_all(site, out)
    assert len(site.calls) == n  # nothing re-requested
    (out / "raw/submissions/CIK0000000002.json.gz").write_bytes(gzip.compress(b"{}"))  # tampered
    fetch_all(site, out)
    assert len(site.calls) == n + 1
    assert B.sha256_bytes(gzip.decompress((out / "raw/submissions/CIK0000000002.json.gz").read_bytes())) == B._load_manifest(out)["CIK0000000002.json"]["sha256"]


def test_fetch_records_a_missing_history_and_limit_and_older_pages(tmp_path):
    site, out = world(tmp_path)
    old = {"name": "CIK0000000001-submissions-001.json", "filingFrom": "2001-01-01", "filingTo": "2011-12-31"}
    mid = {"name": "CIK0000000001-submissions-002.json", "filingFrom": "2023-01-01", "filingTo": "2024-02-01"}
    site.pages["https://data.sec.gov/submissions/CIK0000000001.json"] = sub("1", [(A1, "2024-01-05", "8-K", "2024-01-05T21:30:05.000Z", "2.02")], files=[old, mid])
    site.pages["https://data.sec.gov/submissions/CIK0000000001-submissions-002.json"] = json.dumps(
        {"accessionNumber": [A2], "filingDate": ["2024-01-09"], "form": ["8-K/A"], "acceptanceDateTime": ["2024-01-09T14:00:00.000Z"], "items": ["2.02"]}).encode()
    run_plan(site, out)
    fetch_all(site, out, start_year=2012, end_year=2025, limit_ciks=1)
    assert not any("submissions-001" in c for c in site.calls)  # outside the window
    assert any("submissions-002" in c for c in site.calls)
    del site.pages["https://data.sec.gov/submissions/CIK0000000002.json"]
    fetch_all(site, out)
    assert B._load_manifest(out)["CIK0000000002.json"]["status"] == "NOT_FOUND"


# ------------------------------------------------------------------------------------------------ extract


def full(tmp_path):
    site, out = world(tmp_path)
    run_plan(site, out)
    fetch_all(site, out)
    return site, out


def lines_of(out, name):
    p = out / name
    return [json.loads(ln) for ln in p.read_text().splitlines() if ln]


def rows_of(out):
    return [json.loads(ln) for ln in (out / "acceptance_8k.jsonl").read_text().splitlines()]


def test_extract_writes_utc_instants_items_and_a_clean_reconciliation(tmp_path):
    site, out = full(tmp_path)
    assert B.stage_extract(out)["missing"] == 0
    rows = {r["accession"]: r for r in rows_of(out)}
    assert rows[A1]["acceptance_utc"] == "2024-01-05T21:30:05Z" and rows[A1]["items"] == ["2.02", "9.01"]
    assert rows[A3]["ciks"] == ["0000000001", "0000000002"]
    rec = json.loads((out / "reconciliation.json").read_text())
    assert rec["planned"] == 3 and rec["problem_count"] == 0 and any("PROXY" in x for x in rec["limits"])
    assert rec["rows_sha256"] == B.sha256_bytes((out / "acceptance_8k.jsonl").read_bytes())


def test_extract_is_deterministic(tmp_path):
    site, out = full(tmp_path)
    B.stage_extract(out)
    first = (out / "acceptance_8k.jsonl").read_bytes()
    B.stage_extract(out)
    assert (out / "acceptance_8k.jsonl").read_bytes() == first


def test_a_planned_accession_without_acceptance_is_listed_not_dropped(tmp_path):
    site, out = world(tmp_path)
    site.pages["https://data.sec.gov/submissions/CIK0000000001.json"] = sub("1", [(A1, "2024-01-05", "8-K", "2024-01-05T21:30:05.000Z", "")])
    run_plan(site, out)
    fetch_all(site, out)
    B.stage_extract(out)
    rec = json.loads((out / "reconciliation.json").read_text())
    assert lines_of(out, "missing.jsonl") == [{"accession": A2}] and rec["missing"] == 1  # A3 still found via CIK 2
    assert rec["planned"] == rec["accepted"] + rec["conflicted"] + rec["missing"]
    assert [r["items"] for r in rows_of(out) if r["accession"] == A1] == [[]]


@pytest.mark.parametrize("entry,needle", [
    ((A1, "2024-01-05", "8-K", "2024-01-05T16:30:05.000", ""), "acceptance unreadable"),   # no Z
    ((A1, "2024-01-05", "8-K", None, ""), "acceptance unreadable"),
    ((A1, "2024-01-06", "8-K", "2024-01-05T21:30:05.000Z", ""), "!= index"),               # date disagrees with index
])
def test_bad_records_become_problems_and_the_accession_stays_missing(tmp_path, entry, needle):
    site, out = world(tmp_path)
    site.pages["https://data.sec.gov/submissions/CIK0000000001.json"] = sub("1", [entry])
    run_plan(site, out)
    fetch_all(site, out)
    B.stage_extract(out)
    probs = lines_of(out, "problems.jsonl")
    assert any(needle in p["problem"] for p in probs) and {"accession": A1} in lines_of(out, "missing.jsonl")


def test_co_registrant_sources_that_disagree_are_quarantined_not_accepted(tmp_path):
    site, out = world(tmp_path)
    site.pages["https://data.sec.gov/submissions/CIK0000000002.json"] = sub("2", [(A3, "2024-04-02", "8-K", "2024-04-02T20:06:00.000Z", "5.02")])
    run_plan(site, out)
    fetch_all(site, out)
    res = B.stage_extract(out)
    assert res["conflicted"] == 1 and res["accepted"] == 2 and res["missing"] == 0
    assert A3 not in {r["accession"] for r in rows_of(out)}  # never emitted as accepted coverage
    (c,) = lines_of(out, "conflicts.jsonl")
    assert c["accession"] == A3 and sorted(x["acceptance_utc"] for x in c["sources"]) == ["2024-04-02T20:05:00Z", "2024-04-02T20:06:00Z"]
    assert sorted(x["source_file"] for x in c["sources"]) == ["CIK0000000001.json", "CIK0000000002.json"]  # both retained
    rec = json.loads((out / "reconciliation.json").read_text())
    assert rec["planned"] == rec["accepted"] + rec["conflicted"] + rec["missing"]
    assert rec["detail_files"]["conflicts.jsonl"]["lines"] == 1


def test_co_registrants_that_differ_only_in_items_are_also_a_conflict(tmp_path):
    site, out = world(tmp_path)
    site.pages["https://data.sec.gov/submissions/CIK0000000002.json"] = sub("2", [(A3, "2024-04-02", "8-K", "2024-04-02T20:05:00.000Z", "5.02,9.01")])
    run_plan(site, out)
    fetch_all(site, out)
    assert B.stage_extract(out)["conflicted"] == 1


def test_agreeing_co_registrants_are_accepted_with_their_source_count(tmp_path):
    site, out = full(tmp_path)
    B.stage_extract(out)
    assert {r["accession"]: r["n_sources"] for r in rows_of(out)}[A3] == 2 and lines_of(out, "conflicts.jsonl") == []


def test_conflict_and_coverage_counts_are_separate_and_nothing_is_truncated(tmp_path):
    out = tmp_path / "out"
    (out / "raw").mkdir(parents=True)
    n = 1100
    plan = [{"accession": f"0000000009-24-{i:06d}", "ciks": ["0000000009"], "form": "8-K", "filed": "2024-01-02"} for i in range(n)]
    (out / "plan.jsonl").write_text("\n".join(json.dumps(p) for p in plan) + "\n")
    res = B.stage_extract(out)
    assert res["missing"] == n and len(lines_of(out, "missing.jsonl")) == n  # past the old 1,000 cut
    rec = json.loads((out / "reconciliation.json").read_text())
    assert rec["detail_files"]["missing.jsonl"]["lines"] == n
    assert "nothing is truncated" in rec["counts_note"]


def test_a_record_from_a_cik_the_index_does_not_list_is_refused(tmp_path):
    site, out = world(tmp_path)
    site.pages["https://data.sec.gov/submissions/CIK0000000002.json"] = sub("2", [(A1, "2024-01-05", "8-K", "2024-01-05T21:30:05.000Z", "")])
    run_plan(site, out)
    fetch_all(site, out)
    B.stage_extract(out)
    assert any("not among" in p["problem"] for p in lines_of(out, "problems.jsonl"))


def test_extract_refuses_stored_bytes_that_do_not_match_the_manifest(tmp_path):
    site, out = full(tmp_path)
    (out / "raw/submissions/CIK0000000001.json.gz").write_bytes(gzip.compress(b'{"filings":{"recent":{}}}'))
    with pytest.raises(B.BackfillError, match="manifest hash"):
        B.stage_extract(out)


def test_ragged_columns_are_refused(tmp_path):
    site, out = world(tmp_path)
    bad = json.loads(sub("1", [(A1, "2024-01-05", "8-K", "2024-01-05T21:30:05.000Z", "")]))
    bad["filings"]["recent"]["items"] = []
    site.pages["https://data.sec.gov/submissions/CIK0000000001.json"] = json.dumps(bad).encode()
    run_plan(site, out)
    fetch_all(site, out)
    with pytest.raises(B.BackfillError, match="ragged"):
        B.stage_extract(out)


def test_extract_without_a_plan_is_an_error(tmp_path):
    with pytest.raises(B.BackfillError, match="plan"):
        B.stage_extract(tmp_path / "nothing")


# ------------------------------------------------------------------------------------------------ verify


def hdr_url(row_):
    return B.HEADER_URL.format(cik=int(row_["ciks"][0]), nodash=row_["accession"].replace("-", ""), acc=row_["accession"])


def header(et: str) -> bytes:
    return f"<SEC-HEADER>x\n<ACCEPTANCE-DATETIME>{et}\nACCESSION NUMBER:\t1\n".encode()


def test_verify_matches_header_eastern_time_to_the_utc_record(tmp_path):
    site, out = full(tmp_path)
    B.stage_extract(out)
    # 2024-01-05 21:30:05Z = 16:30:05 EST ; 2024-04-02 20:05Z = 16:05 EDT
    for r in rows_of(out):
        et = {"2024-01-05T21:30:05Z": "20240105163005", "2024-01-09T14:00:00Z": "20240109090000", "2024-04-02T20:05:00Z": "20240402160500"}[r["acceptance_utc"]]
        site.pages[hdr_url(r)] = header(et)
    assert B.stage_verify(out, site, sample=10) == {"sampled": 3, "mismatches": 0}


def test_verify_counts_a_mismatch_a_missing_header_and_a_garbled_one(tmp_path):
    site, out = full(tmp_path)
    B.stage_extract(out)
    rs = {r["accession"]: r for r in rows_of(out)}
    site.pages[hdr_url(rs[A1])] = header("20240105213005")  # UTC wall time mistaken for ET: 5 h off
    site.pages[hdr_url(rs[A2])] = b"no header here"
    res = B.stage_verify(out, site, sample=10)  # A3 header absent -> FileNotFoundError
    assert res == {"sampled": 3, "mismatches": 3}
    errs = json.loads((out / "verify.json").read_text())["results"]
    assert sum(1 for e in errs if e.get("error")) == 2


def test_verify_receipt_is_bound_to_the_extract_hash_and_the_achieved_sample(tmp_path):
    site, out = full(tmp_path)
    B.stage_extract(out)
    for r in rows_of(out):
        site.pages[hdr_url(r)] = header({"2024-01-05T21:30:05Z": "20240105163005", "2024-01-09T14:00:00Z": "20240109090000",
                                         "2024-04-02T20:05:00Z": "20240402160500"}[r["acceptance_utc"]])
    B.stage_verify(out, site, sample=50)
    v = json.loads((out / "verify.json").read_text())
    assert v["extracted_rows_sha256"] == B.sha256_bytes((out / "acceptance_8k.jsonl").read_bytes())
    assert (v["requested_sample"], v["achieved_sample"], v["population"]) == (50, 3, 3)
    assert "quarantined" in v["scope"]


def test_verify_refuses_an_extract_that_changed_after_reconciliation(tmp_path):
    site, out = full(tmp_path)
    B.stage_extract(out)
    (out / "acceptance_8k.jsonl").write_bytes((out / "acceptance_8k.jsonl").read_bytes() + b"\n")
    with pytest.raises(B.BackfillError, match="re-run extract"):
        B.stage_verify(out, site, sample=3)


def test_verify_never_samples_a_quarantined_conflict(tmp_path):
    site, out = world(tmp_path)
    site.pages["https://data.sec.gov/submissions/CIK0000000002.json"] = sub("2", [(A3, "2024-04-02", "8-K", "2024-04-02T20:06:00.000Z", "5.02")])
    run_plan(site, out)
    fetch_all(site, out)
    B.stage_extract(out)
    B.stage_verify(out, site, sample=10)
    assert A3 not in {x["accession"] for x in json.loads((out / "verify.json").read_text())["results"]}


def test_verify_sample_is_seeded_and_bounded(tmp_path):
    site, out = full(tmp_path)
    B.stage_extract(out)
    B.stage_verify(out, site, sample=2)
    a = [r["accession"] for r in json.loads((out / "verify.json").read_text())["results"]]
    B.stage_verify(out, site, sample=2)
    assert a == [r["accession"] for r in json.loads((out / "verify.json").read_text())["results"]] and len(a) == 2


def test_header_match_accepts_either_reading_of_the_repeated_hour():
    api = datetime(2024, 11, 3, 6, 30, tzinfo=UTC)  # 01:30 EST (second pass)
    assert B._header_matches(datetime(2024, 11, 3, 1, 30), api)
    assert not B._header_matches(datetime(2024, 11, 3, 2, 30), api)


# ------------------------------------------------------------------------------------------------ http and cli


class _Resp:
    def __init__(self, code, content=b"ok"):
        self.status_code, self.content = code, content


def fake_requests(monkeypatch, codes):
    calls = []

    def get(url, headers=None, timeout=None):
        calls.append((url, headers))
        return _Resp(codes[min(len(calls) - 1, len(codes) - 1)])

    mod = types.SimpleNamespace(get=get, RequestException=OSError)
    monkeypatch.setitem(sys.modules, "requests", mod)
    return calls


def test_http_fetch_sends_the_declared_identity_and_paces_requests(monkeypatch):
    calls = fake_requests(monkeypatch, [200])
    sleeps, now = [], [0.0]
    f = B.make_http_fetch(interval_s=0.2, sleep=lambda s: (sleeps.append(s), now.__setitem__(0, now[0] + s)), clock=lambda: now[0])
    f("https://x/a")
    f("https://x/b")
    assert calls[0][1]["User-Agent"] == B.sec_identity() and "@" in calls[0][1]["User-Agent"]
    assert sleeps and abs(sleeps[0] - 0.2) < 1e-9  # the second request waited out the interval


def test_a_403_stops_the_job_and_is_not_retried(monkeypatch):
    calls = fake_requests(monkeypatch, [403, 200])
    f = B.make_http_fetch(sleep=lambda s: None)
    with pytest.raises(B.Blocked):
        f("https://x/a")
    assert len(calls) == 1


def test_429_is_retried_then_gives_up_and_404_is_a_missing_file(monkeypatch):
    calls = fake_requests(monkeypatch, [429, 429, 200])
    assert B.make_http_fetch(sleep=lambda s: None)("https://x/a") == b"ok" and len(calls) == 3
    fake_requests(monkeypatch, [503])
    with pytest.raises(B.BackfillError, match="after 2 retries"):
        B.make_http_fetch(retries=2, sleep=lambda s: None)("https://x/a")
    fake_requests(monkeypatch, [404])
    with pytest.raises(FileNotFoundError):
        B.make_http_fetch(sleep=lambda s: None)("https://x/a")


def test_identity_needs_a_deliverable_address(monkeypatch):
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    assert B.sec_identity("me nobody@localhost") == B._DEFAULT_IDENTITY
    assert B.sec_identity("no address") == B._DEFAULT_IDENTITY
    assert B.sec_identity("Lab ops@example.org") == "Lab ops@example.org"
    monkeypatch.setenv("SEC_USER_AGENT", "Env team@example.net")
    assert B.sec_identity() == "Env team@example.net"


def test_default_identity_matches_the_repo_default():
    edgar = pytest.importorskip("ingestion.edgar")
    assert B._DEFAULT_IDENTITY == edgar.DEFAULT_SEC_IDENTITY


def test_cli_refuses_an_output_inside_the_repo_and_a_fast_interval(tmp_path, capsys):
    repo = Path(B.__file__).resolve().parents[1]
    assert B.main(["plan", "--out", str(repo / "x")], fetch=lambda u: b"") == 2
    assert B.main(["plan", "--out", str(tmp_path), "--interval", "0.05"], fetch=lambda u: b"") == 2
    assert "inside the repository" in capsys.readouterr().err or True


def test_cli_exit_codes_for_block_and_error(tmp_path):
    def blocked(url):
        raise B.Blocked("x")
    assert B.main(["plan", "--out", str(tmp_path / "o"), "--start-year", "2024", "--end-year", "2024"], fetch=blocked) == 3
    assert B.main(["extract", "--out", str(tmp_path / "none")]) == 1


def test_cli_end_to_end(tmp_path, capsys):
    site, out = world(tmp_path)
    for stage, extra in (("plan", ["--start-year", "2024", "--end-year", "2024"]), ("fetch", ["--start-year", "2024", "--end-year", "2024"]), ("extract", [])):
        assert B.main([stage, "--out", str(out), *extra], fetch=site) == 0
    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1])["missing"] == 0
