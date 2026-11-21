"""Mocked runner integration: no request, DB call or checkpoint file write."""
from datetime import datetime, timezone
import json
from unittest.mock import MagicMock

import pytest

from ingestion.altdata import sec_xbrl_financials as sec
from ingestion.altdata.sec_financial_snapshots import NORMALIZER_VERSION
from store import sec_financial_snapshots as store


def response(value=100):
    data=dict(cik=1045810,entityName='NVIDIA',facts={'us-gaap':{'Revenues':{'units':{'USD':[
        dict(start='2026-04-27',end='2026-07-26',val=value,form='10-Q',fp='Q2',
             accn='0001045810-26-000075',filed='2026-08-26')
    ]}}}})
    now=datetime.now(timezone.utc)
    return json.dumps(data).encode(),now,now


@pytest.fixture
def runner(monkeypatch):
    monkeypatch.setattr(sec,'_sector_map_tickers',lambda:['NVDA'])
    monkeypatch.setattr(sec,'_load_fx_rates',lambda _: {'USD':1})
    monkeypatch.setattr(sec,'_fetch_ticker_to_cik_map',lambda:{'NVDA':'0001045810'})
    monkeypatch.setattr(sec,'_load_checkpoint',lambda:{})
    monkeypatch.setattr(sec.time,'sleep',lambda _:None)
    checkpoints=[]
    monkeypatch.setattr(sec,'_save_checkpoint',lambda *args:checkpoints.append(args))
    fetch=MagicMock(return_value=response())
    write=MagicMock(return_value=dict(status='applied',rows_written=1,snapshot_id='synthetic',rows_removed=0))
    monkeypatch.setattr(sec,'_fetch_company_facts_response',fetch)
    monkeypatch.setattr(store,'write_snapshot',write)
    return sec.SECXBRLFinancialsPuller(MagicMock()),fetch,write,checkpoints


def test_runner_only_checkpoints_successfully_admitted_capture(runner):
    puller,fetch,write,checkpoints=runner
    result=puller.pull_all()
    assert result[0]['status']=='SUCCESS'
    assert len(checkpoints)==1 and checkpoints[0][0]==['NVDA']
    snapshot=write.call_args.args[1]
    assert snapshot.rows[0]['amount_usd']==100
    assert result[-1]['tickers_processed']==1


@pytest.mark.parametrize('failure',['fetch','malformed','write','regressed'])
def test_failures_never_invalidate_or_checkpoint_as_complete(runner,failure):
    puller,fetch,write,checkpoints=runner
    if failure=='fetch':
        fetch.return_value=None
    elif failure=='malformed':
        now=datetime.now(timezone.utc)
        fetch.return_value=(b'{}',now,now)
    elif failure=='write':
        write.side_effect=RuntimeError('Synthetic writer failure')
    else:
        write.return_value=dict(status='rejected_revision_regression',rows_written=0)
    result=puller.pull_all()
    assert result[0]['status']=='FAILED'
    assert len(checkpoints)==1
    assert checkpoints[0][0]==[] and checkpoints[0][3]==['NVDA']
    assert result[-1]['tickers_processed']==1
    if failure in ('fetch','malformed'):
        write.assert_not_called()


def test_completed_sweep_starts_new_revision_check(runner,monkeypatch):
    puller,fetch,write,checkpoints=runner
    monkeypatch.setattr(sec,'_load_checkpoint',lambda:dict(normalizer_version=NORMALIZER_VERSION,completed=['NVDA']))
    assert puller.pull_all()[0]['status']=='SUCCESS'
    fetch.assert_called_once()


def test_checkpoint_from_older_normalizer_does_not_suppress_new_parser(runner,monkeypatch):
    puller,fetch,write,checkpoints=runner
    monkeypatch.setattr(sec,'_load_checkpoint',lambda:dict(normalizer_version='old',completed=['NVDA']))
    assert puller.pull_all()[0]['status']=='SUCCESS'
    fetch.assert_called_once()


def test_current_incomplete_sweep_resumes_without_refetching_done_ticker(runner,monkeypatch):
    puller,fetch,write,checkpoints=runner
    monkeypatch.setattr(sec,'_sector_map_tickers',lambda:['NVDA','AAPL'])
    monkeypatch.setattr(sec,'_fetch_ticker_to_cik_map',lambda:{'NVDA':'0001045810','AAPL':'0000320193'})
    monkeypatch.setattr(sec,'_load_checkpoint',lambda:dict(normalizer_version=NORMALIZER_VERSION,completed=['NVDA']))
    fetch.return_value=None
    result=puller.pull_all(limit=1)
    fetch.assert_called_once_with('0000320193')
    assert result[0]['ticker']=='AAPL'


def test_shared_shares_json_reader_remains_compatible(monkeypatch):
    raw,start,end=response()
    monkeypatch.setattr(sec,'_fetch_company_facts_response',lambda _:(raw,start,end))
    assert sec._fetch_company_facts('0001045810')['cik']==1045810


def test_fetch_preserves_bytes_and_records_actual_boundaries(monkeypatch):
    raw,_,_=response()
    monkeypatch.setattr(sec.requests,'get',lambda *a,**k:MagicMock(status_code=200,content=raw))
    body,start,end=sec._fetch_company_facts_response('0001045810')
    assert body is raw and start<=end
    assert start.tzinfo is not None


def test_persistent_failure_does_not_starve_other_issuers_or_next_sweep(runner,monkeypatch):
    puller,fetch,write,checkpoints=runner
    monkeypatch.setattr(sec,'_sector_map_tickers',lambda:['AAPL','NVDA'])
    monkeypatch.setattr(sec,'_fetch_ticker_to_cik_map',lambda:{'AAPL':'0000320193','NVDA':'0001045810'})
    fetch.side_effect=lambda cik: None if cik=='0000320193' else response()
    checkpoint={}
    monkeypatch.setattr(sec,'_load_checkpoint',lambda:checkpoint.copy())
    def save(completed,last_ticker,total_rows,attempted):
        checkpoint.update(normalizer_version=NORMALIZER_VERSION,completed=completed,
                          attempted=attempted,total_rows=total_rows)
    monkeypatch.setattr(sec,'_save_checkpoint',save)
    # A failed first ticker cannot monopolize limit=1 resumed invocations.
    assert puller.pull_all(limit=1)[0]['ticker']=='AAPL'
    assert checkpoint['completed']==[] and checkpoint['attempted']==['AAPL']
    assert puller.pull_all(limit=1)[0]['ticker']=='NVDA'
    assert checkpoint['completed']==['NVDA']
    # A complete attempt sweep restarts, including both failed and healthy issuers.
    assert puller.pull_all(limit=1)[0]['ticker']=='AAPL'
    assert puller.pull_all(limit=1)[0]['ticker']=='NVDA'
    assert write.call_count==2 and fetch.call_count==4


def test_no_cik_is_attempted_not_successful(runner,monkeypatch):
    puller,fetch,write,checkpoints=runner
    monkeypatch.setattr(sec,'_fetch_ticker_to_cik_map',lambda:{'AAPL':'0000320193'})
    assert puller.pull_all()[0]['status']=='NO_CIK'
    assert checkpoints[0][0]==[] and checkpoints[0][3]==['NVDA']
    fetch.assert_not_called()
    write.assert_not_called()
