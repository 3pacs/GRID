"""Daily capture regressions; providers and persistence are inert memory fixtures."""
from __future__ import annotations
import copy,json,os,sys,types,unittest
from pathlib import Path
from datetime import date,datetime,timedelta,timezone
from unittest.mock import patch

# Standalone capsule runner: dependency shims are explicit, never installed.
if os.environ.get('GRID_YF_OFFLINE_SOURCE'):
    root=Path(os.environ['GRID_YF_OFFLINE_SOURCE'])
    sys.path.insert(0,str(root))
    sys.path.append('/usr/lib/python3/dist-packages')
    fake=types.ModuleType('yfinance')
    def forbidden_download(*args,**kwargs):
        raise AssertionError('provider must be replaced by a memory frame')
    fake.download=forbidden_download
    sys.modules['yfinance']=fake
    loguru=types.ModuleType('loguru')
    class Logger:
        def __getattr__(self,name):return lambda *a,**k:None
    loguru.logger=Logger();sys.modules['loguru']=loguru
    ranges=types.ModuleType('ingestion.sanity_ranges');ranges.get_range_for_series=lambda *a:None;ranges.MAX_PCT_CHANGE=100
    sys.modules['ingestion.sanity_ranges']=ranges
import pandas as pd
from ingestion import yfinance_pull as yf
import price_close_contract as contract
from store import observations

UTC=timezone.utc
DAY=date(2026,10,15)
EARLY=datetime(2026,10,15,13,33,tzinfo=UTC)
LATE=datetime(2026,10,16,5,30,tzinfo=UTC)
NY_CLASSES=[(contract.US_LISTED_DAILY_TICKERS,'us_listed_ny_label_day_2026'),
            (contract.CBOE_VOL_INDEX_DAILY_TICKERS,'cboe_vol_index_ny_label_day_2026'),
            (contract.ICE_DXY_DAILY_TICKERS,'ice_dxy_ny_label_day_2026'),
            (contract.CME_GLOBEX_DAILY_TICKERS,'cme_globex_ny_label_day_2026'),
            (contract.YAHOO_FX_DAILY_TICKERS,'yahoo_fx_ny_close_label_day_2026')]
EXTENDED_CLASSES=NY_CLASSES[1:]
class Result:
    def __init__(self,rows=()):self.rows=list(rows)
    def fetchall(self):return self.rows
    def fetchone(self):return self.rows[0] if self.rows else None
class MemoryDB:
    def __init__(self):self.rows=[];self.calls=[];self.now=EARLY
    def begin(self):return self
    def connect(self):return self
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def execute(self,stmt,params=None):
        sql=str(stmt);p=params or {};self.calls.append((sql,copy.deepcopy(p)))
        if 'pg_advisory_xact_lock' in sql:return Result()
        if 'SELECT raw_payload, pull_timestamp, value' in sql:
            rows=[r for r in self.rows if r['sid']==p['sid'] and r['src']==p['src'] and r['od']==p['od'] and r['payload'] and r['payload'].get('interval')==p['interval']]
            rows.sort(key=lambda r:r['pulled_at'],reverse='ORDER BY pull_timestamp DESC' in sql)
            return Result([(r['payload'],r['pulled_at'],r['val']) for r in rows[:1]])
        if 'SELECT raw_payload, pull_timestamp FROM' in sql:
            marker=json.loads(p['payload'])
            return Result([(r['payload'],r['pulled_at']) for r in self.rows if r['sid']==p['sid'] and r['src']==p['src'] and r['od']==p['od'] and r['payload'] and all(r['payload'].get(k)==v for k,v in marker.items()) and r['pulled_at']>=p['period_end']])
        if 'SELECT id FROM source_catalog' in sql:return Result([(2,)])
        if 'INSERT INTO raw_series' in sql:
            self.rows.append({'sid':p['sid'],'src':p['src'],'od':p['od'],'val':p['val'],'pulled_at':p.get('pulled_at',self.now),'payload':json.loads(p['payload']) if p.get('payload') else None})
            return Result()
        rows=[r for r in self.rows if r['sid']==p.get('sid')]
        if 'SELECT DISTINCT obs_date' in sql:
            return Result([(r['od'],) for r in rows if (not p.get('start_date') or r['od']>=p['start_date']) and (not p.get('end_date') or r['od']<=p['end_date'])])
        if 'SELECT obs_date, value' in sql:
            return Result([(r['od'],r['val']) for r in rows if p['start_date']<=r['od']<=p['end_date']])
        if 'SELECT 1 FROM raw_series' in sql:
            marker=json.loads(p['payload'])
            return Result([(1,)] if any(r['od']==p['od'] and r['payload'] and all(r['payload'].get(k)==v for k,v in marker.items()) and r['pulled_at']>=p['period_end'] for r in rows) else [])
        if 'SELECT r.obs_date' in sql:
            rows=[r for r in rows if (not p.get('start') or r['od']>=p['start']) and (not p.get('as_of') or r['od']<=p['as_of']) and (not p.get('as_of_ts') or r['pulled_at']<=p['as_of_ts'])]
            rows.sort(key=lambda r:(r['od'],r['pulled_at']),reverse=True)
            if 'obs_date ASC' in sql:rows.sort(key=lambda r:r['od'])
            if 'LIMIT 1' in sql:rows=rows[:1]
            return Result([(r['od'],r['val'],r['pulled_at'],'yfinance',r['payload']) if 'r.raw_payload' in sql else (r['od'],r['val'],r['pulled_at'],'yfinance') for r in rows])
        raise AssertionError('unmodeled SQL: '+sql)

def pull(db,ticker='^GSPC',day=DAY,now=LATE,value=685.,interval='1d',fields=None,frame=None):
    db.now=now
    if frame is None:frame=pd.DataFrame({'Close':[value]},index=pd.DatetimeIndex([pd.Timestamp(day)]))
    with patch.object(yf,'_utc_now',return_value=now),patch.object(yf.yf,'download',return_value=frame):
        return yf.YFinancePuller(db).pull_ticker(ticker,start_date=day,interval=interval,only_fields=fields)

class FinalityTests(unittest.TestCase):
    def test_current_session_raw_then_later_qualified_capture(self):
        db=MemoryDB();first=pull(db,now=EARLY,value=680.);original=copy.deepcopy(db.rows)
        self.assertEqual(first['rows_inserted'],1);self.assertEqual(first['qualified_rows_inserted'],0)
        self.assertEqual(first['status'],'SUCCESS');self.assertEqual(original[0]['payload']['capture_state'],'INCOMPLETE')
        self.assertEqual(observations.read_daily_policy_n(db,'YF:^GSPC:close',2,start=DAY,as_of_ts=EARLY),[])
        second=pull(db,now=LATE,value=685.)
        self.assertEqual(second['rows_inserted'],1);self.assertEqual(second['qualified_rows_inserted'],1)
        self.assertEqual(db.rows[0],original[0]);self.assertEqual([r['val'] for r in db.rows],[680.,685.])
        self.assertEqual(observations.read_daily_policy_n(db,'YF:^GSPC:close',2,start=DAY,as_of_ts=LATE-timedelta(microseconds=1)),[])
        self.assertEqual([o.value for o in observations.read_daily_policy_n(db,'YF:^GSPC:close',2,start=DAY,as_of_ts=LATE)],[685.])

class PolicyTests(unittest.TestCase):
    def test_default_universe_is_partitioned_without_suffix_inference(self):
        supported=contract.NY_LABEL_DAY_DAILY_TICKERS|contract.UTC_DAILY_TICKERS
        self.assertEqual(len(yf.YF_TICKER_LIST),70)
        self.assertEqual(set(yf.YF_TICKER_LIST),supported|contract.UNKNOWN_DAILY_TICKERS)
        self.assertFalse(supported & contract.UNKNOWN_DAILY_TICKERS)
        # Deliberate 2026-10-19 owner decision: every default has a convention.
        self.assertEqual(contract.UNKNOWN_DAILY_TICKERS,frozenset())
        self.assertFalse(contract.NY_LABEL_DAY_DAILY_TICKERS & contract.UTC_DAILY_TICKERS)
        classes=[tickers for tickers,_ in NY_CLASSES]
        self.assertEqual(sum(len(c) for c in classes),len(contract.NY_LABEL_DAY_DAILY_TICKERS))
        self.assertEqual(frozenset().union(*classes),contract.NY_LABEL_DAY_DAILY_TICKERS)
        self.assertEqual(len(contract.NY_LABEL_DAY_DAILY_TICKERS-contract.US_LISTED_DAILY_TICKERS),16)
        self.assertIsNone(contract.daily_capture_boundary("AAPL",DAY))

    def test_dst_and_exact_offset_boundaries(self):
        for day,end in [(date(2026,3,6),datetime(2026,3,7,5,tzinfo=UTC)),
                        (date(2026,3,9),datetime(2026,3,10,4,tzinfo=UTC)),
                        (date(2026,11,2),datetime(2026,11,3,5,tzinfo=UTC))]:
            with self.subTest(day=day):
                self.assertEqual(contract.daily_capture_boundary("^GSPC",day),end)
                self.assertIsNone(contract.daily_capture_payload("^GSPC","close",day,end-timedelta(microseconds=1)))
                self.assertIsNotNone(contract.daily_capture_payload("^GSPC","close",day,end))
                self.assertIsNotNone(contract.daily_capture_payload("^GSPC","close",day,end.astimezone(timezone(timedelta(hours=5,minutes=30)))))
        self.assertIsNone(contract.daily_capture_payload("^GSPC","close",DAY,LATE.replace(tzinfo=None)))

    def test_weekend_holiday_and_early_close_are_conservative(self):
        for day in [date(2026,7,3),date(2026,12,25),date(2026,10,17),date(2026,3,8),date(2025,10,15)]:
            self.assertIsNone(contract.daily_capture_boundary("^GSPC",day))
        early=date(2026,11,27)
        self.assertIsNone(contract.daily_capture_payload("^GSPC","close",early,datetime(2026,11,27,22,tzinfo=UTC)))
        self.assertIsNotNone(contract.daily_capture_payload("^GSPC","close",early,datetime(2026,11,28,5,tzinfo=UTC)))

    def test_crypto_utc_day_including_weekends_and_all_six_fields(self):
        day=date(2026,10,17);end=contract.observation_end_utc(day)
        self.assertEqual(contract.daily_capture_boundary("BTC-USD",day),end)
        for field in contract.DAILY_FIELDS:
            self.assertIsNone(contract.daily_capture_payload("ETH-USD",field,day,end-timedelta(microseconds=1)))
            self.assertIsNotNone(contract.daily_capture_payload("ETH-USD",field,day,end))

    def test_unsupported_tickers_and_intervals_refuse_all_fields(self):
        for ticker in contract.UNKNOWN_DAILY_TICKERS|{"AAPL","NEW-USD","OTHER=X","ES=F","^VIX1D","DX-Y",""}:
            for field in contract.DAILY_FIELDS:
                self.assertIsNone(contract.daily_capture_payload(ticker,field,DAY,LATE))
        for interval in ['1wk','1mo','1h']:
            self.assertIsNone(contract.daily_capture_payload("^GSPC","close",DAY,LATE,interval))
            self.assertIsNone(contract.daily_capture_payload("^VIX","close",DAY,LATE,interval))
            db=MemoryDB();r=pull(db,interval=interval)
            self.assertEqual(r['outcome'],'inserted');self.assertEqual(r['qualified_rows_inserted'],0)
            self.assertEqual(db.rows[0]['payload']['capture_state'],'UNKNOWN')
            self.assertEqual(db.rows[0]['sid'],f'YF:^GSPC:close:{interval}')
            self.assertEqual(observations.read_daily_policy_n(db,'YF:^GSPC:close',1,start=DAY),[])

    def test_marker_rejects_incorrect_provenance(self):
        p=contract.daily_capture_payload('^GSPC','close',DAY,LATE)
        self.assertTrue(contract.is_daily_policy_capture(p,'YF:^GSPC:close',DAY,LATE))
        for key,value in [('interval','1wk'),('price_basis','YF:QQQ:close'),('obs_date','2026-10-14'),('provider_certified_final',True),('provider_certified_final',0),('eligible_after','2000-01-01'),('capture_state','UNKNOWN'),('capture_state','INCOMPLETE')]:
            with self.subTest(key=key,value=value):
                bad=dict(p);bad[key]=value
                self.assertFalse(contract.is_daily_policy_capture(bad,'YF:^GSPC:close',DAY,LATE))
        self.assertFalse(contract.is_daily_policy_capture(p,'YF:^GSPC:close',DAY,EARLY))

    def test_spy_exact_contract_unchanged(self):
        end=contract.observation_end_utc(DAY)
        self.assertIsNone(contract.capture_payload(DAY,end-timedelta(microseconds=1)))
        self.assertEqual(contract.capture_payload(DAY,end),{
            'price_contract_version':'spy_close_v1','capture_policy':'post_utc_day_end_v1',
            'price_basis':'YF:SPY:close','interval':'1d','obs_date':'2026-10-15','provider_certified_final':False})
        db=MemoryDB();early=pull(db,'SPY',now=end-timedelta(microseconds=1))
        self.assertEqual(early['rows_inserted'],0);self.assertEqual(early['status'],'PARTIAL');self.assertEqual(early['outcome'],'no_data')
        self.assertEqual(pull(db,'SPY',now=end)['rows_inserted'],1)
        self.assertEqual(pull(db,'SPY',now=end+timedelta(hours=1))['rows_inserted'],0)

class ExtendedSessionClassTests(unittest.TestCase):
    """VIX/DXY/futures/FX share the elapsed NY-label-day boundary, each with its own label."""
    def test_class_membership_is_exact(self):
        self.assertEqual(contract.CBOE_VOL_INDEX_DAILY_TICKERS,{"^VIX","^VIX9D","^VIX3M","^VIX6M"})
        self.assertEqual(contract.ICE_DXY_DAILY_TICKERS,{"DX-Y.NYB"})
        self.assertEqual(contract.CME_GLOBEX_DAILY_TICKERS,{"HG=F","GC=F","SI=F","CL=F"})
        self.assertEqual(contract.YAHOO_FX_DAILY_TICKERS,{"EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCHF=X","USDCAD=X","NZDUSD=X"})

    def test_each_class_eligible_after_boundary_incomplete_before(self):
        for day,end in [(DAY,datetime(2026,10,16,4,tzinfo=UTC)),(date(2026,11,2),datetime(2026,11,3,5,tzinfo=UTC))]:
            for tickers,convention in EXTENDED_CLASSES:
                for ticker in tickers:
                    with self.subTest(ticker=ticker,day=day):
                        self.assertEqual(contract.daily_capture_boundary(ticker,day),end)
                        for field in contract.DAILY_FIELDS:
                            before=end-timedelta(microseconds=1)
                            self.assertIsNone(contract.daily_capture_payload(ticker,field,day,before))
                            raw=contract.raw_capture_payload(ticker,field,day,before,'1d')
                            self.assertEqual((raw['capture_state'],raw['capture_reason']),('INCOMPLETE','supported_label_day_not_elapsed'))
                            self.assertFalse(contract.is_daily_policy_capture(raw,f'YF:{ticker}:{field}',day,before))
                            p=contract.daily_capture_payload(ticker,field,day,end)
                            self.assertEqual(p,{
                                'price_contract_version':'yf_daily_capture_v1','capture_policy':'post_label_day_end_v1',
                                'price_basis':f'YF:{ticker}:{field}','interval':'1d','obs_date':day.isoformat(),
                                'session_convention':convention,'eligible_after':end.isoformat(),
                                'provider_certified_final':False})
                            raw=contract.raw_capture_payload(ticker,field,day,end,'1d')
                            self.assertEqual(raw['capture_state'],'POLICY_ELIGIBLE')

    def test_nyse_closed_labels_and_other_years_stay_unknown(self):
        # Good Friday, MLK, July 3 observed, Thanksgiving, Christmas, Saturday,
        # Sunday: FX/futures may trade, but NYSE-closed labels never qualify.
        days=[date(2026,4,3),date(2026,1,19),date(2026,7,3),date(2026,11,26),date(2026,12,25),
              date(2026,10,17),date(2026,10,18),date(2025,10,15),date(2027,1,4)]
        for tickers,_ in EXTENDED_CLASSES:
            for ticker in tickers:
                for day in days:
                    with self.subTest(ticker=ticker,day=day):
                        later=datetime.combine(day+timedelta(days=3),datetime.min.time(),UTC)
                        self.assertIsNone(contract.daily_capture_boundary(ticker,day))
                        self.assertIsNone(contract.daily_capture_payload(ticker,'close',day,later))
                        raw=contract.raw_capture_payload(ticker,'close',day,later,'1d')
                        self.assertEqual((raw['capture_state'],raw['capture_reason']),('UNKNOWN','daily_session_policy_unknown'))
                        self.assertIs(raw['provider_certified_final'],False)

    def test_marker_round_trip_and_cross_class_provenance_rejected(self):
        for tickers,convention in EXTENDED_CLASSES:
            for ticker in tickers:
                sid=f'YF:{ticker}:close'
                with self.subTest(ticker=ticker):
                    p=contract.daily_capture_payload(ticker,'close',DAY,LATE)
                    self.assertTrue(contract.is_daily_policy_capture(p,sid,DAY,LATE))
                    raw=contract.raw_capture_payload(ticker,'close',DAY,LATE,'1d')
                    self.assertTrue(contract.is_daily_policy_capture(json.loads(json.dumps(raw)),sid,DAY,LATE))
                    for key,value in [('session_convention','us_listed_ny_label_day_2026'),('session_convention','utc_24h'),
                                      ('price_basis','YF:^GSPC:close'),('interval','1wk'),('obs_date','2026-10-14'),
                                      ('eligible_after',contract.observation_end_utc(DAY).isoformat()),
                                      ('provider_certified_final',True),('capture_state','UNKNOWN'),('capture_state','INCOMPLETE')]:
                        bad=dict(p);bad[key]=value
                        self.assertFalse(contract.is_daily_policy_capture(bad,sid,DAY,LATE),(key,value))
                    self.assertFalse(contract.is_daily_policy_capture(p,sid,DAY,EARLY))
                    self.assertFalse(contract.is_daily_policy_capture(p,'YF:^GSPC:close',DAY,LATE))
                    self.assertFalse(contract.is_daily_policy_capture(p,f'{sid}:1wk',DAY,LATE))

    def test_puller_raw_incomplete_then_qualified_and_reader_selects_it(self):
        for tickers,convention in EXTENDED_CLASSES:
            for ticker in tickers:
                with self.subTest(ticker=ticker):
                    sid=f'YF:{ticker}:close';db=MemoryDB()
                    first=pull(db,ticker,now=EARLY,value=20.)
                    self.assertEqual((first['rows_inserted'],first['qualified_rows_inserted']),(1,0))
                    self.assertEqual(db.rows[0]['payload']['capture_state'],'INCOMPLETE')
                    self.assertEqual(observations.read_daily_policy_n(db,sid,1,start=DAY),[])
                    second=pull(db,ticker,value=21.)
                    self.assertEqual((second['rows_inserted'],second['qualified_rows_inserted']),(1,1))
                    self.assertEqual(db.rows[1]['payload']['session_convention'],convention)
                    self.assertEqual(observations.read_daily_policy_n(db,sid,1,start=DAY,as_of_ts=LATE-timedelta(microseconds=1)),[])
                    self.assertEqual([o.value for o in observations.read_daily_policy_n(db,sid,1,start=DAY)],[21.])
                    weekly=pull(db,ticker,interval='1wk',now=LATE+timedelta(seconds=1),value=22.)
                    self.assertEqual(weekly['qualified_rows_inserted'],0)
                    self.assertEqual(db.rows[-1]['sid'],f'{sid}:1wk')
                    self.assertEqual([o.value for o in observations.read_daily_policy_n(db,sid,1,start=DAY)],[21.])

class PersistenceTests(unittest.TestCase):
    def test_append_preserves_unmarked_history_and_idempotency(self):
        db=MemoryDB();old={'sid':'YF:^GSPC:close','src':2,'od':DAY,'val':680.,'pulled_at':EARLY,'payload':None}
        db.rows.append(copy.deepcopy(old));r=pull(db)
        self.assertEqual(r['rows_inserted'],1);self.assertEqual(db.rows[0],old)
        self.assertEqual(db.rows[1]['val'],685.);self.assertEqual(db.rows[1]['pulled_at'],LATE)
        second=pull(db,now=LATE+timedelta(hours=1),value=686.)
        self.assertEqual(second['rows_inserted'],0);self.assertEqual(second['outcome'],'duplicate_only')
        self.assertEqual(len(db.rows),2)
        sql=[s for s,_ in db.calls];self.assertTrue(any('pg_advisory_xact_lock' in s for s in sql))
        self.assertFalse(any(s.lstrip().startswith(('UPDATE','DELETE')) for s in sql))

    def test_six_fields_are_marked_only_after_boundary(self):
        frame=pd.DataFrame({key:[685.] for key in yf._FIELD_MAP},index=pd.DatetimeIndex([pd.Timestamp(DAY)]))
        db=MemoryDB();early=pull(db,now=EARLY,frame=frame)
        self.assertEqual(early['rows_inserted'],6);self.assertEqual(early['qualified_rows_inserted'],0)
        self.assertEqual(early['status'],'SUCCESS');self.assertEqual(early['outcome'],'inserted')
        self.assertEqual({r['payload']['capture_state'] for r in db.rows},{'INCOMPLETE'})
        late=pull(db,frame=frame);self.assertEqual(late['rows_inserted'],6);self.assertEqual(late['qualified_rows_inserted'],6)
        self.assertEqual({r['sid'].split(':')[-1] for r in db.rows},set(contract.DAILY_FIELDS))
        for row in db.rows[:6]:self.assertFalse(contract.is_daily_policy_capture(row['payload'],row['sid'],DAY,row['pulled_at']))
        for row in db.rows[6:]:self.assertTrue(contract.is_daily_policy_capture(row['payload'],row['sid'],DAY,LATE))

    def test_supported_default_groups_execute_actual_puller(self):
        for ticker in contract.NY_LABEL_DAY_DAILY_TICKERS|contract.UTC_DAILY_TICKERS:
            db=MemoryDB();r=pull(db,ticker)
            self.assertEqual(r['rows_inserted'],1,(ticker,r));self.assertEqual(r['qualified_rows_inserted'],1,(ticker,r))
        for ticker in contract.UNKNOWN_DAILY_TICKERS|{'AAPL'}:
            db=MemoryDB();r=pull(db,ticker)
            self.assertEqual(r['rows_inserted'],1,(ticker,r));self.assertEqual(r['qualified_rows_inserted'],0)
            self.assertEqual(db.rows[0]['payload']['capture_state'],'UNKNOWN')
            self.assertEqual(observations.read_daily_policy_n(db,f'YF:{ticker}:close',1,start=DAY),[])

    def test_selected_reader_pit_and_raw_history_remain_distinct(self):
        db=MemoryDB();db.rows.append({'sid':'YF:^GSPC:close','src':2,'od':DAY,'val':680.,'pulled_at':EARLY,'payload':None})
        before=copy.deepcopy(db.rows);pull(db)
        raw=observations.read_latest_n(db,'YF:^GSPC:close',2,source='yfinance',as_of_ts=EARLY)
        self.assertEqual([o.value for o in raw],[680.])
        kwargs=dict(start=DAY,as_of=DAY)
        selected=lambda ts:observations.read_daily_policy_n(db,'YF:^GSPC:close',2,as_of_ts=ts,**kwargs)
        self.assertEqual(selected(LATE-timedelta(microseconds=1)),[])
        self.assertEqual([o.value for o in selected(LATE)],[685.])
        self.assertEqual(db.rows[0],before[0])

    def test_filter_before_dedup_and_limit(self):
        db=MemoryDB();pull(db)
        db.rows.append({'sid':'YF:^GSPC:close','src':2,'od':DAY,'val':999.,'pulled_at':LATE+timedelta(hours=1),'payload':{'interval':'1wk'}})
        chosen=observations.read_daily_policy_n(db,'YF:^GSPC:close',1,start=DAY)
        self.assertEqual([o.value for o in chosen],[685.])
        self.assertEqual([o.value for o in observations.read_latest_n(db,'YF:^GSPC:close',1,source='yfinance')],[999.])

    def test_diary_selects_qualified_vix_pair_after_policy_extension(self):
        from intelligence import market_diary
        db=MemoryDB();prior=DAY-timedelta(days=1)
        # EARLY is after the prior label's NY midnight but inside DAY itself.
        self.assertEqual(pull(db,'^VIX',day=prior,now=EARLY,value=17.)['qualified_rows_inserted'],1)
        self.assertEqual(pull(db,'^VIX',now=EARLY,value=18.)['qualified_rows_inserted'],0)
        self.assertIsNone(market_diary._fresh_pair(db,'YF:^VIX:close',DAY))
        pull(db,'^VIX',value=18.5)
        pair=market_diary._fresh_pair(db,'YF:^VIX:close',DAY)
        self.assertIsNotNone(pair);self.assertEqual([o.value for o in pair],[18.5,17.])

    def test_diary_refuses_unmarked_current_day_and_selects_qualified_pair(self):
        from intelligence import market_diary
        db=MemoryDB();prior=DAY-timedelta(days=1)
        for d,value in [(prior,679.),(DAY,680.)]:
            db.rows.append({'sid':'YF:^GSPC:close','src':2,'od':d,'val':value,'pulled_at':EARLY,'payload':None})
        self.assertIsNone(market_diary._fresh_pair(db,'YF:^GSPC:close',DAY))
        pull(db,day=prior,value=681.);pull(db,value=685.)
        pair=market_diary._fresh_pair(db,'YF:^GSPC:close',DAY)
        self.assertIsNotNone(pair);self.assertEqual([o.value for o in pair],[685.,681.])

    def test_instrument_adjustment_and_bounded_guards_stay_active(self):
        db=MemoryDB();frame=pd.DataFrame({('Close','QQQ'):[685.]},index=pd.DatetimeIndex([pd.Timestamp(DAY)]))
        self.assertEqual(pull(db,frame=frame)['outcome'],'error');self.assertEqual(db.rows,[])
        db=MemoryDB();frame=pd.DataFrame([[680.,685.]],columns=['Close','Close'],index=pd.DatetimeIndex([pd.Timestamp(DAY)]))
        pull(db,frame=frame);self.assertEqual(db.rows,[])
        db=MemoryDB()
        with patch.object(yf.yf,'download',return_value=pd.DataFrame({'Close':[685.]},index=pd.DatetimeIndex([pd.Timestamp(DAY)]))) as download:
            with patch.object(yf,'_utc_now',return_value=LATE):
                yf.YFinancePuller(db).pull_ticker('^GSPC',start_date=DAY,end_date=DAY+timedelta(days=1),only_fields=frozenset({'close'}))
        self.assertIs(download.call_args.kwargs['auto_adjust'],False)
        for sql,p in db.calls:
            if 'SELECT DISTINCT obs_date' in sql:
                self.assertEqual(p['start_date'],DAY);self.assertEqual(p['end_date'],DAY)

class RawCompatibilityTests(unittest.TestCase):
    def test_all_default_six_fields_and_arbitrary_ticker_raw_ingestion(self):
        frame=pd.DataFrame({key:[685.] for key in yf._FIELD_MAP},index=pd.DatetimeIndex([pd.Timestamp(DAY)]))
        for ticker in set(yf.YF_TICKER_LIST)|{'AAPL','AAA','BBB'}:
            with self.subTest(ticker=ticker):
                db=MemoryDB();r=pull(db,ticker,frame=frame)
                self.assertEqual(r['rows_inserted'],6);self.assertEqual(r['raw_rows_inserted'],6)
                self.assertEqual(len(r['series_id_map']),6)
                for row in db.rows:
                    self.assertIs(row['payload']['provider_certified_final'],False)
                    if ticker in contract.UNKNOWN_DAILY_TICKERS or ticker in {'AAPL','AAA','BBB'}:
                        self.assertEqual(row['payload']['capture_state'],'UNKNOWN')
                        self.assertFalse(contract.is_daily_policy_capture(row['payload'],row['sid'],DAY,LATE))
                    elif row['sid']!=contract.SPY_CLOSE_SERIES:
                        self.assertEqual(row['payload']['capture_state'],'POLICY_ELIGIBLE')
                        self.assertTrue(contract.is_daily_policy_capture(row['payload'],row['sid'],DAY,LATE))

    def test_future_year_and_non_session_raw_available_qualification_unknown(self):
        from intelligence import market_diary
        for day,now in [(date(2027,1,4),datetime(2027,1,5,6,tzinfo=UTC)),
                        (date(2026,10,17),datetime(2026,10,18,6,tzinfo=UTC)),
                        (date(2026,7,3),datetime(2026,7,4,6,tzinfo=UTC))]:
            with self.subTest(day=day):
                db=MemoryDB();r=pull(db,day=day,now=now)
                self.assertEqual(r['rows_inserted'],1);self.assertEqual(r['qualified_rows_inserted'],0)
                self.assertEqual(db.rows[0]['payload']['capture_state'],'UNKNOWN')
                self.assertEqual([o.value for o in observations.read_latest_n(db,'YF:^GSPC:close',1,source='yfinance')],[685.])
                self.assertEqual(observations.read_daily_policy_n(db,'YF:^GSPC:close',1,start=day),[])
                self.assertIsNone(market_diary._fresh_pair(db,'YF:^GSPC:close',day))

    def test_unknown_latest_vintage_A_B_A_and_exact_rerun(self):
        db=MemoryDB()
        for offset,value in enumerate([100.,101.,100.]):
            r=pull(db,'AAPL',value=value,now=LATE+timedelta(seconds=offset))
            self.assertEqual(r['rows_inserted'],1);self.assertEqual(r['qualified_rows_inserted'],0)
        self.assertEqual([r['val'] for r in db.rows],[100.,101.,100.])
        before=copy.deepcopy(db.rows)
        r=pull(db,'AAPL',value=100.,now=LATE+timedelta(seconds=3))
        self.assertEqual(r['rows_inserted'],0);self.assertEqual(r['outcome'],'duplicate_only')
        self.assertEqual(db.rows,before)
        for offset,value in enumerate([100.,101.,100.]):
            got=observations.read_latest_n(db,'YF:AAPL:close',1,source='yfinance',as_of_ts=LATE+timedelta(seconds=offset))
            self.assertEqual([o.value for o in got],[value])

    def test_same_value_raw_promotes_once_then_qualified_value_is_stable(self):
        db=MemoryDB();r=pull(db,now=EARLY,value=685.)
        self.assertEqual(r['rows_inserted'],1);self.assertEqual(r['qualified_rows_inserted'],0)
        raw=copy.deepcopy(db.rows[0]);r=pull(db,now=LATE,value=685.)
        self.assertEqual(r['rows_inserted'],1);self.assertEqual(r['qualified_rows_inserted'],1)
        self.assertEqual(db.rows[0],raw)
        for offset,value in [(1,685.),(2,686.),(3,685.)]:
            r=pull(db,now=LATE+timedelta(seconds=offset),value=value)
            self.assertEqual(r['rows_inserted'],0);self.assertEqual(r['qualified_rows_inserted'],0)
        self.assertEqual(len(db.rows),2)

    def test_interval_ids_requests_and_dedup_are_isolated(self):
        db=MemoryDB()
        for offset,interval in enumerate(['1d','1wk','1mo']):
            r=pull(db,interval=interval,now=LATE+timedelta(seconds=offset))
            expected='YF:^GSPC:close'+('' if interval=='1d' else ':'+interval)
            self.assertEqual(r['series_id_map'],{'close':expected})
            self.assertEqual(r['rows_inserted'],1)
            self.assertEqual(r['qualified_rows_inserted'],1 if interval=='1d' else 0)
        self.assertEqual({r['sid'] for r in db.rows},{'YF:^GSPC:close','YF:^GSPC:close:1wk','YF:^GSPC:close:1mo'})
        for interval in ['1wk','1mo']:
            r=pull(db,interval=interval,now=LATE+timedelta(seconds=5))
            self.assertEqual(r['rows_inserted'],0)
            db2=MemoryDB();frame=pd.DataFrame({'Close':[685.]},index=pd.DatetimeIndex([pd.Timestamp(DAY)]))
            with patch.object(yf.yf,'download',return_value=frame) as download,patch.object(yf,'_utc_now',return_value=LATE):
                yf.YFinancePuller(db2).pull_ticker('^GSPC',DAY,interval=interval)
            self.assertEqual(download.call_args.kwargs['interval'],interval)
            self.assertIs(download.call_args.kwargs['auto_adjust'],False)
        self.assertEqual([o.value for o in observations.read_daily_policy_n(db,'YF:^GSPC:close',2,start=DAY)],[685.])

    def test_unknown_or_incomplete_cannot_override_valid_marker_provenance(self):
        for sid,payload in [('YF:^GSPC:close',contract.daily_capture_payload('^GSPC','close',DAY,LATE)),
                            ('YF:SPY:close',contract.capture_payload(DAY,LATE))]:
            for state in ['UNKNOWN','INCOMPLETE']:
                bad=dict(payload,capture_state=state)
                self.assertFalse(contract.is_daily_policy_capture(bad,sid,DAY,LATE))
                db=MemoryDB();db.rows.append({'sid':sid,'src':2,'od':DAY,'val':685.,'pulled_at':LATE,'payload':bad})
                self.assertEqual(observations.read_daily_policy_n(db,sid,1,start=DAY),[])
            self.assertTrue(contract.is_daily_policy_capture(payload,sid,DAY,LATE))

if __name__=='__main__':
    unittest.main(argv=[sys.argv[0]]+sys.argv[1:],verbosity=2)
