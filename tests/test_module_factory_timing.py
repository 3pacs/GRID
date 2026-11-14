from analysis import module_factory as m
import pytest

def bars(px=100,n=390):
    return {m._hm(570+i):(px,px+1,px-1,px,1000) for i in range(n)}

def spec(**kw):
    return m.RunFile.from_dict(dict(run_id='synthetic-only',first_session='2023-01-03',last_session='2023-01-05',decision_times=['14:00'],horizons=['60m'],features=['F1'],z_thresholds=[1.0],target_unit='dollars',perms=99,z_history=20,**kw))

def rows(a,b):
    return m.build_rows({'2023-01-03':m.Session(a),'2023-01-04':m.Session(b)},spec())[0]['SPY|change|14:00+60m']

def test_entry_is_after_signal_close():
    a,b=bars(),bars()
    b['13:59']=(100,100,100,100,1000)
    b['14:01']=(105,105,105,105,1000)
    b['14:59']=(106,106,106,106,1000)
    r=rows(a,b)[0]
    assert r['target_dollars']==1
    assert r['entry_at'] > r['decision_at']
    assert r['entry_price_basis']=='next_minute_open_proxy'

@pytest.mark.parametrize('missing',['13:59','14:01','14:59'])
def test_missing_exact_minute_never_filled_from_stale_bar(missing):
    a,b=bars(),bars()
    del b[missing]
    assert rows(a,b)==[]

@pytest.mark.parametrize("n", [1, 210, 389])
def test_incomplete_prior_never_substitutes_older_day(monkeypatch,n):
    seen=[]
    original=m.raw_features
    def spy(day,prior,bar):
        if day is s["2023-01-05"]:
            seen.append(prior.last_close)
        return original(day,prior,bar)
    monkeypatch.setattr(m,'raw_features',spy)
    s={'2023-01-03':m.Session(bars(100)),'2023-01-04':m.Session(bars(120,n)),'2023-01-05':m.Session(bars(121))}
    m.build_rows(s,spec())
    assert seen==[]

def test_horizon_must_extend_beyond_delayed_entry():
    body=dict(run_id='x',first_session='2023-01-03',last_session='2023-01-05',decision_times=['14:00'],horizons=['1m'],features=['F1'],z_thresholds=[1.0])
    with pytest.raises(m.RunFileError):
        m.RunFile.from_dict(body)
