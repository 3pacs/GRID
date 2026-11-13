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


def test_post_exit_gaps_do_not_change_decision_or_label():
    a, complete = bars(), bars()
    incomplete = {hm: b for hm, b in complete.items() if hm < "15:00"}
    assert rows(a, complete) == rows(a, incomplete)
    assert len(rows(a, incomplete)) == 1


def test_missing_predecision_bar_withholds_even_if_daily_count_passes():
    a, b = bars(), bars()
    del b["10:17"]
    assert rows(a, b) == []


def test_missing_future_entry_does_not_change_feature_history(monkeypatch):
    seen = []
    original = m.robust_z
    def capture(value, history):
        seen.append((value, tuple(history)))
        return original(value, history)
    monkeypatch.setattr(m, "robust_z", capture)
    s = {"2023-01-03": m.Session(bars()), "2023-01-04": m.Session(bars(101))}
    m.build_rows(s, spec())
    expected = list(seen)
    seen.clear()
    missing_entry = bars(101)
    del missing_entry["14:01"]
    s["2023-01-04"] = m.Session(missing_entry)
    m.build_rows(s, spec())
    assert seen == expected and seen


def test_coverage_labels_calendar_as_unqualified():
    _, coverage = m.build_rows({"2023-01-03":m.Session(bars()),
                                "2023-01-04":m.Session(bars())}, spec())
    assert coverage["calendar_qualification"].startswith("UNQUALIFIED")
    assert coverage["full_bars_role"].startswith("retrospective")
