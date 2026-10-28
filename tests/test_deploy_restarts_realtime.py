"""API-only releases cannot activate realtime/scheduler or claim fresh candles.

Legacy opt-in restart/ack/freshness steps belong to a separate activation scope.
No candle persistence, shutdown flushing, provider behavior or timer is changed.
Source routing and actual transaction/Adapter are exercised with inert ports.
"""
import copy
import pytest
if __package__:
    from . import test_deploy_restarts_hermes as support
else:
    import test_deploy_restarts_hermes as support
pytestmark=pytest.mark.unit
txn=support.txn


def scenario(state='active|enabled'):
    case=support.scenario();b,k,port,held,baseline,*_=case
    held['realtime']={'state':state,'pid':123,'cwd':'/held/realtime','source':'old-realtime','dropin':'old-dropin',
        'writer':'existing-writer','timer':'existing-timer','shutdown_flushes':0,'provider_requests':0,'candle_writes':0}
    held['scheduler']={'state':'active|enabled','source':'held-scheduler','timer':'unchanged'}
    baseline.clear();baseline.update(copy.deepcopy(held));b['protected_snapshot']=txn.digest(held)
    return case


def assert_no_activation_or_freshness_route(value):
    support.assert_api_only_routing(value)
    assert value['on']['workflow_dispatch']=={}
    runs='\n'.join(s.get('run','') for s in value['jobs']['deploy']['steps'])
    assert not any(text in runs for text in ('grid-realtime','grid-scheduler','realtime_activation_gate',
        'realtime_dropin_backup','realtime_freshness_probe','acknowledge_realtime_interruption','activate_realtime'))


def test_api_workflow_has_no_realtime_restart_optin_or_freshness_claim():
    assert_no_activation_or_freshness_route(support.workflow())


@pytest.mark.parametrize('mutation',['activation-input','activation-job','restart-command','freshness-probe'])
def test_reintroduced_activation_or_probe_route_is_rejected(mutation):
    value=support.workflow()
    if mutation=='activation-input':value['on']['workflow_dispatch']={'inputs':{'activate_realtime':{'default':'true'}}}
    elif mutation=='activation-job':value['jobs']['realtime']={'needs':['deploy'],'steps':[{'run':'sudo systemctl restart grid-realtime'}]}
    elif mutation=='restart-command':value['jobs']['deploy']['steps'][0]['run']+='\nsudo systemctl restart grid-realtime\n'
    else:value['jobs']['deploy']['steps'][0]['run']+='\npython scripts/realtime_freshness_probe.py NOW\n'
    with pytest.raises(AssertionError):assert_no_activation_or_freshness_route(value)


@pytest.mark.parametrize('state',['active|enabled','inactive|enabled','inactive|disabled','active|masked'])
def test_api_success_does_not_retarget_flush_restart_or_probe_realtime(state):
    case=scenario(state);txn.deploy(case[0],case[2]);support.assert_preserved(case)
    assert case[5]==[('restart','grid-api.service')]
    assert all(snapshot['realtime']==case[4]['realtime'] for snapshot in case[6])
    assert case[3]['scheduler']==case[4]['scheduler']
    # These are held-service observations, not a claim of zero API startup effects.


@pytest.mark.parametrize('field',['state','pid','cwd','source','dropin','writer','timer','shutdown_flushes','provider_requests','candle_writes'])
def test_realtime_identity_writer_or_candle_drift_prevents_promotion(field):
    case=scenario();b,k,port,held,*_=case;stage=k.copy_bundle_exclusive
    def drift(*args):stage(*args);held['realtime'][field]='EXTERNAL_CHANGE'
    k.copy_bundle_exclusive=drift
    with pytest.raises(txn.Refusal,match='protected drift'):txn.deploy(b,port)
    assert 'swap' not in k.trace and 'service' not in k.trace and case[5]==[]
    assert k.lock.locked() and k.record is not None and 'VERIFIED' not in k.trace


@pytest.mark.parametrize('service',['grid-realtime.service','grid-scheduler.service'])
def test_api_plan_cannot_be_reused_as_realtime_or_scheduler_activation(service):
    case=scenario();case[0]['service']=service
    with pytest.raises(txn.Refusal,match='API service only'):txn.deploy(case[0],case[2])
    assert case[1].trace==[];support.assert_preserved(case)


def test_api_failure_does_not_cascade_to_realtime_or_scheduler():
    case=scenario();b,k,port,*_=case
    def reject(p,target):raise RuntimeError('API observer rejected')
    k.verify_api_observation=reject
    with pytest.raises(RuntimeError):txn.deploy(b,port)
    assert case[5]==[('restart','grid-api.service')] and k.lock.locked()
    assert 'VERIFIED' not in k.trace and 'finish' not in k.trace;support.assert_preserved(case)


def test_unknown_realtime_protection_does_not_run_a_freshness_probe_as_fallback():
    case=scenario();b,k,port,*_=case
    def unknown(binding):raise RuntimeError('realtime observer unknown')
    k.observe_protected=unknown
    with pytest.raises(RuntimeError,match='observer unknown'):txn.deploy(b,port)
    assert k.trace==['preflight'] and case[5]==[];support.assert_preserved(case)
