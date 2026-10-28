"""API-only deployment preserves coordinator/diary ownership and failure guards.

An API release no longer claims to deploy or restart coordinator code. Tests use
the actual transaction/Adapter with inert capabilities; no shell/service calls.
"""
import copy
import pytest
pytestmark=pytest.mark.unit
if __package__:
    from . import test_deploy_restarts_hermes as support
else:
    import test_deploy_restarts_hermes as support
txn=support.txn


def test_coordinator_has_no_restart_dropin_or_health_probe_in_api_workflow():
    value=support.workflow();support.assert_api_only_routing(value)
    runs='\n'.join(s.get('run','') for s in value['jobs']['deploy']['steps'])
    assert 'grid-coordinator' not in runs and ':8100' not in runs and 'diary' not in runs


@pytest.mark.parametrize('state',['active|enabled|{DEPLOY}','inactive|enabled|{DEPLOY}','active|disabled|{DEPLOY}',
    'failed|enabled|{DEPLOY}','active|enabled|/home/grid/grid_v4/grid_repo','active|enabled|','active|masked|{DEPLOY}'])
def test_coordinator_is_preserved_even_if_previously_restart_eligible(state):
    case=support.scenario(coordinator=state);txn.deploy(case[0],case[2]);support.assert_preserved(case)
    assert case[5]==[('restart','grid-api.service')]
    assert all(item['coordinator']==case[4]['coordinator'] for item in case[6])


@pytest.mark.parametrize('unit,field',[('coordinator','state'),('coordinator','cwd'),('coordinator','source_sha'),
    ('coordinator','unit'),('diary','writer'),('diary','timer'),('diary','state')])
def test_coordinator_or_diary_drift_stops_before_any_api_request(unit,field):
    case=support.scenario();b,k,port,held,*_=case;stage=k.copy_bundle_exclusive
    def change(*args):stage(*args);held[unit][field]='EXTERNAL_DRIFT'
    k.copy_bundle_exclusive=change
    with pytest.raises(txn.Refusal,match='protected drift'):txn.deploy(b,port)
    assert 'swap' not in k.trace and case[5]==[] and k.lock.locked()


def test_unknown_coordinator_observer_status_refuses_before_owned_changes():
    case=support.scenario();b,k,port,*_=case
    def unknown(binding):raise RuntimeError('coordinator observation unknown')
    k.observe_protected=unknown
    with pytest.raises(RuntimeError,match='observation unknown'):txn.deploy(b,port)
    assert k.trace==['preflight'];support.assert_preserved(case)


def test_api_observation_and_terminal_receipt_precede_completion_without_coordinator_claim():
    case=support.scenario();b,k,port,*_=case;txn.deploy(b,port)
    assert k.trace.index('stage-verify')<k.trace.index('swap')<k.trace.index('service')<k.trace.index('verify-api')<k.trace.index('VERIFIED')<k.trace.index('finish')
    assert k.terminal['operation_id']==k.operations[0]['operation_id']
    assert k.terminal['target']==txn.plan(b)['candidate'] and k.terminal['phase']=='terminal'
    support.assert_preserved(case)


def test_uncertain_api_request_cannot_be_retried_after_executor_crash():
    case=support.scenario();b,k,port,*_=case
    def unknown(argv,lease,fence,operation):case[5].append(tuple(argv));return 'unknown'
    port.service=unknown
    with pytest.raises(txn.Refusal):txn.deploy(b,port)
    approval={'plan_sha256':txn.plan(b)['plan_sha256'],'journal_sha256':txn.digest(k.journal),
              'lock_record_sha256':txn.digest(k.record),'owner_receipt':'f'*64,
              'compatibility_receipt':b['evidence']['rollback_compatibility']}
    target=k.target;count=len(k.operations);k.crash()
    recovery=support.impl.adapter.Adapter(k,unknown)
    with pytest.raises(txn.Refusal):txn.recover(b,recovery,approval)
    assert k.target==target and len(k.operations)==count==1 and len(case[5])==1
    assert 'finish' not in k.trace;support.assert_preserved(case)


def test_valid_code_rollback_still_preserves_coordinator_and_diary():
    case=support.scenario();b,k,port,*_=case;service=port.service
    def fail_health(p,target):raise RuntimeError('API observation rejected')
    k.verify_api_observation=fail_health
    with pytest.raises(RuntimeError):txn.deploy(b,port)
    approval={'plan_sha256':txn.plan(b)['plan_sha256'],'journal_sha256':txn.digest(k.journal),
              'lock_record_sha256':txn.digest(k.record),'owner_receipt':'f'*64,
              'compatibility_receipt':b['evidence']['rollback_compatibility']}
    k.crash();k.verify_api_observation=lambda p,target:k.trace.append('verify-api')
    txn.recover(b,support.impl.adapter.Adapter(k,service),approval)
    assert k.target==b['old_release'] and k.trace[-2]=='CODE_ROLLED_BACK_DATA_UNCHANGED_BY_TOOL'
    assert case[5]==[('restart','grid-api.service'),('restart','grid-api.service')]
    support.assert_preserved(case)
