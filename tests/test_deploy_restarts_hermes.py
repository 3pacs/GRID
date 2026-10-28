"""API-only deployment preserves Hermes; no service/shell/native execution.

The former suite required a shared-release Hermes restart/drop-in. This workflow
now deliberately deploys only an independent API bundle. Preserve the original
intent: never activate held services, reject uncertain/drifting protection, and
never claim a running target from liveness or a request acknowledgement alone.
"""
import ast
import copy
from pathlib import Path
import sys
import pytest
import yaml  # Deliberately required: no silent importorskip of integrity checks.
pytestmark=pytest.mark.unit

if __package__:
    from . import test_api_implementation as impl
else:
    import test_api_implementation as impl

ROOT=Path(__file__).resolve().parents[1]
txn=impl.txn


def workflow():
    return yaml.load((ROOT/'.github/workflows/deploy.yml').read_bytes(),Loader=yaml.BaseLoader)


def assert_api_only_routing(value):
    assert value['on']['push']['branches']==['main']
    assert 'workflow_dispatch' in value['on']
    assert value['concurrency']=={'group':'deploy-grid-release','cancel-in-progress':'false'}
    jobs=value['jobs'];assert set(jobs)=={'verify','bundle','deploy','dad-smoke'}
    assert jobs['bundle']['needs']==['verify'] and jobs['deploy']['needs']==['verify','bundle']
    for name in ('bundle','deploy'):
        assert not jobs[name].get('if') and not jobs[name].get('continue-on-error')
    critical=next(s for s in jobs['verify']['steps'] if s.get('name')=='Run critical tests')
    assert 'python -m pytest tests/test_api.py tests/test_pit.py' in critical['run']
    assert not critical.get('if') and not critical.get('continue-on-error')
    target=jobs['deploy'];assert target['runs-on']==['self-hosted','grid-svr','grid-api-isolated']
    assert len(target['steps'])==1
    step=target['steps'][0];assert 'uses' not in step and not step.get('if') and not step.get('continue-on-error')
    command=step['run']
    assert '/data/grid_v4/api-isolated/bootstrap/bin/python3.10 -I -S -B' in command
    assert '/data/grid_v4/api-isolated/bootstrap/deploy_api_isolated.py' in command
    assert '--expected-commit "$EXPECTED_COMMIT" --execute' in command
    assert step['env']['EXPECTED_COMMIT']=='${{ github.sha }}'
    assert not any(word in command for word in ('systemctl','sudo','grid_release','grid-hermes','grid-coordinator','daemon-reload','tee '))
    bundle=next(s for s in jobs['bundle']['steps'] if 'run' in s)
    assert 'bash scripts/deploy_build_hook.sh api-isolated "$EXPECTED_COMMIT" --execute' in bundle['run']
    assert bundle['env']['EXPECTED_COMMIT']=='${{ github.sha }}'
    assert jobs['dad-smoke']['needs']==['deploy'] and jobs['dad-smoke']['if']=="needs.deploy.result == 'success'"


def scenario(hermes='loaded|inactive|dead|enabled',coordinator='active|enabled|/held/coordinator'):
    """Explicit observer/manager stand-ins; actual transaction and Adapter bodies."""
    held={'hermes':{'state':hermes,'pid':0,'start':'retained','cwd':'/held/hermes','source_sha':'old',
                    'unit':'unit-bytes','dropin':'dropin-bytes','alias':'old-target','python':'interpreter-bytes','packages':'package-bytes'},
          'coordinator':{'state':coordinator,'cwd':'/held/coordinator','source_sha':'old-coordinator','unit':'coordinator-unit'},
          'diary':{'writer':'same','timer':'same','state':'same'}}
    baseline=copy.deepcopy(held);b=impl.binding();b['protected_snapshot']=txn.digest(held)
    k=impl.Kernel(b);calls=[];observations=[]
    def observe(binding):observations.append(copy.deepcopy(held));return txn.digest(held)
    k.observe_protected=observe
    def service(argv,lease,fence,operation):
        assert argv==('restart','grid-api.service')
        assert operation==k.operation and operation['service']=='grid-api.service'
        assert operation['target']==k.target and k.journal['state']=='API_SERVICE_REQUEST_INTENT'
        calls.append(tuple(argv));k.settle();k.trace.append('service');return 'request-accepted'
    port=impl.adapter.Adapter(k,service)
    return b,k,port,held,baseline,calls,observations


def assert_preserved(case):
    b,k,port,held,baseline,calls,observations=case
    assert held==baseline
    assert all(c==('restart','grid-api.service') for c in calls)
    assert not any(event in k.trace for event in ('dropin','daemon-reload','enable','start','timer','diary-write'))


def test_deploy_yml_is_valid_and_api_only_with_required_verification():
    assert_api_only_routing(workflow())


@pytest.mark.parametrize('mutation',['target-checkout','hermes-restart','coordinator-restart','lost-verification','cancel-owner','continue-error','skip-api-checks','smoke-always'])
def test_workflow_routing_corruptions_fail_the_actual_contract(mutation):
    value=workflow();jobs=value['jobs']
    if mutation=='target-checkout':jobs['deploy']['steps'].append({'uses':'actions/checkout@v7'})
    elif mutation in ('hermes-restart','coordinator-restart'):
        jobs['deploy']['steps'][0]['run']+='\nsudo systemctl restart grid-'+mutation.split('-')[0]+'\n'
    elif mutation=='lost-verification':jobs['deploy']['needs']=['bundle']
    elif mutation=='cancel-owner':value['concurrency']['cancel-in-progress']='true'
    elif mutation=='continue-error':jobs['deploy']['steps'][0]['continue-on-error']='true'
    elif mutation=='skip-api-checks':next(s for s in jobs['verify']['steps'] if s.get('name')=='Run critical tests')['if']='false'
    else:jobs['dad-smoke']['if']='always()'
    with pytest.raises(AssertionError):assert_api_only_routing(value)


@pytest.mark.parametrize('state',['loaded|active|running|enabled','loaded|inactive|dead|disabled','loaded|inactive|dead|enabled',
    'loaded|active|running|disabled','masked|inactive|dead|masked','masked|active|running|masked',
    'loaded|active|running|masked-runtime','loaded|inactive|dead|enabled-runtime','loaded|active|running|static'])
def test_held_or_active_hermes_is_never_repointed_or_restarted(state):
    case=scenario(hermes=state);b,k,port,*_=case
    txn.deploy(b,port)
    assert_preserved(case);assert case[5]==[('restart','grid-api.service')]
    assert 'verify-api' in k.trace and k.trace[-2:]==['VERIFIED','finish']
    assert all(snapshot['hermes']==case[4]['hermes'] for snapshot in case[6])


@pytest.mark.parametrize('observation',[None,'',{},'unknown','0'*64])
def test_uncertain_protection_never_reaches_claim_or_service(observation):
    case=scenario();b,k,port,*_=case;k.observe_protected=lambda binding:observation
    with pytest.raises(txn.Refusal,match='protected drift'):txn.deploy(b,port)
    assert k.trace==['preflight'];assert case[5]==[];assert_preserved(case)


@pytest.mark.parametrize('check',[1,2,3,4,5])
def test_late_stop_disable_or_observer_failure_never_claims_verified(check):
    for fault in ('stop','disable','query-failure'):
        case=scenario(hermes='loaded|active|running|enabled');b,k,port,held,baseline,calls,_=case;count=0
        def observe(binding):
            nonlocal count
            count+=1
            if count==check:
                if fault=='query-failure':raise RuntimeError('bounded observer failed')
                held['hermes']['state']='loaded|inactive|dead|enabled' if fault=='stop' else 'loaded|active|running|disabled'
            return txn.digest(held)
        k.observe_protected=observe
        with pytest.raises((txn.Refusal,RuntimeError)):txn.deploy(b,port)
        assert 'VERIFIED' not in k.trace and 'finish' not in k.trace
        assert calls==([('restart','grid-api.service')] if check==5 else [])
        assert held['hermes']['unit']==baseline['hermes']['unit'] and held['hermes']['dropin']==baseline['hermes']['dropin']
        assert held['diary']==baseline['diary'] and held['coordinator']==baseline['coordinator']
        if check>1:assert k.lock.locked() and k.record is not None


@pytest.mark.parametrize('field',['pid','start','cwd','source_sha','unit','dropin','alias','python','packages'])
def test_each_held_hermes_identity_change_blocks_promotion(field):
    case=scenario();b,k,port,held,*_=case;stage=k.copy_bundle_exclusive
    def change(*args):stage(*args);held['hermes'][field]='CHANGED_BY_EXTERNAL_ACTOR'
    k.copy_bundle_exclusive=change
    with pytest.raises(txn.Refusal,match='protected drift'):txn.deploy(b,port)
    assert 'swap' not in k.trace and not case[5] and k.lock.locked()


@pytest.mark.parametrize('service',['grid-hermes.service','grid-coordinator.service','grid-diary.service'])
def test_non_api_service_binding_refuses_before_preflight(service):
    case=scenario();case[0]['service']=service
    with pytest.raises(txn.Refusal,match='API service only'):txn.deploy(case[0],case[2])
    assert case[1].trace==[] and case[5]==[];assert_preserved(case)


@pytest.mark.parametrize('fault',['request-failure','ack-without-terminal','missing-ledger','stale-terminal'])
def test_api_request_failure_or_uncertainty_retains_evidence_and_never_restarts_hermes(fault):
    case=scenario();b,k,port,held,baseline,calls,_=case
    def service(argv,lease,fence,operation):
        calls.append(tuple(argv));k.trace.append('service')
        if fault=='missing-ledger':k.operation=None
        if fault=='stale-terminal':k.settle();k.terminal['operation_id']='0'*64
        return 'unknown' if fault=='request-failure' else 'request-accepted'
    port.service=service
    with pytest.raises(txn.Refusal):txn.deploy(b,port)
    assert calls==[('restart','grid-api.service')] and 'verify-api' not in k.trace
    assert 'VERIFIED' not in k.trace and 'finish' not in k.trace and k.lock.locked()
    assert k.record is not None and k.journal['state']=='API_SERVICE_REQUEST_INTENT'
    assert_preserved(case)


@pytest.mark.parametrize('fault',['pid','start','cwd','sha','interpreter','health'])
def test_untrusted_api_observation_cannot_complete_or_claim_hermes_tree(fault):
    case=scenario();b,k,port,*_=case
    def refuse(p,target):k.trace.append('observe-api:'+fault);raise RuntimeError('API observer rejected '+fault)
    k.verify_api_observation=refuse
    with pytest.raises(RuntimeError,match='API observer rejected'):txn.deploy(b,port)
    assert 'VERIFIED' not in k.trace and 'finish' not in k.trace and k.lock.locked()
    assert k.target==txn.plan(b)['candidate'];assert_preserved(case)
    # This proves observer refusal propagates, not native PID/health correctness.


@pytest.mark.parametrize('entry,argv',[('deploy_api_isolated',['--binding','MUST_NOT_READ','--expected-commit','b'*40,'--execute']),
    ('deploy_api_bundle',['--expected-commit','b'*40,'--execute']),('deploy_api_launcher',['--execute']),
    ('deploy_api_rollback',['--binding','MUST_NOT_READ','--approval','MUST_NOT_READ','--execute'])])
def test_manual_or_push_execution_cannot_bypass_unadopted_guards(monkeypatch,entry,argv):
    modules={'deploy_api_isolated':txn,'deploy_api_bundle':impl.builder,'deploy_api_launcher':impl.launcher,'deploy_api_rollback':impl.rollback}
    monkeypatch.setattr(sys,'argv',[entry,*argv])
    def refuse_open(*args,**kwargs):raise AssertionError('execute guard read input')
    monkeypatch.setattr('builtins.open',refuse_open)
    with pytest.raises(SystemExit) as outcome:modules[entry].main()
    assert outcome.value.code==78


def test_causal_removal_of_protected_recheck_exposes_forbidden_promotion():
    def setup():
        case=scenario();b,k,port,held,*_=case;stage=k.copy_bundle_exclusive
        def drift(*args):stage(*args);held['hermes']['dropin']='EXTERNAL_CHANGE'
        k.copy_bundle_exclusive=drift;return case
    green=setup()
    with pytest.raises(txn.Refusal):txn.deploy(green[0],green[2])
    assert 'swap' not in green[1].trace
    syntax=ast.parse((ROOT/'scripts/deploy_api_isolated.py').read_bytes())
    function=next(n for n in syntax.body if isinstance(n,ast.FunctionDef) and n.name=='deploy')
    # Remove only the post-stage protective observation from the actual body.
    checks=[n for n in function.body if isinstance(n,ast.Expr) and isinstance(n.value,ast.Call) and isinstance(n.value.func,ast.Name) and n.value.func.id=='unchanged']
    assert len(checks)==5;function.body.remove(checks[2])
    namespace=dict(vars(txn));exec(compile(ast.Module(body=[function],type_ignores=[]),'inert-protection-mutant','exec'),namespace)
    red=setup()
    with pytest.raises(txn.Refusal):namespace['deploy'](red[0],red[2])
    assert 'swap' in red[1].trace and not red[5]  # Later guard detects it, too late to prevent promotion.


def test_causal_terminal_guard_removal_exposes_ack_only_false_success():
    def setup():
        case=scenario();b,k,port,*_=case
        def ack(argv,lease,fence,operation):case[5].append(tuple(argv));return 'request-accepted'
        port.service=ack;return case
    green=setup()
    with pytest.raises(txn.Refusal):txn.deploy(green[0],green[2])
    assert 'finish' not in green[1].trace
    red=setup();original=red[2]._service_settled
    red[2]._service_settled=lambda p:original(p) if red[1].operation is None else None
    txn.deploy(red[0],red[2])
    assert red[1].terminal is None and 'VERIFIED' in red[1].trace and 'finish' in red[1].trace
