"""CURRENT pure/injected controls; do not exercise native filesystem fixtures."""
import copy
import ast
import types
import unittest
from unittest.mock import patch
if __package__:
    from . import test_api_implementation as impl
else:
    import test_api_implementation as impl
import api_isolation_bootstrap as bootstrap
import api_isolation_native_store as native
from api_bundle_contract import Refusal,json_hash


class BootstrapTests(unittest.TestCase):
    def binding(self): return impl.LauncherTests.state_binding()
    def arrange(self,fail=None):
        events=[]; app=types.SimpleNamespace(state=types.SimpleNamespace())
        def selection(binding):
            events.append('selection')
            if fail=='selection': raise ValueError('invalid selection')
            return []
        def configure(path):
            events.append('output')
            if fail=='output': raise ValueError('invalid output custody')
        modules={'workflows.loader':types.SimpleNamespace(read_api_selection=selection),
                 'api.routers.ollama':types.SimpleNamespace(configure_api_output_state=configure),
                 'api.main':types.SimpleNamespace(app=app)}
        def imports(name): events.append('import:'+name); return modules[name]
        def verify(binding,bundle): events.append('authority'); return fail!='authority'
        authority=types.SimpleNamespace(verify_state_handoff=verify)
        def serve(value):
            self.assertEqual(value.state.workflow_state_binding,self.binding()['workflow'])
            events.append('serve'); return 'SYNTHETIC'
        return events,app,authority,imports,serve

    def test_state_capabilities_constructed_before_application_and_serve(self):
        events,app,authority,imports,serve=self.arrange()
        result=bootstrap.serve_bound(self.binding(),'/immutable/source',authority,imports,serve)
        self.assertEqual(result,'SYNTHETIC')
        self.assertEqual(events,['authority','import:workflows.loader','import:api.routers.ollama','selection','output','import:api.main','serve'])

    def test_missing_or_bad_binding_has_no_import_or_legacy_fallback(self):
        candidates=[None,{},self.binding(),self.binding(),self.binding()]
        candidates[2]['workflow']['state_root']='/immutable/source/workflows'
        candidates[3]['output']['state_root']=candidates[3]['workflow']['state_root']
        candidates[4]['composition_receipt_sha256']='UNKNOWN'
        for binding in candidates:
            events,app,authority,imports,serve=self.arrange()
            with self.assertRaises(Refusal): bootstrap.serve_bound(binding,'/immutable/source',authority,imports,serve)
            self.assertEqual(events,[])

    def test_authority_selection_output_failures_never_import_main_or_serve(self):
        for failure in ('authority','selection','output'):
            events,app,authority,imports,serve=self.arrange(failure)
            with self.assertRaises((Refusal,ValueError)):
                bootstrap.serve_bound(self.binding(),'/immutable/source',authority,imports,serve)
            self.assertNotIn('import:api.main',events); self.assertNotIn('serve',events)

    def test_conflicting_existing_workflow_binding_refuses_traffic(self):
        events,app,authority,imports,serve=self.arrange()
        app.state.workflow_state_binding={'foreign':True}
        with self.assertRaises(Refusal): bootstrap.serve_bound(self.binding(),'/immutable/source',authority,imports,serve)
        self.assertNotIn('serve',events)

    def test_launcher_requires_binding_before_any_pin_or_exec(self):
        class K:
            def pin_current(self): raise AssertionError('must not pin')
        with self.assertRaises(Refusal): impl.launcher.launch(K(),'b'*40,{}, {'api':'built-in'})

    def test_rendered_program_uses_strict_handoff_and_missing_authority_fails_before_app(self):
        program=bootstrap.render_bound_program({'cwd':'/immutable/source','release_root':'/immutable','sys_path_exact':['/immutable/source']},self.binding())
        syntax=ast.parse(program)
        self.assertNotIn('api.main:app',program)
        self.assertIn('serve_bound(',program)
        imports=[n.module for n in ast.walk(syntax) if isinstance(n,ast.ImportFrom)]
        self.assertEqual(imports,['api_isolation_bootstrap','api_isolation_runtime_admission'])
        # Parse only: no exec, actual import, provider lookup or socket.


class Lease:
    def assert_held(self): pass


class MemoryStore:
    def __init__(self):
        self.value={'schema':'grid.api-service-history.v1','genesis':{'fixture':True},'entries':[]}
        self.writes=[]
    def read(self,name):
        if self.value is None: raise FileNotFoundError('synthetic missing history')
        return copy.deepcopy(self.value)
    def cas(self,name,old,new,lease):
        if self.value!=old: raise ValueError('synthetic CAS conflict')
        self.value=copy.deepcopy(new); self.writes.append(copy.deepcopy(new))


class Driver:
    def __init__(self): self.calls=[]
    def verify_never_issued(self,receipt): return receipt=={'fixture':True}
    def submit_once(self,operation): self.calls.append(operation); return 'unknown'
    def observe_terminal(self,operation): return None
    def verify_terminal(self,operation,receipt): return True  # shape still checked by broker


class BrokerTests(unittest.TestCase):
    def operation(self,nonce='one'):
        op={'plan':'a'*64,'fence':'b'*64,'host':'fixture','service':'grid-api.service',
            'target':'/synthetic','nonce':nonce,'manager_identity_sha256':'d'*64}
        op['operation_id']=json_hash(op); return op

    def test_durable_uncertain_before_submit_and_no_resend_after_reconstruction(self):
        store=MemoryStore(); driver=Driver(); broker=native.DurableBroker(store,driver); lease=Lease(); op=self.operation()
        broker.prepare(lease,op,None)
        with self.assertRaises(Refusal): broker.dispatch(('restart','grid-api.service'),lease,'b'*64,op)
        self.assertEqual(store.value['entries'][-1]['phase'],'DISPATCH_UNCERTAIN')
        reconstructed=native.DurableBroker(store,driver)
        with self.assertRaises(Refusal): reconstructed.dispatch(('restart','grid-api.service'),lease,'b'*64,op)
        self.assertEqual(len(driver.calls),1)

    def test_missing_history_and_unavailable_manager_refuse(self):
        store=MemoryStore(); broker=native.DurableBroker(store)
        with self.assertRaises(RuntimeError): broker.service_manager_identity({})
        store.value=None
        with self.assertRaises(FileNotFoundError): broker.read_service_operation(Lease())

    def test_corrupt_chain_duplicate_identity_and_unsafe_terminal_refuse(self):
        store=MemoryStore(); broker=native.DurableBroker(store,Driver()); lease=Lease(); op=self.operation()
        broker.prepare(lease,op,None)
        with self.assertRaises(Refusal): broker.prepare(lease,self.operation('two'),{'operation_id':op['operation_id']})
        self.assertEqual(len(store.value['entries']),1)
        store.value['entries'][0]['previous_sha256']='0'*64
        with self.assertRaises(Refusal): broker.history()

    def test_identity_substitution_refuses_before_any_history_write(self):
        store=MemoryStore(); broker=native.DurableBroker(store,Driver()); op=self.operation(); op['target']='/other'
        with self.assertRaises(Refusal): broker.prepare(Lease(),op,None)
        self.assertEqual(store.writes,[])


class AtomicStoreTests(unittest.TestCase):
    def directory(self):
        class D:
            fd=7
            def __init__(self): self.files={'executor-state.json':native.canonical({'epoch':0})}; self.events=[]
            def read(self,name,limit): return self.files[name]
            def write_new(self,name,raw):
                if name in self.files: raise FileExistsError(name)
                self.files[name]=raw; self.events.extend(['write-exclusive','file-fsync','directory-fsync'])
        return D()
    def test_cas_mismatch_refuses_before_any_write(self):
        d=self.directory(); store=native.AtomicStore(d)
        with self.assertRaises(Refusal): store.cas('executor-state.json',{'epoch':1},{'epoch':2},Lease())
        self.assertEqual(d.events,[])
    def test_durable_sequence_and_retained_temp_on_failed_replace(self):
        for fail in (False,True):
            d=self.directory(); store=native.AtomicStore(d)
            def replace(src,dst,**kwargs):
                d.events.append('replace')
                if fail: raise OSError('injected rename failure')
                d.files[dst]=d.files.pop(src)
            fake=types.SimpleNamespace(replace=replace,fsync=lambda fd:d.events.append('directory-fsync'))
            with patch.object(native,'os',fake):
                if fail:
                    with self.assertRaises(OSError): store.cas('executor-state.json',{'epoch':0},{'epoch':1},Lease())
                    self.assertEqual(store.read('executor-state.json'),{'epoch':0})
                    self.assertEqual(len(d.files),2)
                else:
                    store.cas('executor-state.json',{'epoch':0},{'epoch':1},Lease())
                    self.assertEqual(store.read('executor-state.json'),{'epoch':1})
                    self.assertEqual(d.events,['write-exclusive','file-fsync','directory-fsync','replace','directory-fsync'])
    def test_reserved_lock_name_cannot_be_replaced(self):
        d=self.directory()
        with self.assertRaises(Refusal): native.AtomicStore(d).cas('executor.lock',None,{},Lease())
        self.assertEqual(d.events,[])


if __name__=='__main__': unittest.main()
