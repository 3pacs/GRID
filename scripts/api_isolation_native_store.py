"""Real durable JSON/CAS, executor epochs, and operation history; native UNRUN.

All filesystem capabilities refer to admitted private directory descriptors.
Manager/receipt authority is injected; the default manager refuses every action.
"""
from copy import deepcopy
import hashlib
import json
import os
import uuid
from api_isolation_posix import canonical, Lease
from api_bundle_contract import require, json_hash


class AtomicStore:
    NAMES=frozenset(('executor-state.json','journal.json','terminal.json','service-history.json'))
    def __init__(self,directory): self.directory=directory
    def read(self,name):
        require(name in self.NAMES,'reserved store name')
        raw=self.directory.read(name,16*1024*1024)
        value=json.loads(raw)
        require(canonical(value)==raw,'noncanonical durable record')
        return value
    def optional(self,name):
        try: return self.read(name)
        except FileNotFoundError: return None
    def cas(self,name,old,new,lease):
        require(name in self.NAMES,'reserved store name'); lease.assert_held()
        require(self.optional(name)==old,'durable record CAS changed')
        temp='.record-'+uuid.uuid4().hex
        self.directory.write_new(temp,canonical(new))
        lease.assert_held()
        os.replace(temp,name,src_dir_fd=self.directory.fd,dst_dir_fd=self.directory.fd)
        os.fsync(self.directory.fd)


class UnavailableManager:
    """No target systemd commands, cancellation, polling or guessed request IDs."""
    def __getattr__(self,name):
        def refuse(*args,**kwargs):
            raise RuntimeError('NOT_ADOPTED: trusted native manager/broker authority unavailable: '+name)
        return refuse


class DurableBroker:
    """Native persisted operation ledger; external authoritative manager driver.

History schema has a separately admitted genesis receipt and append-only entry
prefix. Empty local files are NOT manager proof. Driver.verify_never_issued must
check authoritative CURRENT namespace history under custody, not replay genesis.
observe/verify methods never submit a request. A prepared operation cannot be
resent by this broker, even when its acknowledgement record is missing.
"""
    def __init__(self,store,driver=None):
        self.store=store; self.driver=driver if driver is not None else UnavailableManager()

    def history(self):
        history=self.store.read('service-history.json')
        require(history.get('schema')=='grid.api-service-history.v1' and
                isinstance(history.get('entries'),list),'service history schema')
        previous=json_hash(history['genesis'])
        ids=set()
        for entry in history['entries']:
            require(entry['previous_sha256']==previous,'service history chain')
            require(entry['phase'] in ('PREPARED','DISPATCH_UNCERTAIN'),'service history phase')
            operation=entry['operation']
            require(operation['operation_id']==json_hash({k:v for k,v in operation.items() if k!='operation_id'}),
                    'service operation identity')
            require(operation['operation_id'] not in ids,'replayed operation ID')
            ids.add(operation['operation_id']); previous=json_hash(entry)
        return history

    def read_service_operation(self,lease):
        lease.assert_held(); h=self.history()
        if not h['entries']: return {'state':'NEVER_ISSUED','receipt':deepcopy(h['genesis'])}
        return {'state':'ISSUED','operation':deepcopy(h['entries'][-1]['operation'])}

    def verify_never_issued_service_receipt(self,receipt):
        h=self.history()
        require(not h['entries'] and h['genesis']==receipt,'history is not never-issued')
        return self.driver.verify_never_issued(receipt)

    def service_manager_identity(self,p): return self.driver.identity(p)
    def observe_terminal_service_operation(self,lease,operation):
        lease.assert_held()
        require(self.read_service_operation(lease)=={'state':'ISSUED','operation':operation},'current operation changed')
        return self.driver.observe_terminal(operation)  # read-only; unknown stays unknown
    def verify_terminal_service_receipt(self,operation,receipt):
        return self.driver.verify_terminal(operation,receipt)

    def prepare(self,lease,operation,terminal):
        lease.assert_held(); old=self.history(); new=deepcopy(old)
        if old['entries']:
            previous_operation=old['entries'][-1]['operation']
            require(isinstance(terminal,dict) and terminal.get('operation_id')==previous_operation['operation_id'],
                    'previous operation has no terminal binding')
            require(terminal.get('phase')=='terminal' and terminal.get('outcome') in ('succeeded','failed','cancelled') and
                    terminal.get('no_further_effects') is True,'previous operation is not terminal')
            require(all(terminal.get(k)==previous_operation[k] for k in ('operation_id','manager_identity_sha256','host','service','target','plan')),
                    'previous terminal identity mismatch')
            require(self.driver.verify_terminal(previous_operation,terminal) is True,'untrusted previous terminal')
        else:
            require(terminal is None and self.verify_never_issued_service_receipt(old['genesis']) is True,'unproved fresh history')
        require(all(e['operation']['operation_id']!=operation['operation_id'] for e in old['entries']),'duplicate operation')
        require(operation['operation_id']==json_hash({k:v for k,v in operation.items() if k!='operation_id'}),'new operation identity')
        previous=json_hash(old['entries'][-1]) if old['entries'] else json_hash(old['genesis'])
        new['entries'].append({'previous_sha256':previous,'operation':deepcopy(operation),
                               'previous_terminal':deepcopy(terminal),'phase':'PREPARED'})
        self.store.cas('service-history.json',old,new,lease)

    def dispatch(self,argv,lease,fence,operation):
        lease.assert_held()
        require(tuple(argv)==('restart','grid-api.service') and operation['fence']==fence,'service scope/fence')
        old=self.history(); entry=old['entries'][-1]
        require(entry['operation']==operation and entry['phase']=='PREPARED','already submitted/foreign operation')
        # Durable DISPATCH_UNCERTAIN BEFORE external handoff. Crash at either side
        # forbids resend; only authoritative terminal reconciliation can advance.
        new=deepcopy(old); new['entries'][-1]['phase']='DISPATCH_UNCERTAIN'
        self.store.cas('service-history.json',old,new,lease)
        response=self.driver.submit_once(operation)
        require(response=='request-accepted','uncertain manager submission')
        return response


class ExecutorStore:
    """Native stable-lock ownership and monotonic epoch with durable terminal."""
    def __init__(self,directory,lock_identity):
        self.directory=directory; self.lock_identity=lock_identity; self.store=AtomicStore(directory)
    def acquire_stable_lock_nonblocking(self): return Lease(self.directory,self.lock_identity)
    def state(self):
        value=self.store.read('executor-state.json')
        require(value.get('schema')=='grid.api-executor.v1' and isinstance(value.get('epoch'),int) and value['epoch']>=0,
                'executor state schema')
        return value
    def read_record_and_journal(self,lease):
        lease.assert_held(); return self.state()['active'],self.store.optional('journal.json')
    def last_epoch(self,lease): lease.assert_held(); return self.state()['epoch']
    def fresh_nonce(self,lease): lease.assert_held(); return uuid.uuid4().hex
    def cas_record_durable(self,lease,old,record):
        current=self.state()
        require(current['active']==old and record['epoch']==current['epoch']+1,'executor epoch/CAS')
        self.store.cas('executor-state.json',current,{'schema':'grid.api-executor.v1','epoch':record['epoch'],'active':record},lease)
    def assert_live_fence(self,lease,fence,plan):
        lease.assert_held(); active=self.state()['active']
        require(isinstance(active,dict) and active['fence']==fence and active['plan']==plan,'stale executor fence')
    def write_journal_durable(self,lease,fence,journal):
        self.assert_live_fence(lease,fence,journal['plan'])
        self.store.cas('journal.json',self.store.optional('journal.json'),journal,lease)
    def finish_durable(self,lease,fence,state):
        old=self.state(); active=old['active']
        self.assert_live_fence(lease,fence,active['plan'])
        journal=self.store.read('journal.json')
        require(journal['state']==state and journal['fence']==fence,'terminal journal mismatch')
        terminal={'state':state,'epoch':old['epoch'],'plan':active['plan'],'fence':fence,'journal_sha256':json_hash(journal)}
        self.store.cas('terminal.json',self.store.optional('terminal.json'),terminal,lease)
        self.store.cas('executor-state.json',old,{'schema':'grid.api-executor.v1','epoch':old['epoch'],'active':None},lease)
        lease.release_after_durable_terminal()
