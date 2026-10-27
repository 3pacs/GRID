"""Concrete native Adapter kernel composition; no production capability factory.

Construct only after independent descriptor/identity/mount/receipt admission.
Native fixture source is supplied separately and has NOT been run in this packet.
"""
import os
import stat
from api_bundle_contract import require, json_hash, verify_bundle
from api_isolation_native_fs import Tree, ReceivingSink, directory_at
from api_isolation_native_store import ExecutorStore, DurableBroker


class NativeKernel(ExecutorStore):
    def __init__(self,control,lock_identity,api_root,inbox,releases,
                 manifest,expected,protected_inodes,authority,manager=None):
        super().__init__(control,lock_identity)
        self.api_root,self.inbox,self.releases=api_root,inbox,releases
        self.manifest,self.expected=manifest,expected
        self.protected=set(protected_inodes); self.authority=authority
        self.broker=DurableBroker(self.store,manager)
        self.staged={}

    def verify_bound_receipts_and_topology(self,p):
        require(self.authority.verify_adoption(p,self) is True,'native adoption authority refused')
        require(json_hash(self.manifest)==p['binding']['bundle_manifest'],'manifest binding')
        require(self.manifest['commit']==p['binding']['candidate_commit'] and
                self.manifest['tree']==p['binding']['candidate_tree'],'receiving source binding')
        # Both baseline and candidate identities, readonly DB observation, CPU,
        # imports, protected aliases/packages and UID/mount custody are verified
        # by this exact authority, never fabricated from digest-shaped strings.
        require(self.authority.verify_baseline_and_evidence(p) is True,'baseline/evidence authority refused')

    def observe_protected(self,binding): return self.authority.observe_protected(binding)
    def verify_recovery_approval(self,p,approval):
        require(self.authority.verify_recovery(p,approval) is True,'recovery authority refused')
    def verify_api_observation(self,p,target):
        require(self.authority.verify_api(p,target) is True,'API observation authority refused')

    def current(self):
        s=os.stat('current',dir_fd=self.api_root.fd,follow_symlinks=False)
        require(stat.S_ISLNK(s.st_mode) and s.st_uid==self.api_root.uid and s.st_nlink==1,'current alias identity')
        return os.readlink('current',dir_fd=self.api_root.fd)

    def copy_bundle_exclusive(self,lease,fence,p):
        self.assert_live_fence(lease,fence,p['plan_sha256'])
        commit=p['binding']['candidate_commit']
        source=directory_at(self.inbox,commit)
        sink=None
        try:
            observations=Tree(source,self.protected).observations()
            verify_bundle(self.manifest,observations,self.expected,self.protected)
            sink=ReceivingSink(self.releases,lease,self.protected)
            token=sink.begin_exclusive(commit,p['binding']['bundle_manifest'])
            for name,obs in sorted(observations.items()):
                self.assert_live_fence(lease,fence,p['plan_sha256'])
                sink.write_exclusive(token,name,obs['mode'],obs['content'])
            require(sink.inventory(token)==self.manifest['files'],'staged inventory mismatch')
            sink.publish_no_replace(token,p['binding']['bundle_manifest'])
            self.staged[commit]=p['binding']['bundle_manifest']
        finally:
            if sink is not None: sink.close()
            os.close(source.fd)

    def verify_staged_descriptors(self,p):
        commit=p['binding']['candidate_commit']
        release=directory_at(self.releases,commit)
        try:
            verify_bundle(self.manifest,Tree(release,self.protected).observations(),self.expected,self.protected)
        finally: os.close(release.fd)

    def compare_exchange_current_durable(self,lease,fence,expected,target):
        active=self.state()['active']; self.assert_live_fence(lease,fence,active['plan'])
        allowed=self.authority.allowed_release_targets(active['plan'])
        require(type(allowed) is tuple and len(allowed)==2,'unbound alias target set')
        self.api_root.replace_alias(expected,target,allowed,lease)

    def read_service_operation(self,lease): return self.broker.read_service_operation(lease)
    def verify_never_issued_service_receipt(self,receipt): return self.broker.verify_never_issued_service_receipt(receipt)
    def service_manager_identity(self,p): return self.broker.service_manager_identity(p)
    def observe_terminal_service_operation(self,lease,operation): return self.broker.observe_terminal_service_operation(lease,operation)
    def verify_terminal_service_receipt(self,operation,receipt): return self.broker.verify_terminal_service_receipt(operation,receipt)
    def prepare_service_operation_durable(self,lease,fence,operation,terminal):
        self.assert_live_fence(lease,fence,operation['plan'])
        self.broker.prepare(lease,operation,terminal)
    def service(self,argv,lease,fence,operation):
        self.assert_live_fence(lease,fence,operation['plan'])
        return self.broker.dispatch(argv,lease,fence,operation)
