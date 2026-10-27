"""Real BytesIO archive tests; injected transfer/kernel/exec boundary tests.

No tempfiles, OS links, package imports, subprocesses or application execution.
"""
import ast
import copy
import io
from pathlib import Path
import sys
import tarfile
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import api_bundle_contract as c
import api_isolation_artifact as a
import api_isolation_adapter as adapter
import deploy_api_isolated as txn
import deploy_api_launcher as launcher
import deploy_api_rollback as rollback
import deploy_api_bundle as builder
from test_api_bundle_contract import fixture
from test_api_isolation_prototype import Simulation


def binding():
    return Simulation().b


def artifact():
    old, observations, expected = fixture()
    payload = {n: (o["mode"], o["content"]) for n, o in observations.items()}
    runtime = {n: v for n, v in payload.items() if n.startswith("runtime/")}
    raw_runtime = a.encode(runtime)
    identity = {k: old[k] for k in ("commit", "tree", "lock_sha256", "native_receipt_sha256")}
    identity["runtime_archive_sha256"] = a.sha(raw_runtime)
    inventory = {n: {"mode": mode, "size": len(data), "sha256": a.sha(data)} for n, (mode, data) in runtime.items()}
    blobs = {c.git_blob(v[1]): v[1] for n, v in payload.items() if n.startswith("source/")}
    manifest, raw = a.produce(identity, old["provenance"], old["projection"], blobs,
                              raw_runtime, a.sha(raw_runtime), inventory)
    expected.update(runtime_archive_sha256=a.sha(raw_runtime), inventory_sha256=c.json_hash(manifest["files"]))
    admission = {"manifest_sha256": c.json_hash(manifest), "archive_sha256": a.sha(raw),
                 "receiving_commit": manifest["commit"], "expected": expected,
                 "max_bytes": 1024*1024, "max_members": 100}
    return manifest, raw, admission, (identity, old, blobs, raw_runtime, inventory)


class Sink:
    def __init__(self, fault=None):
        self.events, self.files, self.fault = [], {}, fault
    def begin_exclusive(self, commit, manifest):
        self.events.append("begin")
        if self.fault == "collision":
            raise FileExistsError("retained transaction")
        return (commit, manifest)
    def write_exclusive(self, token, name, mode, data):
        if self.fault == "write" and self.files:
            raise OSError("injected partial transfer")
        self.files[name] = {"mode": mode, "size": len(data), "sha256": a.sha(data)}
        self.events.append("write")
    def inventory(self, token):
        self.events.append("readback")
        return {} if self.fault == "readback" else self.files
    def publish_no_replace(self, token, manifest):
        self.events.append("publish")


class ArtifactTests(unittest.TestCase):
    def test_builder_interface_binds_exact_profile_and_projection(self):
        m, raw, adm, (identity, old, blobs, rt, inv) = artifact()
        profile = {"schema":"grid.api-builder.v2", "python":"3.10", "source_tree":identity["tree"],
                   "projection_sha256":c.json_hash(old["projection"]), "runtime_archive_sha256":a.sha(rt),
                   "requirements_lock_sha256":identity["lock_sha256"], "network":"none",
                   "production_mounts":[], "credential_mounts":[], "wheels":{"fixture.whl":"f"*64}}
        for key in ("builder_image_sha256","interpreter_sha256","node_archive_sha256","package_lock_sha256"):
            profile[key]="f"*64
        actual, payload = builder.assemble(profile,identity["commit"],old["provenance"],old["projection"],blobs,rt,inv,identity["native_receipt_sha256"])
        self.assertEqual(actual,m); self.assertEqual(payload,raw)
        profile["projection_sha256"]="0"*64
        with self.assertRaises(c.Refusal): builder.assemble(profile,identity["commit"],old["provenance"],old["projection"],blobs,rt,inv,identity["native_receipt_sha256"])

    def test_real_deterministic_tar_roundtrip_and_reader(self):
        m, raw, adm, _ = artifact()
        self.assertEqual(raw, artifact()[1])
        with tarfile.open(fileobj=io.BytesIO(raw)) as reader:
            self.assertEqual(reader.extractfile("source/api/main.py").read(), b"safe source\n")
        self.assertEqual(len(a.receive(m, raw, adm)), 5)

    def test_transfer_success_and_digest_failure_before_first_write(self):
        m, raw, adm, _ = artifact()
        sink = Sink()
        self.assertFalse(a.transfer(m, raw, adm, sink)["physical_qualification"])
        self.assertEqual(sink.events[-2:], ["readback", "publish"])
        for bad in (raw[:-1], raw + b"malicious", b"x" + raw[1:]):
            sink = Sink()
            with self.assertRaises(c.Refusal):
                a.transfer(m, bad, adm, sink)
            self.assertEqual(sink.events, [])

    def test_partial_transfer_collision_and_readback_never_publish_or_clean(self):
        m, raw, adm, _ = artifact()
        for fault in ("collision", "write", "readback"):
            sink = Sink(fault)
            with self.assertRaises((c.Refusal, OSError)):
                a.transfer(m, raw, adm, sink)
            self.assertNotIn("publish", sink.events)
            if fault != "collision":
                self.assertTrue(sink.files)  # evidence retained; no cleanup

    def test_wrong_receiving_head_manifest_and_projection_rejected(self):
        m, raw, adm, _ = artifact()
        for key in ("receiving_commit", "manifest_sha256"):
            bad = copy.deepcopy(adm); bad[key] = "0" * len(bad[key])
            with self.assertRaises(c.Refusal):
                a.receive(m, raw, bad)
        bad = copy.deepcopy(m); bad["projection"]["entries"]["api/main.py"]["action"] = "omit"
        adm["manifest_sha256"] = c.json_hash(bad)
        with self.assertRaises(c.Refusal):
            a.receive(bad, raw, adm)

    def test_real_tar_special_duplicate_and_escape_controls(self):
        for kind, name, duplicate in ((tarfile.SYMTYPE,"source/link",False),
                                      (tarfile.LNKTYPE,"source/link",False),
                                      (tarfile.FIFOTYPE,"source/fifo",False),
                                      (tarfile.DIRTYPE,"source/dir",False),
                                      (tarfile.REGTYPE,"../escape",False),
                                      (tarfile.REGTYPE,"source/file",True)):
            stream = io.BytesIO()
            with tarfile.open(fileobj=stream, mode="w", format=tarfile.USTAR_FORMAT) as tar:
                info = tarfile.TarInfo(name); info.type = kind; info.mode = 0o644
                if kind in (tarfile.SYMTYPE, tarfile.LNKTYPE): info.linkname = "/held/legacy"
                tar.addfile(info)
                if duplicate: tar.addfile(info)
            raw = stream.getvalue()
            with self.assertRaises(c.Refusal): a.decode(raw, a.sha(raw), len(raw), 10)

    def test_tar_hidden_tail_limits_and_ancestor_collision(self):
        raw = a.encode({"source/a": ("100644", b"data")})
        for bad, size, count in ((raw+b"tail",len(raw)+4,10),(raw,len(raw)-1,10),(raw,len(raw),0)):
            with self.assertRaises(c.Refusal): a.decode(bad,a.sha(bad),size,count)
        raw = a.encode({"source/a": ("100644",b"x"), "source/a/b": ("100644",b"y")})
        with self.assertRaises(c.Refusal): a.decode(raw,a.sha(raw),len(raw),10)

    def test_missing_git_and_partial_prebuilt_runtime_refused(self):
        _, _, _, (identity, old, blobs, rt, inv) = artifact()
        for bad_blobs, bad_inv in (({},inv),(blobs,{})):
            with self.assertRaises(c.Refusal):
                a.produce(identity, old["provenance"], old["projection"], bad_blobs, rt, a.sha(rt), bad_inv)

    def test_repacked_runtime_path_leak_and_source_shadow_still_refuse(self):
        m, raw, adm, _ = artifact()
        files = a.decode(raw,a.sha(raw),len(raw),100)
        for name in ("runtime/lib/python3.10/site-packages/leak.pth", "source/api/shadow.py"):
            bad_files = dict(files); bad_files[name] = ("100644",b"/held/shared")
            bad = a.encode(bad_files); manifest = copy.deepcopy(m)
            manifest["files"][name] = {"mode":"100644","size":12,"sha256":a.sha(b"/held/shared")}
            admission = copy.deepcopy(adm)
            admission.update(manifest_sha256=c.json_hash(manifest), archive_sha256=a.sha(bad))
            admission["expected"]["inventory_sha256"] = c.json_hash(manifest["files"])
            with self.assertRaises(c.Refusal): a.receive(manifest,bad,admission)


class Kernel:
    """Volatile fake kernel with actual process-local exclusive threading.Lock."""
    def __init__(self, b):
        self.b=b; self.lock=threading.Lock(); self.record=None; self.journal=None
        self.epoch=0; self.nonce=0; self.target=b["old_release"]; self.trace=[]; self.token=None
        self.operation=None; self.terminal=None; self.operations=[]; self.trusted=True
    def acquire_stable_lock_nonblocking(self):
        if not self.lock.acquire(False): raise RuntimeError("owned")
        self.token=object(); self.trace.append("claim"); return self.token
    def read_record_and_journal(self, lease): return copy.deepcopy(self.record),copy.deepcopy(self.journal)
    def last_epoch(self, lease): return self.epoch
    def fresh_nonce(self, lease): self.nonce+=1; return str(self.nonce)
    def cas_record_durable(self, lease, old, new):
        if lease is not self.token or self.record != old: raise RuntimeError("CAS")
        self.record=copy.deepcopy(new); self.epoch=new["epoch"]; self.trace.append("record")
    def assert_live_fence(self, lease, fence, plan):
        if not self.lock.locked() or lease is not self.token or self.record["fence"]!=fence or self.record["plan"]!=plan: raise RuntimeError("stale")
    def verify_bound_receipts_and_topology(self,p): self.trace.append("preflight")
    def observe_protected(self,b): return b["protected_snapshot"]
    def current(self): return self.target
    def verify_recovery_approval(self,p,a): self.trace.append("recovery-check")
    def read_service_operation(self,lease):
        if self.operation is not None: return {"state":"ISSUED","operation":copy.deepcopy(self.operation)}
        if self.operations: return None  # missing is NOT a fresh empty history
        return {"state":"NEVER_ISSUED","receipt":{"state":"NEVER_ISSUED","host":"grid-svr",
                "service":"grid-api.service","namespace":"grid-api-isolated",
                "manager_identity_sha256":"d"*64,"receipt_sha256":"c"*64}}
    def verify_never_issued_service_receipt(self,receipt): return self.trusted
    def service_manager_identity(self,p): return "d"*64
    def observe_terminal_service_operation(self,lease,operation): return copy.deepcopy(self.terminal)
    def verify_terminal_service_receipt(self,operation,receipt): return self.trusted
    def prepare_service_operation_durable(self,lease,fence,operation,terminal):
        self.assert_live_fence(lease,fence,operation["plan"])
        if self.operation is not None and terminal != self.terminal: raise RuntimeError("operation CAS")
        self.operation=copy.deepcopy(operation); self.operations.append(copy.deepcopy(operation)); self.terminal=None
        self.trace.append("operation-intent")
    def settle(self,outcome="succeeded"):
        self.terminal={k:self.operation[k] for k in ("operation_id","manager_identity_sha256","host","service","target","plan")}
        self.terminal.update(phase="terminal",outcome=outcome,no_further_effects=True,receipt_sha256="e"*64)
    def copy_bundle_exclusive(self,lease,fence,p):
        self.assert_live_fence(lease,fence,p["plan_sha256"]); self.trace.append("stage")
    def verify_staged_descriptors(self,p): self.trace.append("stage-verify")
    def write_journal_durable(self,lease,fence,j):
        self.assert_live_fence(lease,fence,j["plan"]); self.journal=copy.deepcopy(j); self.trace.append(j["state"])
    def compare_exchange_current_durable(self,lease,fence,expected,target):
        if self.target!=expected: raise RuntimeError("wrong alias")
        self.target=target; self.trace.append("swap")
    def verify_api_observation(self,p,target):
        if self.target!=target: raise RuntimeError("wrong API")
        self.trace.append("verify-api")
    def finish_durable(self,lease,fence,state):
        self.trace.append("finish"); self.record=None; self.lock.release(); self.token=None
    def crash(self): self.lock.release(); self.token=None


class AdapterTests(unittest.TestCase):
    def service(self,k, result="request-accepted"):
        def call(argv, lease, fence, operation):
            self.assertEqual(argv,("restart","grid-api.service"))
            self.assertEqual(k.journal["state"],"API_SERVICE_REQUEST_INTENT")
            self.assertEqual(k.operation,operation)
            if result == "request-accepted": k.settle()
            k.trace.append("service"); return result
        return call

    def test_deploy_through_real_adapter_orders_intent_and_verification(self):
        b=binding(); k=Kernel(b); p=txn.deploy(b,adapter.Adapter(k,self.service(k)))
        self.assertEqual(k.target,p["candidate"])
        self.assertLess(k.trace.index("PROMOTION_INTENT"),k.trace.index("swap"))
        self.assertLess(k.trace.index("stage-verify"),k.trace.index("swap"))
        self.assertLess(k.trace.index("API_SERVICE_REQUEST_INTENT"),k.trace.index("service"))
        self.assertEqual(k.trace[-2:],["VERIFIED","finish"])

    def test_uncertain_request_retains_journal_and_does_not_retry(self):
        b=binding(); k=Kernel(b)
        with self.assertRaises(txn.Refusal): txn.deploy(b,adapter.Adapter(k,self.service(k,"unknown")))
        self.assertEqual(k.trace.count("service"),1)
        self.assertTrue(k.lock.locked()); self.assertIsNotNone(k.record)
        self.assertEqual(k.journal["state"],"API_SERVICE_REQUEST_INTENT")
        self.assertNotIn("finish",k.trace)

    def test_rollback_after_uncertain_request_uses_new_fence_and_keeps_code(self):
        b=binding(); k=Kernel(b); first=adapter.Adapter(k,self.service(k,"unknown"))
        with self.assertRaises(txn.Refusal): txn.deploy(b,first)
        approval={"plan_sha256":txn.plan(b)["plan_sha256"],"journal_sha256":txn.digest(k.journal),
                  "lock_record_sha256":txn.digest(k.record),"owner_receipt":"f"*64,
                  "compatibility_receipt":b["evidence"]["rollback_compatibility"]}
        k.crash()
        k.settle("failed")  # explicit trusted simulated terminal reconciliation
        rollback.rollback(b,approval,k,self.service(k))
        self.assertEqual(k.target,b["old_release"]); self.assertEqual(k.epoch,2)
        self.assertNotIn("delete",k.trace)
        self.assertEqual(k.trace[-2],"CODE_ROLLED_BACK_DATA_UNCHANGED_BY_TOOL")

    def test_live_owner_and_stale_fence_cannot_mutate(self):
        b=binding(); k=Kernel(b); p=txn.plan(b); first=adapter.Adapter(k,self.service(k))
        p["executor_fence"]=first.claim(p)
        with self.assertRaises(RuntimeError): adapter.Adapter(k,self.service(k)).claim(p)
        p["executor_fence"]="0"*64
        with self.assertRaises(txn.Refusal): first.promote(p,p["old"],p["candidate"])
        self.assertEqual(k.target,p["old"])

    def test_recovery_record_change_refused_under_lock_before_mutation(self):
        b=binding(); k=Kernel(b); p=txn.plan(b); first=adapter.Adapter(k,self.service(k))
        p["executor_fence"]=first.claim(p); first.journal(p,"CLAIMED")
        adm={"lock_record_sha256":txn.digest(k.record),"journal_sha256":txn.digest(k.journal)}
        k.crash(); k.record["nonce"]="changed"; count=len(k.trace)
        with self.assertRaises(txn.Refusal): adapter.Adapter(k,self.service(k)).claim_recovery(p,adm)
        self.assertEqual(k.trace[count:],["claim"])


class ExecCaptured(BaseException): pass


class LauncherTests(unittest.TestCase):
    @staticmethod
    def state_binding():
        return {"schema":"grid.api-state-handoff.v1","composition_receipt_sha256":"a"*64,
                "workflow":{"state_root":"/owned/workflows","receipt_sha256":"b"*64,"source_identity":"fixture"},
                "output":{"state_root":"/owned/output","receipt_sha256":"c"*64}}
    def test_one_alias_resolution_and_exact_inert_exec_capture(self):
        commit="b"*40; target=txn.ROOT+"/releases/"+commit
        class K:
            reads=0; captured=None
            def pin_current(self): self.reads+=1; return "descriptor",target
            def verify_release(self,token,commit,spec): self.checked=token
            def state_bound_program(self,token,spec,bound): return "raise RuntimeError('INERT_BOUND_BOOTSTRAP')"
            def exec_pinned(self,token,argv,env,cwd):
                self.captured=(token,argv,env,cwd); raise ExecCaptured()
        k=K()
        with self.assertRaises(ExecCaptured): launcher.launch(k,commit,{"GRID_ENV":"production"},{"api":target+"/source/api/main.py"},self.state_binding())
        self.assertEqual(k.reads,1)
        token,argv,env,cwd=k.captured
        self.assertEqual(argv[1:5],["-I","-S","-B","-c"])
        ast.parse(argv[-1])  # NEVER exec/run API code
        self.assertEqual(token,"descriptor"); self.assertEqual(cwd,target+"/source")
        self.assertEqual(env,{"GRID_ENV":"production"})

    def test_legacy_import_origin_fails_before_exec(self):
        class K:
            def pin_current(self): return "token",txn.ROOT+"/releases/"+"b"*40
            def verify_release(self,*args): raise AssertionError("too late")
        with self.assertRaises(c.Refusal): launcher.launch(K(),"b"*40,{}, {"api":"/held/shared/api.py"},self.state_binding())


if __name__ == "__main__": unittest.main()
