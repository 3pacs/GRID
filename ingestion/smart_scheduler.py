"""
GRID Smart Scheduler — runs only due/stale pullers per cycle.

Replaces the old pattern of running ALL 50+ pullers every pipeline cycle.
Each puller has an expected frequency. On each tick, we check which pullers
are overdue and run only those, capped at MAX_PULLERS_PER_TICK to keep
cycles short (a 5-minute scheduling cutoff; the final puller may exceed it).

Pullers that fail or timeout get exponential backoff cooldowns. The old
full pipeline (run_full_pipeline.py) is still available for manual runs.

Usage from Hermes:
    from ingestion.smart_scheduler import SmartScheduler
    sched = SmartScheduler(engine)
    result = sched.tick()  # options may occupy up to 15 minutes
"""

from __future__ import annotations

# Finite SEC-only front door. This section uses stdlib only and exits before
# the ordinary scheduler's imports, constructors, catalog reads or broad tick.
_SEC_ONLY_BASE = "7e3dec01d5634b674a345195e72999c0d30b6f2f"
_SEC_ONLY_PINS = {
    "driver": "c188ae624e89fcd5d10c9c70f1c24c6116167f9a7a7e16417090f988f2f8a369",
    "supervisor": "8ff0278d827f32860bdfc89d2d8fa8854a27915da237cf9fc2ef3290151f6f22",
    "producer": "974222b930bee06864c59123f798192a50cde7585eff75690ee60675b56b05c6",
    "helper": "93b9d64552dbb0adc4ae0050cde89c481d9a9116386f3373b713beaa1beaecdc",
}
_SEC_ONLY_DEPENDENCY_STATUS = "ACCEPTED_HELPER_SOURCE_NATIVE_UNQUALIFIED"
_SEC_ONLY_USED = False


class SecOnlyRefused(ValueError):
    pass


def _sec_require(value, code):
    if value is not True:
        raise SecOnlyRefused(code)


def _sec_outer_clock(origin, clock):
    """Sticky observed deadline, with one captured actual reader and origin.

    No callback preemption or kernel quota is claimed. A late callback cannot
    grant a fresh budget or permit a subsequent effect.
    """
    from math import isfinite
    _sec_require(type(origin) in (int, float) and isfinite(origin)
                 and isfinite(origin + 150) and callable(clock), "OUTER_CLOCK_INVALID")
    original = origin
    reader = clock
    last = origin
    invalid = False
    def observe(*, cleanup=False):
        nonlocal last, invalid
        if invalid:
            raise SecOnlyRefused("OUTER_CLOCK_INVALID")
        try:
            value = reader()
            _sec_require(type(value) in (int, float) and isfinite(value)
                         and value >= last and value >= original, "OUTER_CLOCK_INVALID")
            last = value
            _sec_require(value < original + (150 if cleanup else 120), "OUTER_DEADLINE")
            return value
        except BaseException:
            invalid = True
            raise
    observe()
    return observe


def _sec_same_outer(actual, expected):
    _sec_require(type(actual) is dict and type(expected) is dict
                 and _sec_canonical(actual) == _sec_canonical(expected), "OUTER_FRAME_BINDING")


def _sec_bound_saved_receipt(parent, expected_call, outer, driver, *, inner_sha):
    """Saved PENDING is diagnostic evidence, never terminal/persisted success."""
    _sec_require(type(parent) is dict and parent.get("schema") == "sec-occurrence-parent-v1",
                 "DIAGNOSTIC_RECEIPT_BINDING")
    child = parent.get("child_receipt")
    _sec_require(type(child) is dict and child.get("schema") == "sec-occurrence-live-v1",
                 "DIAGNOSTIC_RECEIPT_BINDING")
    for receipt in (parent, child):
        binding = receipt.get("binding")
        _sec_require(type(binding) is dict and _sec_canonical(binding) == _sec_canonical(expected_call),
                     "DIAGNOSTIC_RECEIPT_BINDING")
        # Normalized expected_call was produced by the pinned driver's Call.
        # JSON type equality, including t0 and bool/int distinctions, is exact.
        _sec_same_outer(receipt.get("outer_deadline"), outer)
        _sec_require(type(receipt.get("driver_admission_sha256")) is str
                     and receipt["driver_admission_sha256"] == inner_sha, "DIAGNOSTIC_INNER_ADMISSION_BINDING")
    finalization = parent.get("parent_finalization")
    _sec_require(type(finalization) is dict and finalization.get("state") == "PENDING_AT_PERSIST",
                 "SAVED_PARENT_FINALIZATION")
    return {"saved_parent_state": "PENDING_AT_PERSIST", "terminal_parent_state": "UNKNOWN",
            "diagnostics": _sec_diagnostics(child, driver)}


def _sec_terminal_parent(result, expected_call, outer, inner_sha):
    """Validate only the existing root's closed safe terminal summary."""
    from math import isfinite
    status = result.get("terminal_parent_status")
    terminal = result.get("terminal_parent")
    if status == "UNKNOWN":
        _sec_require(terminal is None, "TERMINAL_PARENT_BINDING")
        return {"terminal_parent_state": "UNKNOWN", "terminal_parent_observation": "UNKNOWN"}
    _sec_require(type(status) is str and status in ("OBSERVED_FINAL_CLOSED", "OBSERVED_FINAL_FAILURE")
                 and type(terminal) is dict and set(terminal) == {
                     "schema", "parent_schema", "call_id", "binding", "outer_deadline",
                     "driver_admission_sha256", "live_qualified", "error_type", "parent_finalization"}, "TERMINAL_PARENT_BINDING")
    _sec_require(terminal["schema"] == "sec-parent-terminal-frame-v1"
                 and terminal["parent_schema"] == "sec-occurrence-parent-v1"
                 and terminal["call_id"] == expected_call["call_id"]
                 and type(terminal["binding"]) is dict
                 and _sec_canonical(terminal["binding"]) == _sec_canonical(expected_call)
                 and terminal["driver_admission_sha256"] == inner_sha
                 and terminal["live_qualified"] is False
                 and (terminal["error_type"] is None or (type(terminal["error_type"]) is str
                      and terminal["error_type"] in ("Refused", "UNKNOWN", "ParentFinalizationRefused"))), "TERMINAL_PARENT_BINDING")
    _sec_same_outer(terminal["outer_deadline"], outer)
    final = terminal["parent_finalization"]
    _sec_require(type(final) is dict and set(final) == {"state", "stop_end", "observations", "failures", "persisted_version"}, "TERMINAL_PARENT_FINALIZATION")
    _sec_require(type(final["state"]) is str and final["state"] in ("CLOSED_WITHIN_BOUND", "FAILED")
                 and type(final["stop_end"]) is type(outer["stop_end_mono"])
                 and final["stop_end"] == outer["stop_end_mono"]
                 and final["persisted_version"] == "PENDING_AT_PERSIST; returned/CLI frame carries final close proof"
                 and type(final["observations"]) is list and len(final["observations"]) <= 4
                 and type(final["failures"]) is list and len(final["failures"]) <= 8, "TERMINAL_PARENT_FINALIZATION")
    stages = ("before_persist", "after_persist", "before_close", "after_close")
    seen = []
    last_time = outer["origin_mono"]
    for point in final["observations"]:
        _sec_require(type(point) is dict and set(point) == {"stage", "monotonic"}
                     and type(point["stage"]) is str and point["stage"] in stages
                     and (not seen or stages.index(point["stage"]) > stages.index(seen[-1]))
                     and type(point["monotonic"]) in (int, float) and isfinite(point["monotonic"])
                     and last_time <= point["monotonic"] < outer["stop_end_mono"], "TERMINAL_PARENT_FINALIZATION")
        seen.append(point["stage"]); last_time = point["monotonic"]
    for failure in final["failures"]:
        _sec_require(type(failure) is dict and set(failure) == {"stage", "error_type"}
                     and type(failure["stage"]) is str and failure["stage"] in stages + ("persist", "close")
                     and type(failure["error_type"]) is str and failure["error_type"] in ("Refused", "UNKNOWN"), "TERMINAL_PARENT_FINALIZATION")
    _sec_require((final["state"] == "CLOSED_WITHIN_BOUND" and status == "OBSERVED_FINAL_CLOSED"
                  and seen == list(stages) and final["failures"] == [])
                 or (final["state"] == "FAILED" and status == "OBSERVED_FINAL_FAILURE"), "TERMINAL_PARENT_FINALIZATION")
    return {"terminal_parent_state": final["state"], "terminal_parent_observation": status}


def _sec_json(raw, cap=1048576):
    import json
    _sec_require(type(raw) is bytes and len(raw) <= cap, "INPUT_BOUND")
    def unique(pairs):
        out = {}
        for key, value in pairs:
            _sec_require(key not in out, "DUPLICATE_KEY")
            out[key] = value
        return out
    return json.loads(raw, object_pairs_hook=unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(SecOnlyRefused("NONFINITE_JSON")))


def _sec_canonical(value):
    import json
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sec_read(path, cap):
    import os
    _sec_require(type(path) is str and path.startswith("/"), "ABSOLUTE_PATH")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        import stat
        _sec_require(stat.S_ISREG(os.fstat(fd).st_mode), "REGULAR_FILE")
        data = bytearray()
        while len(data) <= cap:
            chunk = os.read(fd, min(65536, cap + 1 - len(data)))
            if not chunk: break
            data.extend(chunk)
        _sec_require(len(data) <= cap, "FILE_BOUND")
        return bytes(data)
    finally:
        os.close(fd)


def _sec_load(path, raw, digest):
    # Compile the already-hashed bytes, never reopen a mutable import path.
    import hashlib
    import sys
    import types
    _sec_require(hashlib.sha256(raw).hexdigest() == digest, "SOURCE_PIN")
    name = "_hermes_sec_dependency_" + digest
    module = types.ModuleType(name)
    module.__file__ = path
    sys.modules[name] = module
    try:
        exec(compile(raw, path, "exec"), module.__dict__)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return module


def _sec_transport_evidence(receipt):
    unknown = {"state": "UNKNOWN", "records": [], "zero_network_proven": False}
    records = receipt.get("transport_attempts") if type(receipt) is dict else None
    if type(records) is not list or len(records) > 2:
        return unknown
    safe = []
    for item in records:
        if not (type(item) is dict and set(item) == {"url_id", "phase_reached", "status", "will_close"}
                and type(item["url_id"]) is str and item["url_id"] in ("MAP", "FACTS")
                and type(item["phase_reached"]) is str and item["phase_reached"] in ("CONSUMED", "HEADERS", "STATUS_OK", "READ_ATTEMPT", "PARSED")
                and (item["status"] is None or (type(item["status"]) is int and 100 <= item["status"] <= 599))
                and (item["will_close"] is None or type(item["will_close"]) is bool)):
            return unknown
        safe.append(dict(item))
    return {"state": "REPORTED", "records": safe, "zero_network_proven": False}


def _sec_diagnostics(receipt, driver):
    # No exception strings, notes, arbitrary keys or process output are exported.
    unknown = {"phase": "UNKNOWN", "site": "UNKNOWN", "code": "UNKNOWN", "exception_kind": "UNKNOWN"}
    def record(value):
        if value is None: return None
        if type(value) is not dict: return dict(unknown)
        return {
            "phase": value.get("phase") if type(value.get("phase")) is str and value["phase"] in driver._SAFE_PHASES else "UNKNOWN",
            "site": value.get("site") if type(value.get("site")) is str and value["site"] in driver._SAFE_SITES else "UNKNOWN",
            "code": value.get("code") if type(value.get("code")) is str and value["code"] in driver._SAFE_REFUSAL_CODES else "UNKNOWN",
            "exception_kind": value.get("exception_kind") if type(value.get("exception_kind")) is str and value["exception_kind"] in ("REFUSED", "UNKNOWN") else "UNKNOWN",
        }
    safe = receipt.get("safe_failures") if type(receipt) is dict else None
    if type(safe) is not dict or safe.get("schema") != "sec-safe-refusals-v1":
        return {"first_producer_failure": dict(unknown), "worker_failure": dict(unknown),
                "transport_attempts": _sec_transport_evidence(receipt)}
    return {"first_producer_failure": record(safe.get("first_producer_failure")),
            "worker_failure": record(safe.get("worker_failure")),
            "transport_attempts": _sec_transport_evidence(receipt)}


def _sec_read_saved_receipt(admission, call_id, cap, io):
    """Read one bound saved parent receipt after owned cleanup; no new writer."""
    import stat
    root = admission["runtime"].get("receipt_root")
    _sec_require(type(root) is dict and set(root) == {"path", "device", "inode", "uid"}, "RECEIPT_ROOT_BINDING")
    os = io.os
    directory = fd = None
    pins = {}
    primary = None
    result = None
    close_ok = True
    try:
        io._guard()
        directory = io._safe_open(root["path"], os.O_RDONLY | os.O_DIRECTORY)
        st = os.fstat(directory)
        pins[directory] = (st.st_dev, st.st_ino, st.st_mode)
        _sec_require((st.st_dev, st.st_ino, st.st_uid, stat.S_IMODE(st.st_mode)) ==
                     (root["device"], root["inode"], root["uid"], 0o700), "RECEIPT_ROOT_BINDING")
        fd = os.open(call_id + ".json", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory)
        st = os.fstat(fd)
        pins[fd] = (st.st_dev, st.st_ino, st.st_mode)
        _sec_require(stat.S_ISREG(st.st_mode) and st.st_uid == root["uid"] and stat.S_IMODE(st.st_mode) == 0o600
                     and st.st_nlink == 1 and st.st_size <= cap, "RECEIPT_FILE_BINDING")
        data = bytearray()
        while len(data) <= cap:
            io._guard()
            chunk = os.read(fd, min(65536, cap + 1 - len(data)))
            _sec_require(type(chunk) is bytes, "RECEIPT_READ_BOUND")
            if not chunk: break
            data.extend(chunk)
        _sec_require(len(data) <= cap, "RECEIPT_READ_BOUND")
        io._guard()
        result = bytes(data)
    except BaseException as exc:
        primary = exc
    finally:
        for owned in (fd, directory):
            if owned is not None:
                try:
                    st = os.fstat(owned)
                    _sec_require((st.st_dev, st.st_ino, st.st_mode) == pins.get(owned), "RECEIPT_FD_REBOUND")
                    os.close(owned)
                except BaseException:
                    close_ok = False
    if primary is not None: raise primary
    _sec_require(close_ok, "RECEIPT_CLOSE_UNVERIFIED")
    io._guard()
    return result


def _sec_owned_stop_proof(result, call_id):
    _sec_require(type(result) is dict and result.get("schema") == "sec-root-supervisor-result-v6"
                 and result.get("call_id") == call_id, "OWNED_RECEIPT_BINDING")
    _sec_require(result.get("cleanup_checks_pass") is True and result.get("eof") is True
                 and type(result.get("observed_children")) is int and result["observed_children"] > 0
                 and type(result.get("reaped_children")) is int and type(result.get("closed_handles")) is int
                 and result["observed_children"] == result["reaped_children"] == result["closed_handles"]
                 and result.get("unknown_owned_children") == [], "OWNED_STOP_REAP_EOF_CLOSE_REQUIRED")
    return True


def run_sec_only_job(selection_bytes, expected_sha256, *, read_bytes=None,
                     load_artifact=None, native_factory=None, read_receipt=None,
                     entry_t0=None, clock=None):
    """One exact SEC occurrence using the existing owned supervisor, never tick.

    Injected file/module/native adapters are synthetic-test dependencies, not a
    runtime authority grant. Default NativeOS remains subject to its unchanged
    root/source/window/spent/cgroup guards. No fallback or repeated job follows.
    """
    # Capture before this entry's imports or dependency/selection reads. CLI
    # passes the earlier operator origin, and never substitutes a later sample.
    from time import monotonic
    reader = monotonic if clock is None else clock
    try:
        origin = reader() if entry_t0 is None else entry_t0
    except BaseException:
        return {"schema": "hermes-sec-only-result-v1", "status": "REFUSED", "code": "OUTER_CLOCK_INVALID", "workload_success": False, "commit": "UNKNOWN"}
    import hashlib
    import re
    global _SEC_ONLY_USED
    read_callback = read_bytes or _sec_read
    load_callback = load_artifact or _sec_load
    synthetic = any(value is not None for value in (read_bytes, load_artifact, native_factory, read_receipt)) or reader is not monotonic
    output = {"schema": "hermes-sec-only-result-v1", "source": "sec_xbrl_shares",
              "status": "REFUSED", "code": "INVALID_SELECTION", "owned_cleanup": False,
              "workload_success": False, "commit": "UNKNOWN", "diagnostics": None,
              "native_qualified": False, "synthetic_only": synthetic,
              "dependency_status": _SEC_ONLY_DEPENDENCY_STATUS}
    try:
        pins = _sec_canonical(_SEC_ONLY_PINS)
        def stable():
            _sec_require(_sec_canonical(_SEC_ONLY_PINS) == pins, "SOURCE_FRAME_CHANGED")
        clock_observe = _sec_outer_clock(origin, reader)
        stable()
        def observe(*, cleanup=False):
            stable()
            value = clock_observe(cleanup=cleanup)
            stable()  # clock is a callback too: validate before the next effect
            return value
        def read(path, cap):
            stable(); observe()
            value = read_callback(path, cap)
            observe(); stable()
            return value
        def load(path, raw, digest):
            stable(); observe()
            value = load_callback(path, raw, digest)
            observe(); stable()
            return value
        _sec_require(type(expected_sha256) is str and re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is not None,
                     "SELECTION_PIN")
        _sec_require(type(selection_bytes) is bytes and hashlib.sha256(selection_bytes).hexdigest() == expected_sha256,
                     "SELECTION_PIN")
        selection = _sec_json(selection_bytes, 131072)
        _sec_require(type(selection) is dict and set(selection) == {
            "schema", "base", "source", "module", "class", "method", "kwargs", "artifacts", "root_binding", "driver_admission"}, "SELECTION_FIELDS")
        _sec_require(selection["schema"] == "hermes-sec-only-v1" and selection["base"] == _SEC_ONLY_BASE,
                     "BASE_BINDING")
        _sec_require((selection["source"], selection["module"], selection["class"], selection["method"]) ==
            ("sec_xbrl_shares", "ingestion.altdata.sec_xbrl_shares", "SECXBRLSharesPuller", "pull_all"), "SOURCE_ALLOWLIST")
        _sec_require(_sec_canonical(selection["kwargs"]) == _sec_canonical({"tickers": ["AAPL"], "limit": 1, "backfill_days": 0}),
                     "JOB_ARGUMENTS")
        _sec_require(type(selection["artifacts"]) is dict and set(selection["artifacts"]) == set(_SEC_ONLY_PINS), "ARTIFACT_FIELDS")
        _sec_require(synthetic or entry_t0 is not None, "EARLY_ORIGIN_REQUIRED")
        _sec_require(native_factory is not None or reader is monotonic, "NATIVE_CLOCK_READER")
        modules = {}
        for role, digest in _SEC_ONLY_PINS.items():
            value = selection["artifacts"][role]
            _sec_require(type(value) is dict and set(value) == {"path", "sha256"} and value["sha256"] == digest,
                         "SOURCE_PIN")
            raw = read(value["path"], 2097152)
            _sec_require(type(raw) is bytes and len(raw) <= 2097152 and hashlib.sha256(raw).hexdigest() == digest, "SOURCE_PIN")
            if role in ("driver", "supervisor"):
                modules[role] = load(value["path"], raw, digest)
        driver, supervisor = modules["driver"], modules["supervisor"]
        packets = {}
        for role in ("root_binding", "driver_admission"):
            item = selection[role]
            _sec_require(type(item) is dict and set(item) == {"path", "sha256"}, "BINDING_FIELDS")
            raw = read(item["path"], 131072)
            _sec_require(type(raw) is bytes and hashlib.sha256(raw).hexdigest() == item["sha256"], "BINDING_PIN")
            packets[role] = _sec_json(raw, 131072)
        plan, admission = packets["root_binding"], packets["driver_admission"]
        packets_raw_admission = raw  # driver_admission is the final bounded packet read
        _sec_require(type(plan) is dict and type(admission) is dict and admission.get("status") == "ADMITTED"
                     and admission.get("schema") == "sec-occurrence-admission-v1", "ADMISSION_REQUIRED")
        call = admission.get("call")
        _sec_require(type(call) is dict and call.get("t0") is None and call.get("source_sha256") == _SEC_ONLY_PINS["producer"]
                     and call.get("ticker") == "AAPL" and type(call.get("limit")) is int and call["limit"] == 1
                     and type(call.get("backfill_days")) is int and call["backfill_days"] == 0
                     and plan.get("call_id") == call.get("call_id"), "CALL_IDENTITY")
        _sec_require(type(admission.get("runtime")) is dict and all(admission["runtime"].get(role) == selection["artifacts"][role]["path"]
                     for role in ("driver", "producer", "helper")), "EXECUTED_SOURCE_BINDING")
        _sec_require(type(admission.get("caps")) is dict and _sec_canonical(admission["caps"]) == driver.canonical(driver.asdict(driver.Caps())), "CAPS_BINDING")
        observe(); stable()
        outer = supervisor.make_outer_deadline(origin, call["call_id"],
                     selection["root_binding"]["sha256"], plan["wrapper_binding_sha256"])
        # No dynamic authority/frame fields are inserted in static admission.
        outer_original = _sec_json(_sec_canonical(outer).encode())
        supervisor.validate_outer_deadline(outer, observe(), call_id=call["call_id"],
                     root_binding_sha256=selection["root_binding"]["sha256"],
                     postdrop_binding_sha256=plan["wrapper_binding_sha256"], work=True)
        _sec_same_outer(outer, outer_original)
        # Pure validation binds Call.t0 to the SAME original outer origin.
        # Static admission bytes stay frame-free; there is no hash cycle.
        try:
            validated_packet, validated_call, validated_caps = driver.bind_live(packets_raw_admission, selection["driver_admission"]["sha256"], origin, outer_deadline=outer_original)
        except BaseException:
            raise SecOnlyRefused("DRIVER_ADMISSION_INVALID") from None
        _sec_require(admission["runtime"].get("driver_sha256") == _SEC_ONLY_PINS["driver"], "SOURCE_PIN")
        stage = plan.get("stage")
        _sec_require(type(stage) is list, "STAGE_FIELDS")
        stage_driver = [v for v in stage if type(v) is dict and v.get("relative_path") == "scripts/sec_occurrence_driver.py"]
        # Substantive source adoption blocker: unchanged old39 source cannot
        # qualify the new diagnostic package. No constants are patched here.
        if (len(stage_driver) != 1 or stage_driver[0].get("sha256") != _SEC_ONLY_PINS["driver"]
                or hashlib.sha256(supervisor.canonical(stage).encode()).hexdigest() != supervisor.STAGE_SHA):
            output.update(status="NATIVE_UNSUPPORTED", code="ROOT_STAGE_DIAGNOSTIC_ADOPTION_REQUIRED")
            return output
        wrapper_rows = [v for v in stage if type(v) is dict and v.get("relative_path") == "postdrop_exec.py"]
        if len(wrapper_rows) != 1 or wrapper_rows[0].get("sha256") != plan.get("wrapper_sha256"):
            output.update(status="NATIVE_UNSUPPORTED", code="ROOT_STAGE_WRAPPER_ADOPTION_REQUIRED")
            return output
        wrapper_source = read(supervisor.PKG + "/postdrop_exec.py", 2097152)
        if (type(wrapper_source) is not bytes or len(wrapper_source) != wrapper_rows[0].get("bytes")
                or hashlib.sha256(wrapper_source).hexdigest() != wrapper_rows[0]["sha256"]):
            output.update(status="NATIVE_UNSUPPORTED", code="ROOT_STAGE_WRAPPER_ADOPTION_REQUIRED")
            return output
        _sec_require(plan.get("schema") == "sec-owned-root-v7"
                     and plan.get("driver_admission_sha256") == selection["driver_admission"]["sha256"]
                     and type(plan.get("driver_call")) is dict
                     and set(call) == set(driver.asdict(validated_call))
                     and _sec_canonical(plan["driver_call"]) == _sec_canonical(call), "ROOT_INNER_ADMISSION_BINDING")
        wrapper_raw = read(plan["wrapper_binding_path"], 131072)
        _sec_require(type(wrapper_raw) is bytes and hashlib.sha256(wrapper_raw).hexdigest() == plan["wrapper_binding_sha256"], "WRAPPER_BINDING_PIN")
        wrapper = _sec_json(wrapper_raw, 131072)
        _sec_require(type(wrapper) is dict and wrapper.get("call_id") == call["call_id"]
                     and type(wrapper.get("admission")) is dict
                     and wrapper["admission"].get("path") == selection["driver_admission"]["path"]
                     and wrapper["admission"].get("sha256") == selection["driver_admission"]["sha256"], "WRAPPER_ADMISSION_BINDING")
        private = supervisor.validate_plan(plan)
        observe(); stable()
        _sec_require(private.get("controller_sha256") == _SEC_ONLY_PINS["supervisor"], "SUPERVISOR_PIN")
        if native_factory is None and _SEC_ONLY_DEPENDENCY_STATUS != "ACCEPTED_EXACT_SOURCE_ONLY":
            output.update(status="NATIVE_UNSUPPORTED", code="DEPENDENCY_NATIVE_UNQUALIFIED")
            return output
        _sec_require(_SEC_ONLY_USED is False, "FINITE_ATTEMPT_ALREADY_USED")
        _SEC_ONLY_USED = True  # never cleared after allocation/unknown stop
        output.update(status="UNKNOWN", code="OWNED_EXECUTION_UNVERIFIED")
        io = (native_factory(private, selection["root_binding"]["sha256"], entry_t0=origin)
              if native_factory is not None else supervisor.NativeOS(private, selection["root_binding"]["sha256"], entry_t0=origin))
        observe(); stable()
        _sec_same_outer(io.outer_deadline, outer_original)
        # Existing reducer owns consumption-before-launch and every child,
        # TERM/KILL/reap/EOF/capture close. No thread timeout or subsequent tick.
        result = supervisor.run(private, io, entry_t0=origin)
        observe(cleanup=True); stable()
        _sec_same_outer(result.get("outer_deadline") if type(result) is dict else None, outer_original)
        _sec_same_outer(io.outer_deadline, outer_original)
        _sec_owned_stop_proof(result, call["call_id"])
        terminal = _sec_terminal_parent(result, driver.asdict(validated_call), outer_original,
                                        selection["driver_admission"]["sha256"])
        original_result = _sec_canonical(result)
        def receipt_frame_guard():
            _sec_same_outer(io.outer_deadline, outer_original)
            _sec_require(_sec_canonical(result) == original_result, "OWNED_RECEIPT_CHANGED")
        receipt_frame_guard()
        output.update(owned_cleanup=True, status="STOPPED", code="WORKLOAD_RESULT_UNKNOWN")
        observe(cleanup=True); stable(); receipt_frame_guard()
        receipt_raw = (read_receipt(call["call_id"], 1048576) if read_receipt is not None
                       else _sec_read_saved_receipt(admission, call["call_id"], 1048576, io))
        observe(cleanup=True); stable(); receipt_frame_guard()
        saved = _sec_bound_saved_receipt(_sec_json(receipt_raw), driver.asdict(validated_call), outer_original, driver, inner_sha=selection["driver_admission"]["sha256"])
        observe(cleanup=True); stable()
        receipt_frame_guard()
        output.update(saved)
        output.update(terminal)
        return output
    except BaseException as exc:
        # Fixed local codes only; hostile exception __str__/args/class names
        # are never serialized. No failure means zero writes/rollback by default.
        allowed = {"DIAGNOSTIC_INNER_ADMISSION_BINDING", "TERMINAL_PARENT_BINDING", "TERMINAL_PARENT_FINALIZATION", "EARLY_ORIGIN_REQUIRED", "NATIVE_CLOCK_READER", "ROOT_INNER_ADMISSION_BINDING", "OWNED_RECEIPT_CHANGED", "OUTER_CLOCK_INVALID", "OUTER_DEADLINE", "OUTER_FRAME_BINDING", "SAVED_PARENT_FINALIZATION", "SOURCE_FRAME_CHANGED", "DRIVER_ADMISSION_INVALID", "WRAPPER_BINDING_PIN", "WRAPPER_ADMISSION_BINDING", "INPUT_BOUND", "DUPLICATE_KEY", "NONFINITE_JSON", "ABSOLUTE_PATH", "REGULAR_FILE", "FILE_BOUND", "SELECTION_PIN", "SELECTION_FIELDS", "BASE_BINDING", "SOURCE_ALLOWLIST", "JOB_ARGUMENTS", "ARTIFACT_FIELDS", "SOURCE_PIN", "BINDING_FIELDS", "BINDING_PIN", "ADMISSION_REQUIRED", "CALL_IDENTITY", "EXECUTED_SOURCE_BINDING", "CAPS_BINDING", "STAGE_FIELDS", "SUPERVISOR_PIN", "FINITE_ATTEMPT_ALREADY_USED", "OWNED_RECEIPT_BINDING", "OWNED_STOP_REAP_EOF_CLOSE_REQUIRED", "DIAGNOSTIC_RECEIPT_BINDING", "RECEIPT_ROOT_BINDING", "RECEIPT_FILE_BINDING", "RECEIPT_READ_BOUND", "RECEIPT_FD_REBOUND", "RECEIPT_CLOSE_UNVERIFIED"}
        if _SEC_ONLY_USED:
            output["status"] = "UNKNOWN"
        if type(exc) is SecOnlyRefused and len(exc.args) == 1 and type(exc.args[0]) is str and exc.args[0] in allowed:
            output["code"] = exc.args[0]
        else:
            output["code"] = "UNKNOWN_FAILURE"
        return output


def _hermes_sec_cli(argv, *, entry_t0=None, clock=None):
    from time import monotonic
    reader = monotonic if clock is None else clock
    try:
        origin = reader() if entry_t0 is None else entry_t0
    except BaseException:
        print('{"schema":"hermes-sec-only-result-v1","status":"REFUSED","code":"OUTER_CLOCK_INVALID","workload_success":false}')
        return 2
    import json
    try:
        _sec_require(type(argv) is list and argv.count("--sec-only") == 1, "CLI_ARGUMENTS")
        args = [a for a in argv if a != "--sec-only"]
        _sec_require(len(args) == 4 and set(args[::2]) == {"--sec-selection", "--sec-selection-sha256"}, "CLI_ARGUMENTS")
        values = dict(zip(args[::2], args[1::2]))
        _sec_require(entry_t0 is not None, "EARLY_ORIGIN_REQUIRED")
        observe = _sec_outer_clock(origin, reader)
        observe()
        selection_raw = _sec_read(values["--sec-selection"], 131072)
        observe()
        receipt = run_sec_only_job(selection_raw, values["--sec-selection-sha256"], entry_t0=origin, clock=reader)
    except BaseException:
        receipt = {"schema": "hermes-sec-only-result-v1", "status": "REFUSED", "code": "CLI_OR_INPUT_REFUSED", "workload_success": False}
    print(json.dumps(receipt, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return 0 if receipt.get("status") == "STOPPED" and receipt.get("owned_cleanup") is True else 2


if __name__ == "__hermes_sec_only__":
    raise SystemExit(_hermes_sec_cli(globals().get("HERMES_SEC_ONLY_ARGS"),
                     entry_t0=globals().get("HERMES_SEC_OUTER_ORIGIN"),
                     clock=globals().get("HERMES_SEC_CLOCK")))


# Selected FINRA reuses the finite owned-worker driver. It never invokes tick,
# constructs a puller in this process, or delegates retries to this caller.
_FINRA_SELECTED_BASE = "c4662193a1b526e4ccc149b0394bfc52754e3eec"
_FINRA_SELECTED_PINS = {"driver": "4bb5204c2fd45b1714688dda1d892b1192ef7be1217e4d3abc350b42fc27ee68",
                        "producer": "76579df966612c7f215a9f9c27251574ec13488a31c1e40221dbe09a651337f5"}
_FINRA_SELECTED_USED = False
_FINRA_SELECTED_CODES = frozenset({
    "ENTRY_CLOCK", "CLOCK_BINDING", "ENTRY_BOUND", "SELECTION_PIN", "SELECTION_SHAPE",
    "SELECTION_BASE", "SOURCE_ALLOWLIST", "JOB_ARGUMENTS", "SOURCE_ARTIFACTS",
    "ARTIFACT_IDENTITY", "CALLER_ALREADY_CONSUMED", "DEPENDENCY_SOURCE_PENDING",
    "SOURCE_PIN", "ADMISSION_PIN", "ADMISSION_BINDING", "PRODUCER_BINDING", "RESULT_MUTATED",
    "WORKFLOW_RESULT_BINDING", "WORKFLOW_OUTCOME", "WORKFLOW_RESULT_SCHEMA_PENDING"})


class FinraSelectedRefused(ValueError):
    pass


def _finra_require(value, code):
    if value is not True:
        raise FinraSelectedRefused(code)


def _finra_clock(origin, clock, runtime=None):
    """Sticky observed entry bound; callbacks cannot grant another 450s."""
    from math import isfinite
    _finra_require(type(origin) is float and isfinite(origin)
                   and isfinite(origin + 450) and callable(clock), "ENTRY_CLOCK")
    reader = clock
    last = origin
    invalid = False
    def observe():
        nonlocal last, invalid
        if invalid:
            raise FinraSelectedRefused("ENTRY_CLOCK")
        try:
            if runtime is not None:
                _finra_require(runtime.clock is reader, "CLOCK_BINDING")
            value = reader()
            if runtime is not None:
                _finra_require(runtime.clock is reader, "CLOCK_BINDING")
            _finra_require(type(value) in (int, float) and isfinite(value)
                           and last <= value < origin + 450, "ENTRY_BOUND")
            last = value
            return value
        except BaseException:
            invalid = True
            raise
    observe()
    return observe


def _finra_selection(raw, expected_sha):
    import hashlib
    import re
    _finra_require(type(raw) is bytes and len(raw) <= 65536
                   and type(expected_sha) is str and re.fullmatch("[0-9a-f]{64}", expected_sha) is not None
                   and hashlib.sha256(raw).hexdigest() == expected_sha, "SELECTION_PIN")
    s = _sec_json(raw, 65536)
    _finra_require(type(s) is dict and set(s) == {
        "schema", "base", "source", "module", "class", "method", "kwargs",
        "artifacts", "workflow_admission"}, "SELECTION_SHAPE")
    _finra_require(s["schema"] == "hermes-finra-selected-v1" and s["base"] == _FINRA_SELECTED_BASE,
                   "SELECTION_BASE")
    _finra_require(s["source"] == "finra_short_volume"
                   and s["module"] == "ingestion.altdata.finra_short_volume"
                   and s["class"] == "FINRAShortVolumePuller" and s["method"] == "pull", "SOURCE_ALLOWLIST")
    kwargs = {"trade_date": "2026-10-13",
              "url": "https://cdn.finra.org/equity/regsho/daily/CNMSshvol20261013.txt",
              "market_prefix": "CNMS", "dry_run": False}
    _finra_require(type(s["kwargs"]) is dict
                   and _sec_canonical(s["kwargs"]) == _sec_canonical(kwargs), "JOB_ARGUMENTS")
    _finra_require(type(s["artifacts"]) is dict and set(s["artifacts"]) == {"driver", "producer"}, "SOURCE_ARTIFACTS")
    for item in list(s["artifacts"].values()) + [s["workflow_admission"]]:
        _finra_require(type(item) is dict and set(item) == {"path", "sha256"}
                       and type(item["path"]) is str and item["path"].startswith("/")
                       and type(item["sha256"]) is str and re.fullmatch("[0-9a-f]{64}", item["sha256"]) is not None,
                       "ARTIFACT_IDENTITY")
    return s


def _finra_consent_times(packet):
    from datetime import datetime, timezone
    import re
    times = []
    for key in ("issued_utc", "expires_utc"):
        value = packet.get(key)
        _finra_require(type(value) is str and re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)", value) is not None,
            "ADMISSION_BINDING")
        point = datetime.fromisoformat(value.replace("Z", "+00:00"))
        _finra_require(point.tzinfo == timezone.utc, "ADMISSION_BINDING")
        times.append(point)
    _finra_require(0 < (times[1] - times[0]).total_seconds() <= 7 * 86400, "ADMISSION_BINDING")
    return tuple(times)


def _finra_reduce_result(workflow, admission, expected_sha, origin):
    """Closed safe observations; process cleanup never becomes DB success."""
    from math import isfinite
    from datetime import datetime, timezone
    import hashlib
    import re
    p = _sec_json(admission, 65536)
    _finra_require(type(workflow) is dict and set(workflow) == {
        "schema", "workflow_id", "admission_sha256", "call", "frame", "attempts",
        "reservations", "actual_usage", "db_write_bearing_attempts", "state",
        "terminal_reason", "transaction_ack", "committed_rows", "after", "new_rows",
        "durability", "native_accepted", "live_qualified", "synthetic_only"}
        and workflow["schema"] == "finra-selected-workflow-result-v2"
        and workflow["workflow_id"] == p["workflow_id"]
        and workflow["admission_sha256"] == expected_sha
        and _sec_canonical(workflow["call"]) == _sec_canonical(p["call"]), "WORKFLOW_RESULT_BINDING")
    f = workflow["frame"]
    _finra_require(type(f) is dict and set(f) == {
        "workflow_id", "admission_sha256", "origin", "end", "expires_utc",
        "execution_day", "clock_domain", "clock_pair_monotonic", "clock_pair_utc"}
        and f["workflow_id"] == p["workflow_id"] and f["admission_sha256"] == expected_sha
        and f["expires_utc"] == p["expires_utc"] and f["clock_domain"] == "CLOCK_MONOTONIC"
        and all(type(f[k]) is float and isfinite(f[k]) for k in ("origin", "end", "clock_pair_monotonic"))
        and f["origin"] == origin and f["end"] == origin + 450.
        and origin <= f["clock_pair_monotonic"] < f["end"], "WORKFLOW_RESULT_BINDING")
    _finra_consent_times(p)
    attempts = workflow["attempts"]
    _finra_require(type(attempts) is list and len(attempts) <= 3, "WORKFLOW_RESULT_BINDING")
    summaries = []
    totals = {"exchange_calls": 0, "body_bytes": 0, "http_seconds": 0.0, "wire_gets": "UNKNOWN"}
    write_bearing = 0
    unknown = False
    last_stop = origin
    for index, a in enumerate(attempts, 1):
        h = hashlib.sha256(("FINRA_SELECTED_V2|" + p["workflow_id"] + "|" + str(index)).encode()).hexdigest()[:32]
        attempt_id = h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
        _finra_require(type(a) is dict and set(a) == {
            "outcome", "index", "attempt_id", "start", "stop_end", "state", "phase", "code",
            "db_started", "write_bearing", "commit_attempted", "http_requests", "body_bytes",
            "http_elapsed", "transaction_ack", "committed_rows", "after", "stdout_eof",
            "stderr_eof", "cleanup_closed"}, "WORKFLOW_RESULT_BINDING")
        _finra_require(type(a["index"]) is int and a["index"] == index
                       and type(a["attempt_id"]) is str
                       and a["attempt_id"] == attempt_id
                       and re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", a["attempt_id"]) is not None
                       and type(a["start"]) is float and isfinite(a["start"])
                       and type(a["stop_end"]) is float and isfinite(a["stop_end"])
                       and origin <= a["start"] and a["stop_end"] == a["start"] + 150. <= f["end"],
                       "WORKFLOW_RESULT_BINDING")
        # The prior stop deadline may overlap a quicker observed owned closure;
        # actual starts must only be monotonic, not artificially reset or padded.
        _finra_require(a["start"] >= last_stop, "WORKFLOW_RESULT_BINDING")
        last_stop = a["start"]
        _finra_require(type(a["state"]) is str and a["state"] in ("UNKNOWN", "RETRYABLE", "TERMINAL")
                       and type(a["outcome"]) is str and a["outcome"] in (
                           "UNKNOWN", "SUCCESS", "NO_NEW_DATA", "SKIPPED", "FAILED", "PARTIAL")
                       and type(a["phase"]) is str and a["phase"] in ("UNKNOWN", "PRE_DB_TRANSPORT", "TERMINAL")
                       and type(a["code"]) is str and a["code"] in (
                           "UNKNOWN", "NONE", "TRANSPORT_TIMEOUT", "TRANSPORT_CONNECTION", "HTTP_TRANSIENT")
                       and type(a["transaction_ack"]) is str and a["transaction_ack"] in (
                           "UNKNOWN", "NOT_ATTEMPTED", "ACKNOWLEDGED", "ROLLBACK_ACKNOWLEDGED"), "WORKFLOW_OUTCOME")
        for key in ("db_started", "write_bearing", "commit_attempted", "stdout_eof", "stderr_eof", "cleanup_closed"):
            _finra_require(a[key] is None or type(a[key]) is bool, "WORKFLOW_RESULT_BINDING")
        if a["state"] == "UNKNOWN":
            unknown = True
            _finra_require(index == len(attempts), "WORKFLOW_OUTCOME")
        else:
            _finra_require(all(a[key] is True for key in ("stdout_eof", "stderr_eof", "cleanup_closed"))
                           and all(type(a[key]) is bool for key in ("db_started", "write_bearing", "commit_attempted")),
                           "WORKFLOW_OUTCOME")
            _finra_require(type(a["http_requests"]) is int and 0 <= a["http_requests"] <= 1
                           and type(a["body_bytes"]) is int and 0 <= a["body_bytes"] <= 8388608
                           and type(a["http_elapsed"]) in (int, float) and isfinite(a["http_elapsed"])
                           and 0 <= a["http_elapsed"] <= 45, "WORKFLOW_RESULT_BINDING")
            _finra_require((a["write_bearing"] is not True or a["db_started"] is True)
                           and (a["commit_attempted"] is not True or a["write_bearing"] is True), "WORKFLOW_OUTCOME")
            write_bearing += int(a["write_bearing"])
            totals["exchange_calls"] += a["http_requests"]
            totals["body_bytes"] += a["body_bytes"]
            totals["http_seconds"] += a["http_elapsed"]
        if a["state"] == "RETRYABLE":
            _finra_require(a["phase"] == "PRE_DB_TRANSPORT"
                           and a["code"] in ("TRANSPORT_TIMEOUT", "TRANSPORT_CONNECTION", "HTTP_TRANSIENT")
                           and all(a[k] is False for k in ("db_started", "write_bearing", "commit_attempted"))
                           and a["transaction_ack"] == "NOT_ATTEMPTED"
                           and type(a["committed_rows"]) is int and a["committed_rows"] == 0,
                           "WORKFLOW_OUTCOME")
        if index < len(attempts):
            _finra_require(a["state"] == "RETRYABLE", "WORKFLOW_OUTCOME")
        _finra_require(a["committed_rows"] is None or
                       (type(a["committed_rows"]) is int and 0 <= a["committed_rows"] <= 20000), "WORKFLOW_OUTCOME")
        _finra_require(a["transaction_ack"] != "ACKNOWLEDGED" or
                       (a["commit_attempted"] is True and type(a["committed_rows"]) is int), "WORKFLOW_OUTCOME")
        summaries.append({key: a[key] for key in a if key != "after"})
    _finra_require(_sec_canonical(workflow["reservations"]) == _sec_canonical({
        "gets": len(attempts), "body_bytes": len(attempts) * 8388608,
        "http_seconds": len(attempts) * 45}), "WORKFLOW_RESULT_BINDING")
    usage = workflow["actual_usage"]
    _finra_require(type(usage) is dict and set(usage) == {"exchange_calls", "body_bytes", "http_seconds", "wire_gets"}
                   and usage["wire_gets"] == "UNKNOWN", "WORKFLOW_RESULT_BINDING")
    if unknown:
        _finra_require(all(usage[k] is None for k in ("exchange_calls", "body_bytes", "http_seconds"))
                       and workflow["db_write_bearing_attempts"] is None, "WORKFLOW_OUTCOME")
    else:
        _finra_require(type(usage["exchange_calls"]) is int and type(usage["body_bytes"]) is int
                       and type(usage["http_seconds"]) in (int, float) and isfinite(usage["http_seconds"])
                       and usage == totals and type(workflow["db_write_bearing_attempts"]) is int
                       and workflow["db_write_bearing_attempts"] == write_bearing <= 1, "WORKFLOW_OUTCOME")
    state = workflow["state"]
    _finra_require(type(state) is str and state in ("COMPLETE", "EXHAUSTED", "TERMINAL", "UNKNOWN", "HOLD")
                   and type(workflow["terminal_reason"]) is str and workflow["terminal_reason"] in (
                       "OUTCOME_UNKNOWN", "ATTEMPT_TERMINAL", "RETRY_EXHAUSTED", "NO_START_RESERVE", "WORKFLOW_FAILURE"),
                   "WORKFLOW_OUTCOME")
    if state == "COMPLETE":
        _finra_require(bool(attempts) and not unknown and attempts[-1]["state"] == "TERMINAL"
                       and attempts[-1]["outcome"] in ("SUCCESS", "NO_NEW_DATA", "SKIPPED")
                       and workflow["terminal_reason"] == "ATTEMPT_TERMINAL", "WORKFLOW_OUTCOME")
    if attempts and not unknown:
        _finra_require(type(workflow["transaction_ack"]) is str
                       and workflow["transaction_ack"] == attempts[-1]["transaction_ack"]
                       and type(workflow["committed_rows"]) is type(attempts[-1]["committed_rows"])
                       and workflow["committed_rows"] == attempts[-1]["committed_rows"], "WORKFLOW_OUTCOME")
    if unknown:
        _finra_require(state == "UNKNOWN" and workflow["transaction_ack"] == "UNKNOWN"
                       and workflow["committed_rows"] is None and workflow["after"] == "UNKNOWN", "WORKFLOW_OUTCOME")
    _finra_require(workflow["native_accepted"] is False and workflow["live_qualified"] is False
                   and type(workflow["synthetic_only"]) is bool
                   and workflow["new_rows"] == "UNKNOWN" and workflow["durability"] == "UNKNOWN", "WORKFLOW_OUTCOME")
    return {"workflow_id": workflow["workflow_id"], "admission_sha256": expected_sha,
            "frame": dict(f), "state": state, "terminal_reason": workflow["terminal_reason"],
            "attempts": summaries, "reservations": dict(workflow["reservations"]),
            "actual_usage": dict(usage), "db_write_bearing_attempts": workflow["db_write_bearing_attempts"],
            "observed_transaction_ack": workflow["transaction_ack"],
            "observed_committed_rows": workflow["committed_rows"],
            "new_rows": "UNKNOWN", "durability": "UNKNOWN", "synthetic_only": workflow["synthetic_only"]}


def run_finra_selected_job(selection, expected_sha, *, entry_origin=None,
                           clock=None, read_bytes=None, runtime=None, native_factory=None):
    """One driver invocation after exact source/admission validation.

    No caller retry, source rewrite, scheduler cycle or authority renewal exists.
    The driver's workflow owns its bounded pre-DB transient retry decision.
    """
    global _FINRA_SELECTED_USED
    import hashlib
    import time
    result = {"schema": "hermes-finra-selected-result-v1", "status": "REFUSED",
              "code": "UNKNOWN_FAILURE", "native_accepted": False,
              "live_qualified": False, "workload_success": False,
              "commit": "UNKNOWN", "driver_invocations": 0,
              "workflow": None}
    invoked = False
    try:
        reader = time.monotonic if clock is None else clock
        observed = _finra_clock(entry_origin, reader, runtime)
        def observe():
            if runtime is not None:
                _finra_require(runtime.clock is reader, "CLOCK_BINDING")
            value = observed()
            if runtime is not None:
                _finra_require(runtime.clock is reader, "CLOCK_BINDING")
            return value
        s = _finra_selection(selection, expected_sha)
        pins = dict(_FINRA_SELECTED_PINS)
        observe()
        _finra_require(not _FINRA_SELECTED_USED, "CALLER_ALREADY_CONSUMED")
        _finra_require(all(type(v) is str and len(v) == 64 for v in pins.values()),
                       "DEPENDENCY_SOURCE_PENDING")
        for role in ("driver", "producer"):
            _finra_require(s["artifacts"][role]["sha256"] == pins[role], "SOURCE_PIN")
        read = _sec_read if read_bytes is None else read_bytes
        files = {}
        for role in ("driver", "producer"):
            item = s["artifacts"][role]
            observe(); data = read(item["path"], 1048576); observe()
            _finra_require(type(data) is bytes and len(data) <= 1048576
                           and hashlib.sha256(data).hexdigest() == item["sha256"], "SOURCE_PIN")
            files[role] = data
        item = s["workflow_admission"]
        observe(); admission = read(item["path"], 65536); observe()
        _finra_require(type(admission) is bytes and len(admission) <= 65536
                       and hashlib.sha256(admission).hexdigest() == item["sha256"], "ADMISSION_PIN")
        packet = _sec_json(admission, 65536)
        _finra_require(type(packet) is dict and packet.get("schema") == "finra-selected-workflow-admission-v2"
                       and packet.get("status") == "ADMITTED"
                       and type(packet.get("runtime")) is dict and type(packet.get("call")) is dict,
                       "ADMISSION_BINDING")
        for role in ("driver", "producer"):
            _finra_require(packet["runtime"].get(role) == s["artifacts"][role]["path"], "ADMISSION_BINDING")
        _finra_require(packet["runtime"].get("driver_sha256") == pins["driver"]
                       and packet["call"].get("source_sha256") == pins["producer"]
                       and all(key in packet["call"] for key in s["kwargs"])
                       and _sec_canonical({key: packet["call"][key] for key in s["kwargs"]})
                           == _sec_canonical(s["kwargs"]), "ADMISSION_BINDING")
        _finra_consent_times(packet)
        observe()
        driver = _sec_load(s["artifacts"]["driver"]["path"], files["driver"],
                           pins["driver"])
        observe()
        _finra_require(driver.PRODUCER_SHA == pins["producer"], "PRODUCER_BINDING")
        admitted = driver.validate_selected_admission(admission, item["sha256"])
        observe()
        _finra_require(_sec_canonical(admitted) == _sec_canonical(packet), "ADMISSION_BINDING")
        # Only the explicit selected route reaches this exact source-bound
        # constructor. Its real preflight still checks target roles/package/
        # cgroup/receipt roots/expiry and consumes an exclusive workflow marker.
        # No source receipt or boolean can turn those checks into qualification.
        _finra_require(runtime is None or native_factory is None, "ADMISSION_BINDING")
        if runtime is None:
            runtime = (driver.NativeRuntime if native_factory is None else native_factory)()
        observe()
        _FINRA_SELECTED_USED = True
        invoked = True
        result["driver_invocations"] = 1
        workflow = driver.run_selected_workflow(admission, item["sha256"],
                                               entry_origin=entry_origin, runtime=runtime)
        observe()
        frozen = _sec_canonical(workflow)
        driver.validate_selected_result(workflow, admission, item["sha256"], entry_origin)
        observe()
        _finra_require(_sec_canonical(workflow) == frozen, "RESULT_MUTATED")
        reduced = _finra_reduce_result(workflow, admission, item["sha256"], entry_origin)
        observe()
        _finra_require(_sec_canonical(workflow) == frozen, "RESULT_MUTATED")
        result.update(status=reduced["state"], code="WORKFLOW_OBSERVED", workflow=reduced)
    except BaseException as exc:
        code = (exc.args[0] if type(exc) is FinraSelectedRefused and len(exc.args) == 1
                and type(exc.args[0]) is str and exc.args[0] in _FINRA_SELECTED_CODES else "UNKNOWN_FAILURE")
        result.update(status="UNKNOWN" if invoked else "REFUSED", code=code)
    return result


def _hermes_finra_cli(argv, *, entry_origin=None, clock=None):
    import json
    import time
    result = {"schema": "hermes-finra-selected-result-v1", "status": "REFUSED",
              "code": "CLI_OR_INPUT_REFUSED", "native_accepted": False,
              "live_qualified": False, "workload_success": False,
              "commit": "UNKNOWN", "driver_invocations": 0, "workflow": None}
    try:
        reader = time.monotonic if clock is None else clock
        observe = _finra_clock(entry_origin, reader)
        _finra_require(type(argv) is list and len(argv) == 5 and argv[0] == "--finra-only"
                       and argv[1] == "--finra-selection" and argv[3] == "--finra-selection-sha256",
                       "CLI_ARGUMENTS")
        observe(); raw = _sec_read(argv[2], 65536); observe()
        result = run_finra_selected_job(raw, argv[4], entry_origin=entry_origin, clock=reader)
    except BaseException:
        pass
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0 if result["status"] == "COMPLETE" else 2


if __name__ == "__hermes_finra_only__":
    raise SystemExit(_hermes_finra_cli(globals().get("HERMES_FINRA_ONLY_ARGS", []),
                                     entry_origin=globals().get("HERMES_FINRA_ENTRY_ORIGIN"),
                                     clock=globals().get("HERMES_FINRA_CLOCK")))

import calendar
import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from loguru import logger as log
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

# CFTC's own publication clock (15:30 America/New_York) — used by the
# cftc_cot release gate below, kept in sync with cftc_markets._ET.
_CFTC_ET = ZoneInfo("America/New_York")

# GRID-YF-CLOSE-REPAIR-20260926 fix #2 ("timeout honestly sized"): without
# an explicit start_date, YFinancePuller.pull_all() defaults to "1990-01-01"
# — a full-history download for every ticker on every call. That default is
# meant for deliberate, explicitly authorised backfills only (see that
# method's own docstring); the routine scheduled call below was calling it
# with no kwargs at all, so every 4-hourly tick re-downloaded ~35 years of
# history for 70 tickers. That is what regularly overran the old 120s
# timeout_s and produced the orphaned daemon thread that let a later tick's
# concurrent call race it (the confirmed April 2026 contamination
# mechanism). A short rolling window keeps the routine scheduled download
# small and fast while still covering weekends/holidays with margin.
YFINANCE_SCHEDULED_LOOKBACK_DAYS = 10


def _yfinance_incremental_start() -> str:
    """Recent-window start_date for the routine scheduled yfinance pull.

    Evaluated at call time (see _run_puller's callable-kwargs resolution
    below), not once when PULLER_REGISTRY is built at import — otherwise
    this would freeze at whatever date the process happened to start.
    """
    return (
        datetime.now(timezone.utc).date()
        - timedelta(days=YFINANCE_SCHEDULED_LOOKBACK_DAYS)
    ).isoformat()


# ── Wave 1 activation helpers (2026-09-27) ──────────────────────────────
# Registers pullers merged in PR #564 (FINRA short volume + SEC FTD) and
# PR #553 (EIA + LME) that landed "contract-first, unregistered". Both PRs
# shipped an *unapplied* .patch file under docs/handoffs/2026-09-18/ that
# targets ``ingestion/scheduler.py::_get_pullers_for_group`` -- but that
# function is wired to ``cli.py`` / ``orchestration/tasks.py`` /
# ``scripts/run_full_pipeline.py`` only, none of which run on a schedule
# (no cron calls run_full_pipeline; CLAUDE.md #39 calling scheduler.py
# "the authoritative scheduler" is aspirational docs, not current fact).
# The scheduler Hermes actually ticks every cycle is THIS module's
# PULLER_REGISTRY, via scripts/hermes_operator.py's main loop ("3. Smart
# ingestion") -> SmartScheduler.tick(). So these pullers are registered
# here instead, adapted rather than patch-applied verbatim.


def _finra_short_volume_trade_date() -> date:
    """Most recent trade date whose Reg SHO daily short-volume file
    should already be published.

    Evaluated at call time (like ``_yfinance_incremental_start`` above),
    not once at import. FINRA posts each trading day's file that same
    evening -- the task/ops cadence for this source is "daily after
    ~18:00 ET" -- so before that cutoff the prior trading day's file is
    the latest one that can exist yet.

    Bounded, documented limitation: this rolls back over weekends only,
    with no market-holiday calendar. A market holiday is NOT the same
    case as the weekend roll above: FINRA's "still publishes a
    header+trailer-only file for no-trading days" guarantee (module
    docstring quote) is about a file that GOT published, just with zero
    data rows -- it says nothing about whether a file exists AT ALL for a
    day the exchanges never opened. In practice a holiday trade_date can
    come back as a plain HTTP 403/404 (no file was ever created for that
    date), which ``FINRAShortVolumePuller.pull``/``pull_recent`` now
    treats as SKIPPED rather than a failure (see that method's
    docstring) -- so a holiday landing here just means one extra SKIPPED
    date in the catch-up window, not an error.
    """
    try:
        from zoneinfo import ZoneInfo

        now_et = datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        # Bounded fallback if tzdata is unavailable: UTC-4 approximates ET
        # closely enough for a same-day/prior-day weekday roll. Never used
        # for anything PIT-sensitive -- obs_date always comes from the
        # fetched file's own "Date" column, not from this clock.
        now_et = datetime.now(timezone.utc) - timedelta(hours=4)
    trade_date = now_et.date()
    if now_et.hour < 18:
        trade_date -= timedelta(days=1)
    while trade_date.weekday() >= 5:  # Sat=5, Sun=6 -- no trading, no file
        trade_date -= timedelta(days=1)
    return trade_date


def _sec_ftd_latest_published_half(today: date | None = None) -> dict[str, str]:
    """Roll to the most recently PUBLISHED SEC FTD half-month period.

    Per docs/handoffs/2026-09-18/fable-w5b-source-contracts.md (quoting
    SEC): "The first half ... at the end of the month. The second half
    ... at about the 15th of the next month." This walks back over the
    last few half-months and returns the most recent whose approximate
    publish date has already passed. Re-requesting an already-ingested
    half-month is a harmless no-op -- SECFTDPuller skips obs_dates it has
    already stored -- so drifting a few days early/late on the exact SEC
    publish day is bounded, not silently wrong.
    """
    today = today or date.today()
    candidates: list[tuple[date, str, str]] = []
    for back in range(3):
        total = (today.year * 12 + (today.month - 1)) - back
        y, m = divmod(total, 12)
        m += 1
        yyyymm = f"{y:04d}{m:02d}"
        last_day = calendar.monthrange(y, m)[1]
        publish_a = date(y, m, last_day)
        next_total = y * 12 + (m - 1) + 1
        ny, nm = divmod(next_total, 12)
        nm += 1
        publish_b = date(ny, nm, 15)
        candidates.append((publish_a, yyyymm, "a"))
        candidates.append((publish_b, yyyymm, "b"))
    published = [c for c in candidates if c[0] <= today] or [min(candidates)]
    published.sort()
    _pub_date, yyyymm, half = published[-1]
    return {"yyyymm": yyyymm, "half": half}


def _sec_ftd_published_halves_since(
    lookback_months: int = 2, today: date | None = None
) -> list[dict[str, str]]:
    """Every published SEC FTD half-month period in the last ~lookback_months
    months, oldest first.

    Same publish-date model as :func:`_sec_ftd_latest_published_half` (see
    its docstring for the exact SEC quote this rolls against), but returns
    ALL published halves in the window instead of only the most recent
    one -- used by :class:`_SECFTDSchedulerAdapter` to feed
    ``SECFTDPuller.pull_recent``'s catch-up loop, so a missed tick (a
    deploy restart mid-tick, a transient network failure, the source
    simply not existing yet in ``source_catalog`` on the day it was first
    registered) does not silently drop a whole half-month forever the way
    a single "just the latest half" call would.
    """
    today = today or date.today()
    candidates: list[tuple[date, str, str]] = []
    for back in range(lookback_months + 2):  # +2 months slack: a half's
        # publish date can land in the month AFTER the one it covers.
        total = (today.year * 12 + (today.month - 1)) - back
        y, m = divmod(total, 12)
        m += 1
        yyyymm = f"{y:04d}{m:02d}"
        last_day = calendar.monthrange(y, m)[1]
        publish_a = date(y, m, last_day)
        next_total = y * 12 + (m - 1) + 1
        ny, nm = divmod(next_total, 12)
        nm += 1
        publish_b = date(ny, nm, 15)
        candidates.append((publish_a, yyyymm, "a"))
        candidates.append((publish_b, yyyymm, "b"))
    cutoff = today - timedelta(days=31 * lookback_months)
    published = sorted(c for c in candidates if cutoff <= c[0] <= today)
    return [{"yyyymm": yyyymm, "half": half} for (_pub, yyyymm, half) in published]


class _LMEWarehouseSchedulerAdapter:
    """Fetch-then-save shim for LMEWarehousePuller.

    NOT in ``PULLER_REGISTRY`` (2026-09-27 review): both the JSON-probe and
    HTML-report LME URLs returned HTTP 403 from grid-svr when checked live
    -- registering this would just burn a slot every 24h returning
    ``{"fetched": 0, ...}`` forever. Kept defined (unregistered) rather
    than deleted so the fetch-then-save shape is still available/testable
    if a working LME endpoint is found later; see
    ``ingestion/altdata/lme_warehouse.py``'s module docstring for the
    fallback chain this wraps.

    ``LMEWarehousePuller.pull()`` only fetches/parses -- it does not write
    to ``raw_series`` on its own (see its ``save_to_db()``). Every
    PULLER_REGISTRY entry assumes ``getattr(instance, method)(**kwargs)``
    both fetches AND writes, so a bare registration would "succeed" every
    tick while inserting zero rows. Carries forward the adapter drafted in
    the unapplied ``fable-w5-scheduler-registration.patch`` (written
    against ``ingestion/scheduler.py``), wired here instead -- see the
    module comment above for why.
    """

    def __init__(self, db_engine: Engine) -> None:
        from ingestion.altdata.lme_warehouse import LMEWarehousePuller

        self._puller = LMEWarehousePuller(db_engine)

    def pull(self) -> dict[str, Any]:
        snapshots = self._puller.pull()
        inserted = self._puller.save_to_db(snapshots)
        return {
            "status": "SUCCESS",
            "fetched": len(snapshots),
            "inserted": inserted,
            "source": self._puller._last_source,
        }


class _OptionsSchedulerAdapter:
    """Full-universe options runner with cooperative, bounded cancellation.

    The measured ~10 minute universe needs more than the old 180 second
    ceiling. This call may occupy one Hermes tick for up to 15 minutes;
    TICK_TIME_BUDGET_S stops scheduling subsequent jobs after it returns.
    Subset and incomplete runs retain committed rows but never claim the
    entire source fresh. The puller also uses this summary for daily runs.
    """

    SOURCE_NAME = "YFINANCE_OPTIONS"

    def __init__(self, db_engine: Engine) -> None:
        from ingestion.options import OptionsPuller

        self._puller = OptionsPuller(db_engine=db_engine)

    def pull(self, should_continue: Any = None) -> dict[str, Any]:
        return self._puller.pull_all(
            should_continue=should_continue, capture_source="smart_scheduler",
        ).summary


class _SECFTDSchedulerAdapter:
    """Config-gated, half-month-catch-up shim for SECFTDPuller.

    ``SECFTDPuller.pull()`` raises ``RuntimeError`` when
    ``settings.SEC_USER_AGENT`` is unset (SEC requires a descriptive
    contact User-Agent on every request) -- correct fail-closed behaviour
    at the puller layer, but ``SmartScheduler._run_puller`` only treats a
    *returned* ``{"status": "SKIPPED", ...}`` dict specially (see its
    "mitigation 2" docstring); an uncaught exception instead lands as a
    FAILED result with an escalating cooldown. This adapter turns the
    missing-config case into a clean SKIPPED with an operator-actionable
    reason, and — only once configured — feeds
    ``SECFTDPuller.pull_recent`` every published half-month in the last
    ~2 months (see :func:`_sec_ftd_published_halves_since`) instead of
    either the unapplied patch's fixed placeholder
    (``{"yyyymm": "202608", "half": "b"}``) or a single latest-half call
    -- a 24h cadence (see PULLER_REGISTRY) that only ever tried the
    latest half would silently never retry a half that failed or was
    missed.
    """

    def __init__(self, db_engine: Engine) -> None:
        from ingestion.altdata.sec_ftd import SECFTDPuller

        self._puller = SECFTDPuller(db_engine)

    def pull(self) -> dict[str, Any]:
        from config import settings

        if not settings.SEC_USER_AGENT:
            return {
                "status": "SKIPPED",
                "skipped_reason": (
                    "SEC_USER_AGENT not set -- SEC requires a descriptive "
                    "contact User-Agent on every request. Set "
                    "SEC_USER_AGENT in .env (see .env.example, format like "
                    "'GRID research aniksrobot@gmail.com') before this "
                    "puller can run."
                ),
            }
        periods = _sec_ftd_published_halves_since(lookback_months=2)
        return self._puller.pull_recent(periods=periods)


# GRID task A1 (owner-approved 2026-09-27; DST/holiday fix per coordinator
# REQUEST CHANGES on PR #681 @ dd53a3dd): the plain freq_h>=168 cadence let
# cftc_cot fire on whatever hour the previous success happened to land on,
# which could be mid-week — well before that week's CFTC report exists, so
# the pull would silently re-store the prior week's report under a fresh
# pull_timestamp instead of catching the new one. CFTC publishes the COT
# report every Friday at 15:30 America/New_York — 19:30 UTC in EDT but
# 20:30 UTC in EST, and shifted to the next federal business day when a
# holiday falls Tue-Fri of that week. A fixed 19:45 UTC anchor (the first
# version of this gate) is wrong for 4-5 months a year: in EST it fires 45
# minutes BEFORE the report exists, re-stores the prior week under a fresh
# pull_timestamp, and — because that premature run counts as a success at
# or after the (wrong) anchor — the Saturday retry never fires either. This
# version computes the scheduled release_at in America/New_York via
# zoneinfo and cftc_markets.compute_release(), the same holiday-aware rule
# G1 stores on every payload (cftc_markets.ReleaseTime), so DST and holiday
# shifts are handled identically here and there, then adds a 15-minute
# margin (CFTC_RELEASE_MARGIN_MINUTES) past that instant. Due from
# release_at+margin through the end of the next America/New_York calendar
# day (the retry window); never due before release_at+margin or after the
# retry window elapses, regardless of staleness — a missed/failed week
# waits for the next Tuesday report's release rather than firing off-
# schedule. freq_h stays on the registry entry only for the
# overdue-priority sort in _get_due_pullers.
CFTC_RELEASE_MARGIN_MINUTES = 15  # grace past the scheduled 15:30 ET release
CFTC_RELEASE_RETRY_DAYS = 1  # retry through the ET calendar day after release


def _cftc_current_report_date(now: datetime) -> date:
    """The report_date whose scheduled release governs ``now``.

    Chosen by release *anchor*, not weekday arithmetic: the candidates are
    the Tuesday of "this" America/New_York week and the Tuesday of the
    week before, and whichever candidate's
    ``cftc_markets.compute_release().release_at`` is the most recent one
    at-or-before ``now`` wins. Plain "map any Tuesday to itself" arithmetic
    breaks once a holiday shifts a release onto the Monday/Tuesday that
    opens the calendar week *after* the report's own Tuesday: on that
    Tuesday, naive weekday arithmetic would treat it as the start of a
    brand-new (not-yet-due) cycle instead of the retry day for the report
    that just released the day before. Anchoring on the actual release
    time keeps the retry window pointed at the report that was really last
    released, however far its holiday shift moved it. Falls back to the
    current week's own (not-yet-released) Tuesday when neither candidate
    has released yet — the ordinary "too early" case, where the fallback's
    own release_at simply isn't reached, so the gate below still says
    "not due" for the right reason.
    """
    from ingestion.altdata.cftc_markets import compute_release

    today_et = now.astimezone(_CFTC_ET).date()
    this_tuesday = today_et - timedelta(days=(today_et.weekday() - 1) % 7)  # Tue=1

    best_report_date = this_tuesday
    best_release_at: datetime | None = None
    for candidate in (this_tuesday, this_tuesday - timedelta(days=7)):
        release = compute_release(candidate)
        if release.release_at is None or release.release_at > now:
            continue
        if best_release_at is None or release.release_at > best_release_at:
            best_release_at = release.release_at
            best_report_date = candidate
    return best_report_date


def _cftc_cot_is_due(last_success: datetime | None, now: datetime) -> bool:
    """Fail-closed weekly release gate for cftc_cot.

    Due from the current report's scheduled ``release_at`` (holiday- and
    DST-aware, via ``cftc_markets.compute_release``, and selected by
    release anchor via ``_cftc_current_report_date`` so a holiday-shifted
    release is still "current" through its own retry day) through the end
    of the next America/New_York calendar day, as long as no success has
    landed since that release_at. Never due before release_at, and never
    due once the retry window has elapsed, no matter how stale — a missed
    week waits for the next Tuesday report's own release rather than
    firing off-schedule.
    """
    from ingestion.altdata.cftc_markets import compute_release

    report_date = _cftc_current_report_date(now)
    release = compute_release(report_date)
    if release.release_at is None:
        # compute_release() only returns None for a non-Tuesday report_date;
        # _cftc_current_report_date always returns a Tuesday. Unreachable
        # in practice — fail closed defensively if that ever changes.
        return False

    # release.release_at is the CFTC's own 15:30 ET moment, already
    # expressed as a correct tz-aware UTC instant for whichever of
    # EDT/EST applies that day (and for the actual holiday-shifted day,
    # if any) via compute_release(). Adding a fixed 15-minute timedelta to
    # a tz-aware instant is unambiguous (15:45 ET either way) — there is no
    # DST transition anywhere near 15:30/15:45 local time.
    release_at = release.release_at + timedelta(minutes=CFTC_RELEASE_MARGIN_MINUTES)
    retry_window_end = datetime.combine(
        release_at.astimezone(_CFTC_ET).date() + timedelta(days=CFTC_RELEASE_RETRY_DAYS + 1),
        datetime.min.time(),
        tzinfo=_CFTC_ET,
    ).astimezone(timezone.utc)
    if not (release_at <= now < retry_window_end):
        return False

    if last_success is None:
        return True
    if last_success.tzinfo is None:
        last_success = last_success.replace(tzinfo=timezone.utc)
    # Already succeeded at/after this week's release: no retry needed.
    return last_success < release_at


# ── Puller Registry ──────────────────────────────────────────────────────
# Every puller with its import path, method, and expected update frequency.
# Frequency is in hours. "8" means run every 8 hours at most.

PULLER_REGISTRY: list[dict[str, Any]] = [
    # ── Fast domestic (run frequently) ──
    {"name": "yfinance",          "mod": "ingestion.yfinance_pull",       "cls": "YFinancePuller",           "method": "pull_all",  "freq_h": 4,  "timeout_s": 240, "kwargs": {"start_date": _yfinance_incremental_start}},
    {"name": "options",           "mod": "ingestion.smart_scheduler",     "cls": "_OptionsSchedulerAdapter", "method": "pull",      "freq_h": 6, "timeout_s": 900, "stop_margin_s": 60},
    {"name": "coingecko",         "mod": "ingestion.coingecko",           "cls": "CoinGeckoPuller",          "method": "pull_all",  "freq_h": 4,  "timeout_s": 60},
    # fred: 87 series took 91-176 s per pass on 2026-10-06 (grid-scheduler journal); one
    # stalled series costs up to 92 s more (3 x 30 s reads + fedfred's 2 x 1 s waits).
    # FREDPuller.pull_all budgets itself against ingestion.fred.FRED_JOB_BUDGET_S /
    # FRED_DEADLINE_MARGIN_S, which must equal timeout_s / stop_margin_s here (pinned by
    # tests/test_fred_transient_timeout.py): the cooperative callback and FRED's own
    # deadline both fall at 285 s.
    {"name": "fred",              "mod": "ingestion.fred",                "cls": "FREDPuller",               "method": "pull_all",  "freq_h": 12, "timeout_s": 300, "stop_margin_s": 15, "api_key": "FRED_API_KEY"},

    # ── Alt data (daily) ──
    {"name": "insider_filings",   "mod": "ingestion.altdata.insider_filings",   "cls": "InsiderFilingsPuller",     "method": "pull_all",      "freq_h": 12, "timeout_s": 120},
    {"name": "congressional",     "mod": "ingestion.altdata.congressional",     "cls": "CongressionalTradingPuller",      "method": "pull_all",      "freq_h": 24, "timeout_s": 60},
    {"name": "unusual_whales",    "mod": "ingestion.altdata.unusual_whales",    "cls": "UnusualWhalesPuller",      "method": "pull_all",      "freq_h": 12, "timeout_s": 60},
    {"name": "prediction_odds",   "mod": "ingestion.altdata.prediction_odds",   "cls": "PredictionOddsPuller",     "method": "pull_all",      "freq_h": 0.167, "timeout_s": 60},  # 10 min — critical for breaking events
    {"name": "kalshi",            "mod": "ingestion.altdata.kalshi",            "cls": "KalshiPuller",             "method": "pull_all",      "freq_h": 12, "timeout_s": 60},
    {"name": "prediction_pmxt",   "mod": "ingestion.altdata.prediction_pmxt",   "cls": "PmxtPredictionPuller",     "method": "pull",          "freq_h": 12, "timeout_s": 120},
    {"name": "smart_money",       "mod": "ingestion.altdata.smart_money",       "cls": "SmartMoneyPuller",         "method": "pull_all",      "freq_h": 0.167, "timeout_s": 60},  # 10 min — critical for breaking events
    {"name": "fed_liquidity",     "mod": "ingestion.altdata.fed_liquidity",     "cls": "FedLiquidityPuller",       "method": "pull_all",      "freq_h": 12, "timeout_s": 60, "api_key": "FRED_API_KEY"},
    {"name": "etf_flows",         "mod": "ingestion.altdata.institutional_flows","cls": "InstitutionalFlowsPuller", "method": "pull_all",      "freq_h": 24, "timeout_s": 120},
    {"name": "analyst_ratings",   "mod": "ingestion.altdata.analyst_ratings",   "cls": "AnalystRatingsPuller",     "method": "pull_all",      "freq_h": 24, "timeout_s": 60},
    {
        "name": "gdelt",
        "mod": "ingestion.altdata.gdelt",
        "cls": "GDELTPuller",
        "method": "pull_recent",
        "freq_h": 0.083,
        "timeout_s": 60,
        "kwargs": {
            "days_back": 1,
            "max_theme_queries": 4,
            "include_actor_tones": False,
            "include_tensions": False,
            "include_signals": False,
        },
    },  # 5 min — bounded breaking-events lane; full GDELT runs belong in catch-up/backfill
    {
        "name": "news_scraper",
        "mod": "ingestion.altdata.news_scraper",
        "cls": "NewsScraperPuller",
        "method": "pull_all",
        "freq_h": 6,
        "timeout_s": 60,
        "kwargs": {"use_llm": False, "pause_seconds": 0.5},
    },
    {"name": "opportunity",       "mod": "ingestion.altdata.opportunity",       "cls": "OppInsightsPuller","method": "pull_all",      "freq_h": 24, "timeout_s": 60},

    # ── International (daily, but slower) ──
    {"name": "ecb",               "mod": "ingestion.international.ecb",         "cls": "ECBPuller",                "method": "pull_all",      "freq_h": 24, "timeout_s": 180},
    {"name": "bcb",               "mod": "ingestion.international.bcb",         "cls": "BCBPuller",                "method": "pull_all",      "freq_h": 48, "timeout_s": 120},
    {"name": "mas",               "mod": "ingestion.international.mas",         "cls": "MASPuller",                "method": "pull_all",      "freq_h": 48, "timeout_s": 120},
    {"name": "akshare",           "mod": "ingestion.international.akshare_macro","cls": "AKShareMacroPuller",      "method": "pull_all",      "freq_h": 48, "timeout_s": 120},
    {"name": "rbi",               "mod": "ingestion.international.rbi",         "cls": "RBIPuller",                "method": "pull_all",      "freq_h": 168, "timeout_s": 120},

    # ── Weekly ──
    {"name": "oecd",              "mod": "ingestion.international.oecd",        "cls": "OECDPuller",               "method": "pull_all",      "freq_h": 168, "timeout_s": 180},
    {"name": "bis",               "mod": "ingestion.international.bis",         "cls": "BISPuller",                "method": "pull_all",      "freq_h": 168, "timeout_s": 120},
    {"name": "imf",               "mod": "ingestion.international.imf",         "cls": "IMFPuller",                "method": "pull_all",      "freq_h": 168, "timeout_s": 180},
    {"name": "dark_pool",         "mod": "ingestion.altdata.dark_pool",         "cls": "DarkPoolPuller",           "method": "pull_weekly",   "freq_h": 168, "timeout_s": 60},
    {"name": "gov_contracts",     "mod": "ingestion.altdata.gov_contracts",     "cls": "GovContractsPuller",       "method": "pull_all",      "freq_h": 168, "timeout_s": 120},
    {"name": "supply_chain",      "mod": "ingestion.altdata.supply_chain",      "cls": "SupplyChainPuller",        "method": "pull_all",      "freq_h": 168, "timeout_s": 60, "api_key": "FRED_API_KEY"},

    # ── Monthly / slow (run rarely) ──
    {"name": "campaign_finance",  "mod": "ingestion.altdata.campaign_finance",  "cls": "CampaignFinancePuller",    "method": "pull_all",      "freq_h": 720, "timeout_s": 300},
    {"name": "lobbying",          "mod": "ingestion.altdata.lobbying",          "cls": "LobbyingPuller",           "method": "pull_all",      "freq_h": 720, "timeout_s": 120},
    {"name": "export_controls",   "mod": "ingestion.altdata.export_controls",   "cls": "ExportControlsPuller",     "method": "pull_all",      "freq_h": 720, "timeout_s": 60},

    # ── New intelligence sources (from PR merge) ──
    {"name": "fara",              "mod": "ingestion.altdata.fara",              "cls": "FARAPuller",               "method": "pull_all",      "freq_h": 168, "timeout_s": 120},
    {"name": "foia_cables",       "mod": "ingestion.altdata.foia_cables",       "cls": "FOIACablesPuller",         "method": "pull_all",      "freq_h": 168, "timeout_s": 120},

    # ── Corporate registry / asset cross-reference ──
    {"name": "uk_companies",     "mod": "ingestion.altdata.uk_companies_house", "cls": "UKCompaniesHousePuller", "method": "pull_all",      "freq_h": 168, "timeout_s": 120, "api_key": "UK_COMPANIES_HOUSE_KEY"},
    {"name": "opencorporates",   "mod": "ingestion.altdata.opencorporates",     "cls": "OpenCorporatesPuller",   "method": "pull_all",      "freq_h": 168, "timeout_s": 120},
    {"name": "asset_registries", "mod": "ingestion.altdata.asset_registries",  "cls": "AssetRegistryPuller",    "method": "pull_all",      "freq_h": 168, "timeout_s": 120},

    # ── Solana / memecoin scanners (from PR merge) ──
    {"name": "telegram_scanner",  "mod": "ingestion.altdata.telegram_scanner",  "cls": "TelegramScanner",          "method": "pull_all",      "freq_h": 4,  "timeout_s": 60},
    {"name": "discord_scanner",   "mod": "ingestion.altdata.discord_scanner",   "cls": "DiscordScanner",           "method": "pull_all",      "freq_h": 4,  "timeout_s": 60},

    # ── Celestial ──
    {"name": "planetary",         "mod": "ingestion.celestial.planetary",       "cls": "PlanetaryAspectPuller",          "method": "pull_all",      "freq_h": 24, "timeout_s": 30},
    {"name": "lunar",             "mod": "ingestion.celestial.lunar",           "cls": "LunarCyclePuller",         "method": "pull_all",      "freq_h": 24, "timeout_s": 30},
    {"name": "solar",             "mod": "ingestion.celestial.solar",           "cls": "SolarActivityPuller",      "method": "pull_all",      "freq_h": 24, "timeout_s": 30},

    # ── Paid APIs (MUST RUN — user is paying for these) ──
    # tiingo: pull_incremental, not pull_all (stale-sources audit 2026-09-29).
    # pull_all() with no start_date re-pulled every ticker from 2020 and never
    # fit in 120s: every run TIMED OUT and left an orphan thread writing
    # alongside grid-scheduler's own Tiingo run. pull_incremental skips
    # tickers already holding the latest session (so most runs finish in
    # seconds), stops cleanly on the auto-wired should_continue deadline
    # (PARTIAL, retried next tick), and shares one advisory lock with
    # grid-scheduler's daily Tiingo worker (SKIPPED while that holds it).
    {"name": "tiingo",            "mod": "ingestion.tiingo_pull",              "cls": "TiingoPuller",             "method": "pull_incremental", "freq_h": 4,  "timeout_s": 120, "api_key": "TIINGO_API_KEY", "api_key_mode": "env"},
    {"name": "tiingo_news",       "mod": "ingestion.tiingo_news_pull",         "cls": "TiingoNewsPuller",         "method": "pull_all",      "freq_h": 6,  "timeout_s": 120, "api_key": "TIINGO_API_KEY", "api_key_mode": "env"},
    {"name": "tiingo_fundamentals","mod": "ingestion.tiingo_fundamentals_pull","cls": "TiingoFundamentalsPuller", "method": "pull_all",      "freq_h": 24, "timeout_s": 120, "api_key": "TIINGO_API_KEY", "api_key_mode": "env"},
    {"name": "quiverquant",       "mod": "ingestion.altdata.quiverquant",      "cls": "QuiverQuantPuller",        "method": "pull_all",      "freq_h": 12, "timeout_s": 120, "api_key": "QUIVERQUANT_API_KEY", "api_key_mode": "env"},

    # ── Crypto (DexScreener) ──
    # PumpFun is deliberately not registered (E1-V3): the entry named a
    # pull_all that PumpFunPuller never had, so every 6-hour run raised. It is
    # not re-pointed at pull_aggregate_signals because the source is dead as
    # designed: /coins/latest and /coins/king-of-the-hill return 404 on
    # frontend-api-v3 (v2 answers 403), which the puller turns into stored
    # 0.0 "signals", and the two live endpoints are fixed-size pages
    # (limit=1 / limit=50), so pump_live_token_count and pump_graduated_count
    # would be the page size, not a measurement. Checked 2026-09-30.
    {"name": "dexscreener",       "mod": "ingestion.dexscreener",             "cls": "DexScreenerPuller",        "method": "pull_aggregate_signals", "freq_h": 4,  "timeout_s": 60},

    # ── Government / regulatory ──
    # BLSPuller(db_engine, api_key=None): keyword mode, never positional.
    # pull_all is one bounded request (see BLSPuller.pull_all).
    {"name": "bls",               "mod": "ingestion.bls",                     "cls": "BLSPuller",                "method": "pull_all",      "freq_h": 168, "timeout_s": 120, "api_key": "BLS_API_KEY", "api_key_mode": "keyword"},
    {"name": "edgar",             "mod": "ingestion.edgar",                   "cls": "EDGARPuller",              "method": "pull_all",      "freq_h": 24, "timeout_s": 180},
    {"name": "cftc_cot",          "mod": "ingestion.altdata.cftc_cot",        "cls": "CFTCCOTPuller",            "method": "pull_all",      "freq_h": 168, "timeout_s": 120},  # due/not-due decided by _cftc_cot_is_due (holiday/DST-aware release window + 1-day retry), not freq_h — see the GRID task A1 note above _cftc_current_report_date

    # ── Sentiment / alt ──
    {"name": "world_news",        "mod": "ingestion.altdata.world_news",      "cls": "WorldNewsPuller",          "method": "pull_all",      "freq_h": 6,  "timeout_s": 60, "api_key": "WORLDNEWS_API_KEY", "api_key_mode": "env"},
    {"name": "fear_greed",        "mod": "ingestion.altdata.fear_greed",      "cls": "FearGreedPuller",          "method": "pull_all",      "freq_h": 12, "timeout_s": 30},
    {"name": "social_sentiment",  "mod": "ingestion.social_sentiment",        "cls": "SocialSentimentPuller",    "method": "pull_all",      "freq_h": 12, "timeout_s": 60},
    {"name": "polymarket",        "mod": "ingestion.altdata.polymarket",      "cls": "PolymarketPuller",         "method": "pull_all",      "freq_h": 12, "timeout_s": 60},
    {"name": "wiki_history",      "mod": "ingestion.wiki_history",            "cls": "WikiHistoryPuller",        "method": "pull_all",      "freq_h": 24, "timeout_s": 60},

    # ── International (missing) ──
    {"name": "eurostat",          "mod": "ingestion.international.eurostat",   "cls": "EurostatPuller",           "method": "pull_all",      "freq_h": 168, "timeout_s": 180},
    {"name": "kosis",             "mod": "ingestion.international.kosis",     "cls": "KOSISPuller",              "method": "pull_all",      "freq_h": 168, "timeout_s": 120, "api_key": "KOSIS_API_KEY", "api_key_mode": "keyword"},

    # ── Previously unscheduled sources (Phase 1 fix, 2026-04-07) ──
    {"name": "aaii_sentiment",        "mod": "ingestion.altdata.aaii_sentiment",          "cls": "AAIISentimentPuller",       "method": "pull_all",  "freq_h": 168, "timeout_s": 60},
    {"name": "cloudflare_radar",      "mod": "ingestion.altdata.cloudflare_radar_puller", "cls": "CloudflareRadarPuller",     "method": "pull",      "freq_h": 24,  "timeout_s": 120},
    {"name": "marketwatch_news",      "mod": "ingestion.altdata.marketwatch_news",        "cls": "MarketWatchNewsPuller",     "method": "pull_all",  "freq_h": 6,   "timeout_s": 60},
    {"name": "nowcast",               "mod": "ingestion.nowcast_puller",                  "cls": "NowcastPuller",             "method": "pull",      "freq_h": 168, "timeout_s": 60},
    {"name": "sec_edgar_fundamentals","mod": "ingestion.altdata.sec_edgar_company",       "cls": "SECEdgarCompanyPuller",     "method": "pull_all",  "freq_h": 168, "timeout_s": 180},
    {"name": "kalshi_markets",        "mod": "ingestion.altdata.kalshi_markets",           "cls": "KalshiMarketsPuller",      "method": "pull_all",  "freq_h": 12,  "timeout_s": 60},
    {"name": "fed_speeches",          "mod": "ingestion.altdata.fed_speeches",             "cls": "FedSpeechPuller",          "method": "pull_all",  "freq_h": 24,  "timeout_s": 60},
    {"name": "crucix_bridge",         "mod": "ingestion.crucix_bridge",                    "cls": "CrucixBridgePuller",       "method": "pull_all",  "freq_h": 1,   "timeout_s": 60},

    # ── ticker_metrics_daily writer (task #161 fix, 2026-05-17) ──
    # sec_xbrl_shares reads YF:{ticker}:close from raw_series and writes
    # market_cap to ticker_metrics_daily. Was registered in hermes_operator
    # PULLER_CONFIG but missing from this registry, so it never ran from
    # 2026-04-12 onward — leaving blue-chip rows stuck at 03-31/04-08.
    {"name": "sec_xbrl_shares",     "mod": "ingestion.altdata.sec_xbrl_shares",      "cls": "SECXBRLSharesPuller",   "method": "pull_all",  "freq_h": 24,  "timeout_s": 1800, "kwargs": {"limit": 400, "backfill_days": 90}},

    # ── Silent-orphan additions (task #170 fix, 2026-05-17) ──
    # Pullers that were registered in hermes_operator.py _SOURCE_REGISTRY but
    # never made it into this runtime registry. Same class of bug as #161
    # (sec_xbrl_shares). Audited via /tmp/audit_v2.py — all entries are
    # BasePuller-compatible (db_engine=engine ctor + pull/pull_all method).
    # Module-level fn entries (e.g. obsidian, apple_supplier_list, sec_13f_live)
    # are NOT added here because smart_scheduler._build_puller_instance
    # requires a class. Those need their own scheduler path or class wrappers.
    {"name": "cboe",                  "mod": "ingestion.altdata.cboe_indices",       "cls": "CBOEIndicesPuller",          "method": "pull_all",  "freq_h": 24,  "timeout_s": 120},
    {"name": "googletrends",          "mod": "ingestion.altdata.google_trends",      "cls": "GoogleTrendsPuller",         "method": "pull_all",  "freq_h": 24,  "timeout_s": 180, "kwargs": {"days_back": 30}},
    # Static HF corpora: weekly revision check only (see the module's "Revision gate").
    {"name": "hf_financial_news",     "mod": "ingestion.altdata.hf_financial_news",  "cls": "HFFinancialNewsPuller",      "method": "pull_all",  "freq_h": 168, "timeout_s": 300},
    {"name": "ny_fed",                "mod": "ingestion.altdata.nyfed",              "cls": "NYFedPuller",                "method": "pull_all",  "freq_h": 24,  "timeout_s": 120},
    {"name": "nyfed_gscpi",           "mod": "ingestion.altdata.nyfed_gscpi",        "cls": "NYFedGSCPIPuller",           "method": "pull_all",  "freq_h": 24,  "timeout_s": 60},
    {"name": "stocktwits",            "mod": "ingestion.altdata.stocktwits",         "cls": "StockTwitsPuller",           "method": "pull_all",  "freq_h": 12,  "timeout_s": 60},
    {"name": "defillama",             "mod": "ingestion.altdata.defi_llama_puller",  "cls": "DefiLlamaPuller",            "method": "pull_all",  "freq_h": 24,  "timeout_s": 180},
    {"name": "dune",                  "mod": "ingestion.altdata.dune_puller",        "cls": "DunePuller",                 "method": "pull_all",  "freq_h": 6,   "timeout_s": 180, "api_key": "DUNE_API_KEY", "api_key_mode": "keyword"},
    {"name": "repo_market",           "mod": "ingestion.altdata.repo_market",        "cls": "RepoMarketPuller",           "method": "pull_all",  "freq_h": 168, "timeout_s": 120, "api_key": "FRED_API_KEY", "api_key_mode": "first"},
    {"name": "legislation",           "mod": "ingestion.altdata.legislation",        "cls": "LegislationPuller",          "method": "pull_all",  "freq_h": 24,  "timeout_s": 180},
    {"name": "earnings_calendar",     "mod": "ingestion.altdata.earnings_calendar",  "cls": "EarningsCalendarPuller",     "method": "pull_all",  "freq_h": 24,  "timeout_s": 120},
    {"name": "social_attention",      "mod": "ingestion.altdata.social_attention",   "cls": "WikipediaAttentionPuller",   "method": "pull_all",  "freq_h": 24,  "timeout_s": 1800,  "kwargs": {"max_tickers": 100}},
    {"name": "yield_curve_full",      "mod": "ingestion.altdata.yield_curve_full",   "cls": "FullYieldCurvePuller",       "method": "pull_all",  "freq_h": 24,  "timeout_s": 120, "api_key": "FRED_API_KEY", "api_key_mode": "first"},
    {"name": "sec_xbrl_financials",   "mod": "ingestion.altdata.sec_xbrl_financials","cls": "SECXBRLFinancialsPuller",    "method": "pull_all",  "freq_h": 168, "timeout_s": 1800, "kwargs": {"limit": 200}},
    {"name": "fx_rates",              "mod": "ingestion.altdata.fx_rates",           "cls": "FXRatesPuller",              "method": "pull",      "freq_h": 24,  "timeout_s": 120, "kwargs": {"days_back": 7}},
    {"name": "margin_debt",           "mod": "ingestion.altdata.margin_debt",        "cls": "MarginDebtPuller",           "method": "pull",      "freq_h": 168, "timeout_s": 60},
    {"name": "ag_commodity_futures",  "mod": "ingestion.altdata.ag_commodity_futures","cls": "AgCommodityFuturesPuller",  "method": "pull_all",  "freq_h": 24,  "timeout_s": 180},
    {"name": "alphavantage_sentiment","mod": "ingestion.altdata.alphavantage_sentiment","cls": "AlphaVantageSentimentPuller","method": "pull_all","freq_h": 24,  "timeout_s": 180},
    {"name": "ads_index",             "mod": "ingestion.altdata.ads_index",          "cls": "ADSIndexPuller",             "method": "pull_all",  "freq_h": 168, "timeout_s": 60},
    {"name": "baltic_exchange",       "mod": "ingestion.altdata.baltic_dry",         "cls": "BalticDryPuller",            "method": "pull_all",  "freq_h": 24,  "timeout_s": 60,  "api_key": "FRED_API_KEY", "api_key_mode": "first"},
    {"name": "finra_ats",             "mod": "ingestion.altdata.finra_ats",          "cls": "FINRAATSPuller",             "method": "pull_all",  "freq_h": 168, "timeout_s": 120},
    # Held 2026-10-02: name matching is too loose to publish (71,903 matches against 204 actors,
    # mostly "partial" substring hits such as GTC MANAGEMENT LTD. -> Oaktree). See
    # docs/handoffs/2026-10-02/OFFSHORE-LEAKS-MATCH-QUALITY.md for the proposed minimum rule.
    {"name": "offshore_leaks",        "mod": "ingestion.altdata.offshore_leaks",     "cls": "OffshoreLeaksPuller",        "method": "pull",      "freq_h": 720, "timeout_s": 600, "hold_reason": "Match quality under review: partial substring name matches (see docs/handoffs/2026-10-02/OFFSHORE-LEAKS-MATCH-QUALITY.md)"},
    {"name": "wikidata_persons",      "mod": "ingestion.altdata.wikidata_persons",   "cls": "WikidataPersonPuller",       "method": "pull_all",  "freq_h": 168, "timeout_s": 1800},

    # ── Wave 1 activation (2026-09-27): merged-but-unscheduled pullers ──
    # See the "Wave 1 activation helpers" comment above PULLER_REGISTRY for
    # why these are registered here and not via the two .patch files in
    # docs/handoffs/2026-09-18/ (which target the non-live scheduler.py).
    #
    # Restart state is keyed by registry ``name`` and persisted in pull_log
    # (see SmartScheduler._load_state_from_db); the source_catalog row each
    # entry bumps comes from REGISTRY_CATALOG_NAMES below, falling back to
    # the registry name itself. These three names already lower() to their
    # puller classes' SOURCE_NAME ("EIA", "FINRA_SHORT_VOLUME", "SEC_FTD"),
    # so they need no REGISTRY_CATALOG_NAMES entry. LME_Warehouse is
    # deliberately NOT registered here at all -- see
    # _LMEWarehouseSchedulerAdapter's docstring (both LME URLs 403 from
    # grid-svr).
    {"name": "eia",                "mod": "ingestion.altdata.eia_puller",       "cls": "EIAPuller",        "method": "pull",  "freq_h": 24,  "timeout_s": 60, "api_key": "EIA_API_KEY", "api_key_mode": "env"},
    {"name": "finra_short_volume", "mod": "ingestion.altdata.finra_short_volume", "cls": "FINRAShortVolumePuller", "method": "pull_recent", "freq_h": 24, "timeout_s": 60, "kwargs": {"anchor_date": _finra_short_volume_trade_date, "weekdays_back": 5}},
    {"name": "sec_ftd",            "mod": "ingestion.smart_scheduler",         "cls": "_SECFTDSchedulerAdapter", "method": "pull", "freq_h": 24, "timeout_s": 60},
]

# ── Registry name → source_catalog name ─────────────────────────────────
# Stale-sources audit 2026-09-29 (GRID-STALE-SOURCES-AUDIT-20260929 §3-C):
# restart state used to be rebuilt ONLY from source_catalog.last_pull_at
# keyed by ``name.lower()``, and ``_update_last_pull`` wrote back the same
# way. 48 of the 92 registry names match no catalog row under that rule
# (and "defillama" matched an inactive row its puller never writes),
# so every Hermes restart (14 of them on 2026-09-28 alone -- each push to
# main restarts grid-hermes) made those entries look "never run"
# (overdue 9999h), they sorted ahead of every genuinely overdue source,
# and with only MAX_PULLERS_PER_TICK slots per tick bls/eia/cboe/aaii/
# finra_short_volume/sec_ftd/kosis/cftc_cot never got a turn that day.
#
# This map names, for every entry whose registry name does not already
# lower() to it, the source_catalog row its puller class actually writes
# under (the class's own ``SOURCE_NAME``, or the name its hand-rolled
# catalog insert uses). tests/test_smart_scheduler_restart_state.py
# checks every entry against the puller class source, so a renamed
# SOURCE_NAME or a new registry entry can't silently drift again.
#
# It is used for exactly two things:
#   1. ``_update_last_pull`` bumps THIS row on success (so a successful
#      run is visible to the stale-sources alert), and
#   2. a one-time bootstrap of restart state, before this entry has any
#      pull_log history of its own -- see ``_load_state_from_db``.
# Restart state itself comes from pull_log (one row per SmartScheduler
# run, puller_name = SMART_PULL_LOG_PREFIX + registry name), because
# several entries share one catalog row (FRED, yfinance, Kalshi,
# polymarket) and a shared row cannot say when each job last ran.
REGISTRY_CATALOG_NAMES: dict[str, str] = {
    "options":                "YFINANCE_OPTIONS",
    "insider_filings":        "SEC_INSIDER",
    "congressional":          "CONGRESS_TRADING",
    "prediction_odds":        "Polymarket",
    "prediction_pmxt":        "pmxt",
    "smart_money":            "Social_Smart_Money",
    "fed_liquidity":          "FRED",
    "etf_flows":              "INSTITUTIONAL_FLOWS",
    "news_scraper":           "NewsScraperRSS",
    "opportunity":            "OppInsights",
    "ecb":                    "ECB_SDW",
    "bcb":                    "BCB_BR",
    "mas":                    "MAS_SG",
    "oecd":                   "OECD_SDMX",
    "imf":                    "IMF_IFS",
    "dark_pool":              "DARKPOOL",
    "gov_contracts":          "USASPENDING_GOV",
    "campaign_finance":       "FEC_CAMPAIGN_FINANCE",
    "lobbying":               "LOBBYING_DISCLOSURE",
    "export_controls":        "BIS_EXPORT_CONTROLS",
    "fara":                   "FARA_DOJ",
    "uk_companies":           "UK_Companies_House",
    "telegram_scanner":       "Telegram_Solana_Scanner",
    "discord_scanner":        "Discord_Solana_Scanner",
    "planetary":              "PLANETARY_EPHEMERIS",
    "lunar":                  "LUNAR_EPHEMERIS",
    "solar":                  "NOAA_SWPC",
    "edgar":                  "SEC_EDGAR",
    "world_news":             "WorldNewsAPI",
    "social_sentiment":       "SocialSentiment",
    "wiki_history":           "WikiHistory",
    "kalshi_markets":         "KALSHI",
    "fed_speeches":           "FedSpeeches",
    "crucix_bridge":          "Crucix",
    "dune":                   "Dune_Analytics",
    "defillama":              "DeFi_Llama",  # the "defillama" row is inactive; the puller writes DeFi_Llama
    "repo_market":            "FRED",
    "legislation":            "CONGRESS_GOV",
    "earnings_calendar":      "yfinance_earnings",
    "social_attention":       "Wikipedia_Attention",
    "yield_curve_full":       "FRED",
    "fx_rates":               "yfinance",
    "margin_debt":            "FINRA_MARGIN",
    "ag_commodity_futures":   "YFINANCE_COMMODITY_FUTURES",
    "alphavantage_sentiment": "alphavantage_news_sentiment",
    "offshore_leaks":         "ICIJ_OFFSHORE",
}

# pull_log.puller_name prefix for SmartScheduler runs. The prefix keeps
# these rows distinct from grid-scheduler's own pull_log names (it logs a
# "nowcast" and an "SEC_EDGAR_Fundamentals" run of its own, for example).
SMART_PULL_LOG_PREFIX = "smart:"

# pull_log lookback used to rebuild a failure streak (and so the cooldown)
# on restart. Longer than the 24h maximum cooldown in _record_result.
RESTART_FAILURE_LOOKBACK_H = 48


def hold_reason_for(registry_name: str) -> str | None:
    """The registry entry's hold_reason, if held. Other schedulers that run the
    same puller (ingestion/scheduler.py) consult this so a hold is one switch."""
    entry = next((e for e in PULLER_REGISTRY if e["name"] == registry_name), None)
    return entry.get("hold_reason") if entry else None


def catalog_name_for(registry_name: str) -> str:
    """The source_catalog name a registry entry's puller writes under."""
    return REGISTRY_CATALOG_NAMES.get(registry_name, registry_name)


def _cooldown_minutes(consecutive_fails: int) -> int:
    """Exponential backoff: 30min, 1h, 2h, 4h, 8h, 16h, max 24h."""
    return min(30 * (2 ** (max(consecutive_fails, 1) - 1)), 1440)


def _as_utc(value: Any) -> datetime | None:
    """Coerce a DB timestamp (datetime, or ISO text on SQLite) to aware UTC."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


_ROW_COUNT_KEYS = (
    "rows_inserted", "total_inserted", "inserted", "rows_written", "rows",
    "rows_upserted", "records_inserted", "records_written", "articles_inserted",
    "events_inserted", "total_rows", "series_stored", "stored", "raw_series_inserted",
)
# These are write-result envelopes, never provider payloads or counts of tickers.
_RESULT_KEYS = (
    "results", "bills", "hearings", "votes", "member_votes", "registrants",
    "activities", "pac_contributions", "individual_contributions",
)


def _puller_owns_catalog(name: str | None, puller: Any = None) -> bool:
    """Options owns its final bounded catalog publication (#735 contract)."""
    return str(name or "").lower() in {"options", "yfinance_options"} or (
        str(getattr(puller, "SOURCE_NAME", "")).upper() == "YFINANCE_OPTIONS"
    )


def _result_children(out: dict) -> list[Any]:
    return [out[k] for k in _RESULT_KEYS if k in out and isinstance(out[k], (dict, list))]


def _materialization_accounting(out: list) -> tuple[str, int | None, str | None] | None:
    """Consume SEC's coverage summary AND leaves, not a successful prefix.

    This bridges to existing outcome/freshness handling. The immutable helper
    checks declared partitions only; it is not database durability proof.
    """
    marked = [item for item in out if isinstance(item, dict) and "accounting_version" in item]
    if not marked:
        return None
    if len(marked) != 1 or marked[0].get("accounting_version") != "SEC_XBRL_SHARES_V1":
        return OUTCOME_FAILED, None, "invalid materialization accounting version"
    summary = marked[0]
    items = [item for item in out if item is not summary]
    required = ("total", "attempted", "succeeded", "no_data", "failed", "unknown",
                "unattempted", "known_committed_rows", "unknown_commit_counts")
    if summary.get("status") != "SUMMARY" or any(
        key not in summary or type(summary[key]) is not int or summary[key] < 0
        for key in required
    ) or "rows_written" not in summary or type(summary.get("stopped_by_budget")) is not bool:
        return OUTCOME_FAILED, None, "invalid materialization accounting counts"
    if any(not isinstance(item, dict) or item.get("status") == "SUMMARY" for item in items):
        return OUTCOME_FAILED, None, "invalid materialization leaf"
    counts = {"succeeded": 0, "no_data": 0, "failed": 0, "unknown": 0}
    status_keys = {"SUCCESS": "succeeded", "NO_NEW_DATA": "no_data",
                   "FAILED": "failed", "PARTIAL": "unknown"}
    known = unknown_counts = 0
    for item in items:
        key = status_keys.get(item.get("status"))
        if key is None or "rows" not in item:
            return OUTCOME_FAILED, None, "invalid materialization leaf outcome"
        count = item["rows"]
        if count is not None and (type(count) is not int or count < 0):
            return OUTCOME_FAILED, None, "invalid materialization leaf rows"
        if key == "no_data" and count != 0:
            return OUTCOME_FAILED, None, "clean empty materialization has unknown or positive rows"
        if key == "succeeded" and (count is None or count <= 0):
            return OUTCOME_FAILED, None, "materialization success has no known positive rows"
        counts[key] += 1
        if count is None:
            unknown_counts += 1
        else:
            known += count
    if summary["attempted"] != len(items) or any(summary[k] != v for k, v in counts.items()):
        return OUTCOME_FAILED, None, "materialization summary/leaf partition mismatch"
    if summary["known_committed_rows"] != known or summary["unknown_commit_counts"] != unknown_counts:
        return OUTCOME_FAILED, None, "materialization row accounting mismatch"
    rows = None if unknown_counts else known
    if summary["rows_written"] != rows or (
        summary["rows_written"] is not None and type(summary["rows_written"]) is not int
    ):
        return OUTCOME_FAILED, None, "materialization total cannot override unknown committed rows"
    from analysis.materialization_accounting import CoverageDeclaration, CoverageState, diagnose_coverage

    coverage = diagnose_coverage(CoverageDeclaration(
        summary["total"], summary["attempted"], counts["succeeded"], counts["no_data"],
        counts["failed"], counts["unknown"], summary["unattempted"],
    ))
    if coverage.state is CoverageState.INCONSISTENT or coverage.unaccounted_items not in (None, 0):
        return OUTCOME_FAILED, rows, "inconsistent materialization coverage"
    if summary.get("error"):
        return OUTCOME_FAILED, rows, str(summary["error"])
    if coverage.state is not CoverageState.COMPLETE or summary["stopped_by_budget"]:
        return OUTCOME_PARTIAL, rows, "materialization coverage incomplete or deferred"
    if not items:
        return OUTCOME_NO_NEW_DATA, rows, "materialization universe checked, no rows"
    outcome, _leaf_rows, note = _classify_outcome(items)
    return outcome, rows, note


def _extract_rows(out: Any) -> int | None:
    """Reported committed writes; unknown counts stay None, never list length.

    Counts on an aggregate replace its leaf counts. SEC SUMMARY rows are
    metadata when ticker results are present, so they are never added twice.
    """
    summary = getattr(out, "summary", None)
    if isinstance(summary, dict):
        return _extract_rows(summary)
    if isinstance(out, bool) or out is None:
        return None
    if isinstance(out, int):
        return out if out >= 0 else None
    if isinstance(out, dict):
        for key in _ROW_COUNT_KEYS:
            if key in out:
                val = out[key]
                return val if isinstance(val, int) and not isinstance(val, bool) and val >= 0 else None
        children = _result_children(out)
        return _extract_rows(children) if children else None
    if isinstance(out, list):
        materialization = _materialization_accounting(out)
        if materialization is not None:
            return materialization[1]
        items = [i for i in out if not isinstance(i, dict) or i.get("status") != "SUMMARY"]
        if not items:
            items = out  # a summary-only aggregate can report its own count
        counts = [_extract_rows(i) for i in items]
        known = [n for n in counts if n is not None]
        return sum(known) if known else None
    return None


# Only complete runs with known positive writes advance source freshness.
OUTCOME_SUCCESS = "SUCCESS"
OUTCOME_NO_NEW_DATA = "NO_NEW_DATA"
OUTCOME_SKIPPED = "SKIPPED"
OUTCOME_FAILED = "FAILED"
OUTCOME_PARTIAL = "PARTIAL"


def _aggregate_coverage(out: dict, rows: int | None) -> tuple[str, int | None, str] | None:
    """Explicit failure/deferred counts cannot be overridden by SUCCESS.

    Tiingo's incremental envelope counts current, successful and clean no-data
    tickers separately. Its ``fetched`` count excludes current/unattempted
    tickers, and failed_tickers is a capped diagnostic, not another count.
    """
    keys = ("failed", "unattempted", "current", "no_data", "succeeded")
    if not any(k in out for k in ("failed", "failed_tickers", "unattempted", "current")):
        return None
    counts = {k: out.get(k, 0) for k in keys}
    if any(not isinstance(n, int) or isinstance(n, bool) or n < 0 for n in counts.values()):
        return OUTCOME_FAILED, rows, "invalid or unknown coverage counts"
    failed_tickers = out.get("failed_tickers", [])
    if not isinstance(failed_tickers, list):
        return OUTCOME_FAILED, rows, "invalid or unknown failed_tickers coverage"
    failed = max(counts["failed"], len(failed_tickers))
    unattempted = counts["unattempted"]
    checked = counts["current"] + counts["succeeded"] + counts["no_data"]
    detail = f"{checked} items checked, {failed} failed, {unattempted} unattempted"
    # Validate the actual #733 aggregate, including unexplained omissions.
    # Other envelopes may use tickers as a list; it is not a coverage count.
    if "current" in out and "tickers" in out:
        total = out["tickers"]
        if not isinstance(total, int) or isinstance(total, bool) or total < 0:
            return OUTCOME_FAILED, rows, "invalid or unknown ticker coverage"
        if not all(k in out for k in keys):
            return OUTCOME_PARTIAL, rows, "incomplete ticker coverage counts"
        covered = checked + failed + unattempted
        fetched = out.get("fetched")
        if (covered > total or not isinstance(fetched, int) or isinstance(fetched, bool)
                or fetched != counts["succeeded"] + counts["no_data"] + counts["failed"]):
            return OUTCOME_FAILED, rows, "inconsistent ticker coverage counts"
        if covered < total:
            return OUTCOME_PARTIAL, rows, f"{covered} of {total} tickers accounted for: {detail}"
    if failed or unattempted:
        outcome = OUTCOME_PARTIAL if checked or rows or unattempted else OUTCOME_FAILED
        return outcome, rows, detail
    return None


def _classify_outcome(out: Any) -> tuple[str, int | None, str | None]:
    """Normalize committed writes and coverage for every ingestion caller.

    A positive count cannot override errors, deferred work, or unknown
    coverage. UNCHANGED is a completed zero-write check. Opaque returns are
    failures of the write contract, not evidence of freshness.
    """
    summary = getattr(out, "summary", None)
    if isinstance(summary, dict):
        return _classify_outcome(summary)
    rows = _extract_rows(out)
    if isinstance(out, dict):
        status = str(out.get("status") or "").upper()
        note = out.get("error") or out.get("errors") or out.get("reason") or out.get("skipped_reason")
        note = str(note) if note else None
        if status in {"SKIPPED", "SKIP", "DEFERRED"} or out.get("skipped_reason"):
            if rows is not None and rows > 0:
                return OUTCOME_PARTIAL, rows, note or "writes reported by an incomplete run"
            return OUTCOME_SKIPPED, rows, note or "puller reported SKIPPED"
        if status in {"FAILED", "ERROR", "TIMEOUT"}:
            return OUTCOME_FAILED, rows, note or f"puller reported {status}"
        # An explicit PARTIAL can truthfully report unknown committed rows.
        # Keep NULL and its retry semantics; invalid counts and unknown
        # SUCCESS still fail closed before any freshness decision.
        invalid = any(
            k in out and not (status == "PARTIAL" and out[k] is None)
            and (not isinstance(out[k], int) or isinstance(out[k], bool) or out[k] < 0)
            for k in _ROW_COUNT_KEYS
        )
        if invalid:
            return OUTCOME_FAILED, None, "invalid or unknown reported write count"
        children = _result_children(out)
        child_outcome = _classify_outcome(children) if children else None
        coverage = _aggregate_coverage(out, rows)
        if coverage:
            return coverage
        # YFinance's SUCCESS envelope is only a loop-completion claim; its
        # per-ticker outcomes determine coverage and its counts count tickers.
        if status == "PARTIAL" or out.get("stopped_by_budget") or out.get("tickers_not_attempted"):
            if out.get("outcome") in {"error", "no_data"} and not rows:
                return OUTCOME_FAILED, rows, note or str(out["outcome"])
            if "counts" in out and child_outcome and child_outcome[0] == OUTCOME_FAILED:
                return OUTCOME_FAILED, rows, child_outcome[2]
            return OUTCOME_PARTIAL, rows, note or "puller reported PARTIAL"
        if child_outcome and child_outcome[0] in {OUTCOME_FAILED, OUTCOME_PARTIAL, OUTCOME_SKIPPED}:
            return child_outcome[0], rows, child_outcome[2]
        if out.get("error") or out.get("errors") or out.get("outcome") in {"error", "no_data"}:
            return (OUTCOME_PARTIAL if rows else OUTCOME_FAILED), rows, note or str(out.get("outcome"))
        if "succeeded" in out and "total" in out:
            succeeded, total = out["succeeded"], out["total"]
            if not all(isinstance(n, int) and not isinstance(n, bool) and n >= 0 for n in (succeeded, total)) or succeeded > total:
                return OUTCOME_FAILED, rows, "invalid coverage counts"
            if succeeded < total:
                return (OUTCOME_PARTIAL if succeeded or rows else OUTCOME_FAILED), rows, f"{succeeded} of {total} items completed"
        if status in {"UNCHANGED", "NO_NEW_DATA"}:
            if rows not in (None, 0):
                return OUTCOME_FAILED, rows, "unchanged outcome reported positive writes"
            return OUTCOME_NO_NEW_DATA, 0, "run checked, nothing changed"
        if child_outcome and rows is None:
            rows = child_outcome[1]
        if rows is None:
            return OUTCOME_FAILED, None, "puller did not report committed write count"
        if status not in {"", "SUCCESS", "OK", "SUMMARY"}:
            return OUTCOME_FAILED, rows, note or f"unrecognized puller status {status}"
        return (OUTCOME_SUCCESS, rows, None) if rows > 0 else (OUTCOME_NO_NEW_DATA, 0, "run completed, 0 rows written")

    if isinstance(out, list):
        materialization = _materialization_accounting(out)
        if materialization is not None:
            return materialization
        if not out:
            return OUTCOME_SKIPPED, 0, "puller returned no per-item results"
        items = [i for i in out if not isinstance(i, dict) or i.get("status") != "SUMMARY"]
        if not items:
            return _classify_outcome(out[-1])
        classified = [_classify_outcome(i) for i in items]
        statuses = [r[0] for r in classified]
        n_skip = statuses.count(OUTCOME_SKIPPED)
        n_failed = statuses.count(OUTCOME_FAILED)
        n_partial = statuses.count(OUTCOME_PARTIAL)
        n_checked = sum(s in {OUTCOME_SUCCESS, OUTCOME_NO_NEW_DATA} for s in statuses)
        note = next((r[2] for r in classified if r[0] in {OUTCOME_FAILED, OUTCOME_PARTIAL} and r[2]), None)
        if n_skip == len(items):
            reason = next((r[2] for r in classified if r[2]), None)
            return OUTCOME_SKIPPED, 0, f"all {len(items)} items skipped" + (f": {reason}" if reason else "")
        if n_failed or n_partial or n_skip:
            # A sole zero-row PARTIAL attempt with errors is a failed attempt;
            # a clean checked item alongside it makes coverage partial.
            only_failed = not n_checked and not rows and all(
                r[0] in {OUTCOME_FAILED, OUTCOME_SKIPPED} or
                (r[0] == OUTCOME_PARTIAL and isinstance(i, dict) and (i.get("errors") or i.get("error")))
                for i, r in zip(items, classified)
            )
            outcome = OUTCOME_FAILED if only_failed else OUTCOME_PARTIAL
            detail = f"{n_failed + n_partial} of {len(items)} items failed or partial, {n_skip} skipped, {rows if rows is not None else 'unknown'} rows written"
            return outcome, rows, detail + (f": {note}" if note else "")
        if rows is None:
            # All explicit UNCHANGED leaves have a known zero-write meaning.
            if all(r[0] == OUTCOME_NO_NEW_DATA for r in classified):
                return OUTCOME_NO_NEW_DATA, 0, "run checked, nothing changed"
            return OUTCOME_FAILED, None, "puller did not report committed write count"
        return (OUTCOME_SUCCESS, rows, None) if rows > 0 else (OUTCOME_NO_NEW_DATA, 0, "run completed, 0 rows written")

    if isinstance(out, int) and not isinstance(out, bool) and out >= 0:
        return (OUTCOME_SUCCESS, out, None) if out > 0 else (OUTCOME_NO_NEW_DATA, 0, "run completed, 0 rows written")
    return OUTCOME_FAILED, None, "invalid or unknown committed write count"


# How many pullers to run per tick (keeps cycles short)
MAX_PULLERS_PER_TICK = 8

# Flat retry delay after a SKIPPED run (not a failure -- no backoff).
SKIP_RETRY_MINUTES = 30

# Per-tick time budget (seconds) — stop scheduling more if we're over this
TICK_TIME_BUDGET_S = 300  # 5 minutes


class MissingPullerApiKey(Exception):
    """Raised when a registry entry requires an unset API key."""

    def __init__(self, key_name: str) -> None:
        self.key_name = key_name
        super().__init__(f"Missing API key: {key_name}")


class SmartScheduler:
    """Runs only due/stale pullers each tick, with per-source cooldowns.

    Thread-leak caveat (DEV-NOTES H17): a puller that exceeds its
    ``timeout_s`` budget is daemon-detached and reported as ``TIMEOUT``,
    but the underlying ``threading.Thread`` keeps running in the
    background until the process exits. The semaphore is released on
    timeout so a single hung puller cannot starve the others, which
    means orphaned threads do NOT count against
    ``MAX_CONCURRENT_THREADS``. ``self._orphan_thread_count`` tracks the
    cumulative number of orphans for operator-side observability and is
    surfaced through :meth:`get_status` so persistent leaks become
    visible without needing to inspect ``ps``/``/proc``.
    """

    # Maximum number of concurrent puller threads
    MAX_CONCURRENT_THREADS = 10

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        # In-memory tracking: {name: {last_success, last_attempt, consecutive_fails, cooldown_until}}
        self._state: dict[str, dict[str, Any]] = {}
        # Thread concurrency control
        self._thread_semaphore = threading.Semaphore(self.MAX_CONCURRENT_THREADS)
        self._active_threads: set[str] = set()
        self._threads_lock = threading.Lock()
        # Cumulative count of daemon threads that exceeded their timeout
        # and were left running in the background. See class docstring.
        self._orphan_thread_count: int = 0
        self._load_state_from_db()
        self._warn_registry_divergence()

    def _warn_registry_divergence(self) -> None:
        """Detect & log orphan pullers across hermes_operator and this registry.

        Task #170 (2026-05-17) added this guard. Task #179 (2026-05-17)
        consolidated ``_SOURCE_REGISTRY`` to be DERIVED from PULLER_REGISTRY
        + a small explicit ``_SOURCE_EXTRAS`` overlay, so the only remaining
        divergence sources are intentional (fn-based pullers, audit-only
        skip_runtime stubs, and historical alias names). Those live in
        ``hermes_operator._SOURCE_EXTRAS`` and ``_SOURCE_ALIASES`` and are
        filtered out before warning — anything left over IS a regression.
        """
        try:
            from scripts.hermes_operator import (
                _SOURCE_REGISTRY as _CFG,
                _SOURCE_EXTRAS,
                _SOURCE_ALIASES,
            )
        except Exception as exc:
            log.debug("Registry-divergence check skipped: {e}", e=str(exc))
            return
        cfg = set(_CFG.keys())
        reg = {p["name"] for p in PULLER_REGISTRY}
        # Intentional cfg-only entries (fn-based, skip_runtime, not-yet-wired
        # class pullers) live in _SOURCE_EXTRAS, and alias names alias onto
        # a canonical PULLER_REGISTRY entry — neither indicates a bug.
        intentional_cfg_only = set(_SOURCE_EXTRAS.keys()) | set(_SOURCE_ALIASES.keys())
        only_cfg = (cfg - reg) - intentional_cfg_only
        only_reg = reg - cfg
        if only_cfg:
            log.warning(
                "SmartScheduler registry divergence — {n} pullers in "
                "hermes_operator._SOURCE_REGISTRY but NOT in PULLER_REGISTRY "
                "(silent orphans, won't run): {names}",
                n=len(only_cfg), names=sorted(only_cfg),
            )
        if only_reg:
            log.info(
                "SmartScheduler registry note — {n} pullers in PULLER_REGISTRY "
                "but not in hermes_operator._SOURCE_REGISTRY (running but "
                "undocumented in operator): {names}",
                n=len(only_reg), names=sorted(only_reg),
            )
        if not only_cfg and not only_reg:
            log.info(
                "SmartScheduler registry consolidated — _SOURCE_REGISTRY "
                "derived from PULLER_REGISTRY ({n} entries) + {ne} extras "
                "+ {na} aliases. No divergence.",
                n=len(reg), ne=len(_SOURCE_EXTRAS), na=len(_SOURCE_ALIASES),
            )

    def _load_state_from_db(self) -> None:
        """Rebuild restart state for every registry entry, keyed by registry name.

        Source of truth, per entry:
          1. pull_log rows this scheduler wrote itself
             (``puller_name = SMART_PULL_LOG_PREFIX + name``, see
             ``_log_run``): the latest SUCCESS ``completed_at`` is
             the completed-check cadence anchor (including NO_NEW_DATA markers),
             not a freshness claim; the trailing FAILED/PARTIAL streak inside
             RESTART_FAILURE_LOOKBACK_H rebuilds ``consecutive_fails`` and
             the matching cooldown, so an entry that was backing off does
             not get retried on every restart either.
          2. Bootstrap, only while an entry has no SUCCESS in pull_log yet
             (i.e. the first restart after this code ships): the mapped
             source_catalog row's ``last_pull_at`` (``catalog_name_for``) --
             but only when that row belongs to this entry alone, or the
             entry's own name is that row's name. A shared row (FRED,
             yfinance, Kalshi, polymarket) is bumped by other jobs on their
             own cadence; bootstrapping e.g. the 168h ``repo_market`` entry
             from FRED's 12h bumps would re-defer it on every restart and
             starve it for good. Those few entries run once, then have
             pull_log history like everything else.
        An entry with neither is genuinely never-run and stays absent from
        ``_state`` (``_is_due`` treats that as due).
        """
        names = [p["name"] for p in PULLER_REGISTRY]
        self._catalog_ids: dict[str, int] = {}
        catalog_last: dict[str, datetime | None] = {}
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT id, name, last_pull_at FROM source_catalog"
                )).fetchall()
            for r in rows:
                lname = str(r[1]).lower()
                self._catalog_ids[lname] = int(r[0])
                catalog_last[lname] = _as_utc(r[2])
        except Exception as exc:
            log.warning("SmartScheduler source_catalog state load failed: {e}", e=str(exc))

        log_names = [SMART_PULL_LOG_PREFIX + n for n in names]
        last_success: dict[str, datetime | None] = {}
        recent: list[Any] = []
        try:
            since = datetime.now(timezone.utc) - timedelta(hours=RESTART_FAILURE_LOOKBACK_H)
            with self.engine.connect() as conn:
                for r in conn.execute(
                    text(
                        "SELECT puller_name, MAX(completed_at) FROM pull_log "
                        "WHERE status = 'SUCCESS' AND puller_name IN :names "
                        "GROUP BY puller_name"
                    ).bindparams(bindparam("names", expanding=True)),
                    {"names": log_names},
                ).fetchall():
                    last_success[str(r[0])[len(SMART_PULL_LOG_PREFIX):]] = _as_utc(r[1])
                recent = conn.execute(
                    text(
                        "SELECT puller_name, status, started_at, completed_at FROM pull_log "
                        "WHERE puller_name IN :names AND started_at >= :since "
                        "ORDER BY started_at DESC"
                    ).bindparams(bindparam("names", expanding=True)),
                    {"names": log_names, "since": since},
                ).fetchall()
        except Exception as exc:
            log.warning("SmartScheduler pull_log state load failed: {e}", e=str(exc))

        # Trailing failure streak per entry (rows are newest first).
        streaks: dict[str, tuple[int, datetime | None]] = {}
        closed: set[str] = set()
        for r in recent:
            name = str(r[0])[len(SMART_PULL_LOG_PREFIX):]
            if name in closed:
                continue
            status = str(r[1])
            if status == "SUCCESS":
                closed.add(name)
                continue
            if status in ("FAILED", "PARTIAL"):
                fails, last_fail = streaks.get(name, (0, None))
                if last_fail is None:
                    last_fail = _as_utc(r[3]) or _as_utc(r[2])
                streaks[name] = (fails + 1, last_fail)

        owners: dict[str, int] = {}
        for n in names:
            key = catalog_name_for(n).lower()
            owners[key] = owners.get(key, 0) + 1

        from_log = from_catalog = 0
        for n in names:
            success = last_success.get(n)
            source = "pull_log" if success is not None else None
            if success is None:
                key = catalog_name_for(n).lower()
                if owners.get(key, 0) == 1 or key == n.lower():
                    success = catalog_last.get(key)
                    source = "source_catalog" if success is not None else None
            fails, last_fail = streaks.get(n, (0, None))
            if success is None and fails == 0:
                continue
            cooldown_until = None
            if fails and last_fail is not None:
                latest_status = next((str(r[1]) for r in recent if str(r[0]) == SMART_PULL_LOG_PREFIX + n), None)
                retry_minutes = 30 if n == "options" and latest_status == "PARTIAL" else _cooldown_minutes(fails)
                cooldown_until = last_fail + timedelta(minutes=retry_minutes)
            self._state[n] = {
                "last_success": success,
                "last_attempt": last_fail or success,
                "consecutive_fails": fails,
                "cooldown_until": cooldown_until,
                "state_source": source,
            }
            if source == "pull_log":
                from_log += 1
            elif source == "source_catalog":
                from_catalog += 1
        log.info(
            "SmartScheduler restart state: {a} from pull_log, {b} bootstrapped "
            "from source_catalog, {c} with no history (of {t} registered)",
            a=from_log, b=from_catalog, c=len(names) - len(self._state), t=len(names),
        )

    def _is_due(self, puller: dict) -> bool:
        """Check if a puller needs to run based on its frequency."""
        name = puller["name"]
        state = self._state.get(name)

        # cftc_cot is gated to its weekly release window (see
        # _cftc_cot_is_due) instead of the plain freq_h cadence — CFTC only
        # publishes once a week, on Friday, so "due" must mean "in Friday's
        # release window or Saturday's retry", not "168h since last run" (or
        # "never run" — a never-run cftc_cot still waits for the window
        # instead of firing off-schedule the instant this process starts).
        if name == "cftc_cot":
            cooldown = state.get("cooldown_until") if state else None
            if cooldown and datetime.now(timezone.utc) < cooldown:
                return False
            last = state.get("last_success") if state else None
            if last is not None and hasattr(last, "tzinfo") and last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            return _cftc_cot_is_due(last, datetime.now(timezone.utc))

        # Never run before → definitely due
        if state is None:
            return True

        # In cooldown after failure → not due
        cooldown = state.get("cooldown_until")
        if cooldown and datetime.now(timezone.utc) < cooldown:
            return False

        # Check if enough time has passed since last success
        last = state.get("last_success")
        if last is None:
            return True

        if hasattr(last, "tzinfo") and last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)

        age_hours = (datetime.now(timezone.utc) - last).total_seconds() / 3600
        return age_hours >= puller["freq_h"]

    def _get_due_pullers(self) -> list[dict]:
        """Return pullers that are due, sorted by priority (most overdue first)."""
        due = []
        for p in PULLER_REGISTRY:
            if self._is_due(p):
                # Calculate how overdue (for priority sorting)
                state = self._state.get(p["name"])
                if state and state.get("last_success"):
                    last = state["last_success"]
                    if hasattr(last, "tzinfo") and last.tzinfo is None:
                        last = last.replace(tzinfo=timezone.utc)
                    overdue_h = (datetime.now(timezone.utc) - last).total_seconds() / 3600 - p["freq_h"]
                else:
                    overdue_h = 9999  # never run = most overdue
                due.append({**p, "_overdue_h": overdue_h})

        # Most overdue first, but fast pullers (low timeout) get priority
        due.sort(key=lambda x: (-x["_overdue_h"], x["timeout_s"]))
        return due

    def _run_puller(self, puller: dict) -> dict[str, Any]:
        """Import, instantiate, and run a single puller with timeout.

        Uses a semaphore to cap concurrent threads at MAX_CONCURRENT_THREADS
        and tracks active threads for observability.

        GRID-YF-CLOSE-REPAIR-20260926 follow-up: a puller that overruns
        ``timeout_s`` is daemon-detached and left running (see the class
        docstring) — that orphaned thread was the confirmed mechanism behind
        the April 2026 yfinance wrong-instrument contamination, once a
        thread-unsafe download library shared state across the orphan and
        the NEXT tick's fresh call. Two independent, consistent mitigations
        now live alongside this hard join-timeout:
          1. Any puller method that accepts a ``should_continue`` kwarg
             (currently ``YFinancePuller.pull_all``) gets one wired
             automatically, bound to a deadline a little inside this call's
             own ``timeout_s``. That lets a slow multi-item pull stop itself
             cleanly BETWEEN items before the hard join timeout fires,
             instead of relying on abandonment.
          2. If the puller's own return value reports
             ``{"status": "SKIPPED", ...}`` (e.g. YFinancePuller.pull_all's
             single-flight lock finding a previous run still active), this
             call is recorded as SKIPPED rather than SUCCESS — critically,
             it must NOT advance ``source_catalog.last_pull_at``, or a
             lock-skipped run would be indistinguishable from a real one.
        """
        import importlib
        import inspect
        import os

        name = puller["name"]
        timeout_s = puller.get("timeout_s", 120)
        # The cooperative deadline is measured from job start, before import
        # and instantiation, so setup time never extends a puller's budget.
        # Pullers that accept ``deadline_at`` receive this same absolute
        # instant and can cap individual operations against it (FRED does).
        margin = puller.get("stop_margin_s", min(15, max(timeout_s // 8, 1)))
        cooperative_deadline = time.monotonic() + max(timeout_s - margin, 1)
        result: dict[str, Any] = {"name": name, "status": "UNKNOWN"}

        if puller.get("hold_reason"):
            return {"name": name, "status": OUTCOME_SKIPPED, "reason": puller["hold_reason"]}

        # Acquire semaphore (non-blocking) to enforce thread limit
        if not self._thread_semaphore.acquire(blocking=False):
            with self._threads_lock:
                active = list(self._active_threads)
            log.warning(
                "SmartScheduler: thread limit ({lim}) reached, skipping {n} — active: {a}",
                lim=self.MAX_CONCURRENT_THREADS, n=name, a=active,
            )
            result["status"] = "SKIPPED"
            result["reason"] = f"Thread limit ({self.MAX_CONCURRENT_THREADS}) reached"
            return result

        # Register this thread as active
        with self._threads_lock:
            self._active_threads.add(name)

        try:
            mod = importlib.import_module(puller["mod"])
            cls = getattr(mod, puller["cls"])

            try:
                instance = self._build_puller_instance(puller, cls, os.environ)
            except MissingPullerApiKey as exc:
                result["status"] = "SKIPPED"
                result["reason"] = str(exc)
                return result

            method = getattr(instance, puller["method"])
            method_kwargs = dict(puller.get("kwargs") or {})
            # Registry kwargs may be plain values or zero-arg callables
            # (e.g. a "recent N days" start_date that must be computed at
            # call time, not once when PULLER_REGISTRY is built at import).
            # `should_continue` is itself always a callable BY CONTRACT (a
            # cooperative-cancellation check, not a value to precompute) —
            # never resolve it here, or an explicit registry-supplied one
            # would be invoked once and replaced by its boolean result.
            for key, val in list(method_kwargs.items()):
                if key != "should_continue" and callable(val):
                    method_kwargs[key] = val()

            # Cooperative-cancellation wiring: if the puller's method
            # accepts should_continue and the registry didn't already
            # supply one, bind a deadline a bit inside this call's own
            # timeout_s so it can stop itself between items instead of
            # being abandoned by the hard join timeout below. See this
            # method's docstring, mitigation 1.
            try:
                accepted_params = inspect.signature(method).parameters
            except (TypeError, ValueError):
                accepted_params = {}
            if "should_continue" not in method_kwargs and "should_continue" in accepted_params:
                method_kwargs["should_continue"] = (
                    lambda _deadline=cooperative_deadline: time.monotonic() < _deadline
                )
            if "deadline_at" not in method_kwargs and "deadline_at" in accepted_params:
                method_kwargs["deadline_at"] = cooperative_deadline

            # Run with timeout — don't let any puller block for minutes
            out_box: list[Any] = [None]
            err_box: list[Exception | None] = [None]

            def _target() -> None:
                try:
                    out_box[0] = method(**method_kwargs)
                except Exception as e:
                    err_box[0] = e

            t = threading.Thread(target=_target, daemon=True)
            t.start()
            t.join(timeout=timeout_s)

            if t.is_alive():
                # The daemon thread keeps running in the background — we
                # release the semaphore on the way out so other pullers
                # are not starved, but the orphan is not joinable. Track
                # the cumulative count so operators can detect a slow
                # leak via get_status() without resorting to /proc.
                with self._threads_lock:
                    self._orphan_thread_count += 1
                    orphan_total = self._orphan_thread_count
                result["status"] = "TIMEOUT"
                result["error"] = f"Exceeded {timeout_s}s timeout"
                log.warning(
                    "SmartScheduler: {n} TIMEOUT after {s}s — daemon "
                    "thread orphaned (cumulative orphans={c})",
                    n=name, s=timeout_s, c=orphan_total,
                )
                return result

            if err_box[0]:
                raise err_box[0]

            out = out_box[0]
            outcome, rows, note = _classify_outcome(out)
            result["rows_inserted"] = rows
            result["detail"] = str(out)[:200] if out else ""

            # Mitigation 2 (see docstring): a puller can report its own
            # SKIPPED outcome — e.g. YFinancePuller.pull_all's single-flight
            # lock finding a previous run still active, or (2026-09-29) a
            # list whose every item is SKIPPED, like OptionsPuller.pull_all
            # outside an equity session. That is NOT a successful check and
            # must not advance last_pull_at.
            if outcome == OUTCOME_SKIPPED:
                result["status"] = OUTCOME_SKIPPED
                result["reason"] = note or "puller reported SKIPPED"
                return result

            # Incomplete coverage preserves committed rows but cannot
            # establish freshness, even when those rows are positive.
            if outcome in (OUTCOME_FAILED, OUTCOME_PARTIAL):
                result["status"] = outcome
                result["error"] = (note or f"puller reported {outcome}")[:200]
                # Deliberately NOT calling self._update_last_pull(name) --
                # this source did not have a clean, complete run.
                return result

            # 2026-09-29: a clean run that wrote nothing is not a fresh
            # source. NO_NEW_DATA keeps the job's cadence (tick() treats it
            # as a completed check, so it is neither re-run every tick nor
            # backed off like a failure) but never bumps last_pull_at.
            if outcome == OUTCOME_NO_NEW_DATA:
                result["status"] = OUTCOME_NO_NEW_DATA
                result["reason"] = note or "run completed, 0 rows written"
                return result

            result["status"] = OUTCOME_SUCCESS
            if note:
                result["note"] = note
            if not _puller_owns_catalog(name):
                self._update_last_pull(name)

        except Exception as exc:
            result["status"] = "FAILED"
            result["error"] = str(exc)[:200]
            log.warning("SmartScheduler: {n} failed: {e}", n=name, e=str(exc))

        finally:
            # Always clean up: release semaphore and unregister thread
            with self._threads_lock:
                self._active_threads.discard(name)
            self._thread_semaphore.release()

        return result

    def _build_puller_instance(
        self,
        puller: dict[str, Any],
        cls: type,
        environ: dict[str, str],
    ) -> Any:
        """Instantiate a puller using the constructor shape declared in registry."""
        api_key_name = puller.get("api_key")
        if not api_key_name:
            return cls(db_engine=self.engine)

        key_val = environ.get(api_key_name, "")
        if not key_val:
            raise MissingPullerApiKey(api_key_name)

        mode = puller.get("api_key_mode", "first")
        if mode == "first":
            return cls(key_val, self.engine)
        if mode == "keyword":
            return cls(db_engine=self.engine, api_key=key_val)
        if mode == "env":
            return cls(db_engine=self.engine)

        raise ValueError(f"Unknown api_key_mode for {puller['name']}: {mode}")

    def _update_last_pull(self, name: str) -> None:
        """Advance source_catalog.last_pull_at for the row this entry writes.

        ``name`` is the registry name; the catalog row is
        ``catalog_name_for(name)`` (see REGISTRY_CATALOG_NAMES) -- before
        the 2026-09-29 fix this matched ``name.lower()`` directly and was a
        silent no-op for 48 of the 92 entries.
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(text(
                    "UPDATE source_catalog SET last_pull_at = NOW() "
                    "WHERE LOWER(name) = :n"
                ), {"n": catalog_name_for(name).lower()})
        except Exception:
            pass  # best effort

    def _log_run(self, name: str, started_at: datetime, result: dict[str, Any]) -> None:
        """Persist one pull_log row per real run -- the restart-state record.

        SKIPPED runs (thread limit, missing API key, a puller's own
        single-flight skip, an all-items-skipped list) are not attempts and
        are not logged. NO_NEW_DATA is logged as SUCCESS with 0 rows. PARTIAL
        remains PARTIAL; TIMEOUT and other failures are logged as FAILED (pull_log's CHECK
        constraint allows RUNNING/SUCCESS/PARTIAL/FAILED only), with the
        original status kept in error_message. One INSERT at the end of the
        run, never a RUNNING row that a restart could orphan.
        """
        status = result.get("status")
        if status == OUTCOME_SKIPPED:
            return
        error = result.get("error")
        if status == OUTCOME_NO_NEW_DATA:
            # A completed check that wrote nothing. pull_log's CHECK
            # constraint has no such status, so it is stored as SUCCESS
            # with rows_inserted = 0 and the NO_NEW_DATA marker in
            # error_message -- restart state needs it to keep the job's
            # cadence. The freshness layer (source_catalog.last_pull_at)
            # was NOT bumped for it; see _run_puller.
            log_status = OUTCOME_SUCCESS
            error = f"{OUTCOME_NO_NEW_DATA}: {result.get('reason') or 'run completed, 0 rows written'}"
        else:
            log_status = status if status in (OUTCOME_SUCCESS, OUTCOME_PARTIAL) else OUTCOME_FAILED
            if log_status == OUTCOME_FAILED and status not in (None, OUTCOME_FAILED):
                error = f"{status}: {error}" if error else str(status)
            if log_status == OUTCOME_SUCCESS and result.get("note"):
                error = str(result["note"])
        try:
            import socket

            source_id = getattr(self, "_catalog_ids", {}).get(catalog_name_for(name).lower())
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO pull_log (puller_name, source_id, started_at, "
                        "completed_at, status, rows_inserted, error_message, node_name) "
                        "VALUES (:name, :sid, :started, :completed, :status, :rows, "
                        ":error, :node)"
                    ),
                    {
                        "name": SMART_PULL_LOG_PREFIX + name,
                        "sid": source_id,
                        "started": started_at,
                        "completed": datetime.now(timezone.utc),
                        "status": log_status,
                        "rows": result.get("rows_inserted", 0),
                        "error": (str(error)[:500] if error else None),
                        "node": socket.gethostname(),
                    },
                )
        except Exception as exc:
            log.warning(
                "SmartScheduler: pull_log write failed for {n}: {e}", n=name, e=str(exc)
            )

    def _record_result(
        self,
        name: str,
        success: bool,
        error: str | None = None,
        *,
        skipped: bool = False,
        partial: bool = False,
    ) -> None:
        """Record puller result and manage cooldowns.

        ``success`` means "a completed check" (SUCCESS or NO_NEW_DATA): it
        anchors the job's cadence. ``skipped`` (2026-09-29) is neither a
        success nor a failure: the job is retried after a flat
        SKIP_RETRY_MINUTES, its failure streak and last success untouched.
        Before, a SKIPPED run fed the exponential failure backoff -- so an
        options job skipping every run from Friday's close would sit in a
        16-24h cooldown by Monday's open.
        """
        state = self._state.get(name, {"consecutive_fails": 0})
        state["last_attempt"] = datetime.now(timezone.utc)

        if skipped:
            state["cooldown_until"] = (
                datetime.now(timezone.utc) + timedelta(minutes=SKIP_RETRY_MINUTES)
            )
            log.info(
                "SmartScheduler: {n} skipped ({r}), retry in {c}min",
                n=name, r=error or "no reason given", c=SKIP_RETRY_MINUTES,
            )
        elif success:
            state["last_success"] = datetime.now(timezone.utc)
            state["consecutive_fails"] = 0
            state["cooldown_until"] = None
        else:
            fails = state.get("consecutive_fails", 0) + 1
            state["consecutive_fails"] = fails
            # Exponential backoff: 30min, 1h, 2h, 4h, 8h, max 24h
            # Incomplete options coverage is common (including tickers with
            # no chain). Preserve PARTIAL truth without a 24h retry lockout.
            # True FAILED/TIMEOUT outcomes retain normal failure backoff.
            cooldown_min = 30 if name == "options" and partial else _cooldown_minutes(fails)
            state["cooldown_until"] = (
                datetime.now(timezone.utc) + timedelta(minutes=cooldown_min)
            )
            log.info(
                "SmartScheduler: {n} failed {f}x, cooldown {c}min",
                n=name, f=fails, c=cooldown_min,
            )

        self._state[name] = state

    def tick(self) -> dict[str, Any]:
        """Run one scheduler tick — execute up to MAX_PULLERS_PER_TICK due pullers.

        Returns summary of what ran, what succeeded, what failed.
        """
        tick_start = time.monotonic()
        due = self._get_due_pullers()

        summary: dict[str, Any] = {
            "total_due": len(due),
            "ran": 0,
            "succeeded": 0,
            "failed": 0,
            "skipped": 0,
            "no_new_data": 0,
            "results": [],
            "still_due": [],
        }

        if not due:
            log.info("SmartScheduler tick: nothing due")
            return summary

        log.info(
            "SmartScheduler tick: {n} pullers due, running up to {m}",
            n=len(due), m=MAX_PULLERS_PER_TICK,
        )

        for puller in due[:MAX_PULLERS_PER_TICK]:
            # Check time budget
            elapsed = time.monotonic() - tick_start
            if elapsed > TICK_TIME_BUDGET_S:
                log.info("SmartScheduler: time budget exhausted ({e:.0f}s), deferring rest", e=elapsed)
                break

            name = puller["name"]
            log.info("SmartScheduler: running {n} (overdue {h:.0f}h)", n=name, h=puller.get("_overdue_h", 0))

            run_started = datetime.now(timezone.utc)
            result = self._run_puller(puller)
            status = result["status"]
            skipped = status == OUTCOME_SKIPPED
            self._record_result(
                name,
                status in (OUTCOME_SUCCESS, OUTCOME_NO_NEW_DATA),
                result.get("error") or result.get("reason"),
                skipped=skipped,
                partial=status == OUTCOME_PARTIAL,
            )
            self._log_run(name, run_started, result)

            summary["results"].append(result)
            summary["ran"] += 1
            if status == OUTCOME_SUCCESS:
                summary["succeeded"] += 1
            elif status == OUTCOME_NO_NEW_DATA:
                summary["no_new_data"] += 1
            elif skipped:
                summary["skipped"] += 1
            else:
                summary["failed"] += 1

        # Report what's still due for next tick
        summary["still_due"] = [p["name"] for p in due[MAX_PULLERS_PER_TICK:]]

        elapsed = time.monotonic() - tick_start
        log.info(
            "SmartScheduler tick complete in {e:.1f}s — "
            "{ok}/{ran} succeeded, {nd} no new data, {sk} skipped, "
            "{f} failed, {d} still due",
            e=elapsed, ok=summary["succeeded"], ran=summary["ran"],
            nd=summary["no_new_data"], sk=summary["skipped"],
            f=summary["failed"], d=len(summary["still_due"]),
        )
        return summary

    def get_status(self) -> dict[str, Any]:
        """Return current scheduler state for the API."""
        due = self._get_due_pullers()
        in_cooldown = [
            {
                "name": name,
                "fails": s.get("consecutive_fails", 0),
                "cooldown_until": s["cooldown_until"].isoformat() if s.get("cooldown_until") else None,
            }
            for name, s in self._state.items()
            if s.get("cooldown_until") and datetime.now(timezone.utc) < s["cooldown_until"]
        ]
        with self._threads_lock:
            orphan_total = self._orphan_thread_count
            active_thread_names = sorted(self._active_threads)
        return {
            "total_registered": len(PULLER_REGISTRY),
            "total_due": len(due),
            "due_names": [p["name"] for p in due[:20]],
            "in_cooldown": in_cooldown,
            "max_per_tick": MAX_PULLERS_PER_TICK,
            "tick_budget_s": TICK_TIME_BUDGET_S,
            "active_threads": active_thread_names,
            "max_concurrent_threads": self.MAX_CONCURRENT_THREADS,
            "orphan_thread_count_total": orphan_total,
        }
