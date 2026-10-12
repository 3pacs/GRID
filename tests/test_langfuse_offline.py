"""Exact-source controls with dummy providers, DB and Langfuse only."""
from __future__ import annotations

import ast
import importlib.util
import json
import hashlib
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
BASE_AST_SHA256 = {'llamacpp/client.py': '9ec9652cbab0d60d510e9f6aef46b8e254b6ef02d776fb2192995e01b09bf4fe', 'llm/router.py': '2dbdc22dd354167d5488c6fdb0b6b485efdc48a50aa70ea7198229f2456f113f', 'ollama/client.py': 'c2ddd761c9a2e43378a197e7986563eb1a229ed02c9df64e7eabd4fe8ff05008', 'gemma/client.py': 'd934451d6ed0f52adf0d538d37d15bbf69d128e6e38be7e59a3b6fe1337e6229', 'gemma/micro.py': '1dd4304baa1826a7f2fd13b4a3e2adf41f9f0299ce6fc5c45c209b463924eecf'}
BASE_GATE_SHA256 = {'_fallback_chain': '8305bab19e3498e017141bff38c229485c50288fe342845bd5ec880aa9abcc88', '_paid_llm_allowed': 'dd7e6c71aca2b396206f31a124b38511aea94b44df3b69145a368f985e7f32ab', '_create_client': 'f09ed8877ae342df43a9ef51951d97927e9982b74d761d505827ce44bcaa1bba', '_create_gemini_client': '93dab085d9e969f1c6b4a61fe419c7b5cc46d3bfb2780c08421f159cfc0244c7'}


class Clock:
    value = 0.0

    def monotonic(self):
        return self.value

    def time(self):
        return self.value


class Observation:
    def __init__(self, clock, fields):
        self.clock = clock
        self.start = clock.value
        self.finish = None
        self.fields = fields
        self.ends = 0

    def update(self, **fields):
        if "metadata" in fields:
            self.fields.setdefault("metadata", {}).update(fields.pop("metadata"))
        self.fields.update(fields)

    def end(self):
        self.ends += 1
        self.finish = self.clock.value


class ReplacingObservation(Observation):
    """SDK contract control: metadata updates replace rather than merge."""
    def __init__(self, clock, fields):
        super().__init__(clock, {**fields, "metadata": dict(fields.get("metadata", {}))})

    def update(self, **fields):
        if "metadata" in fields:
            fields["metadata"] = dict(fields["metadata"])
        self.fields.update(fields)


class SDK:
    def __init__(self, clock):
        self.clock = clock
        self.observations = []

    # Public SDK signature subset; unsupported timing/private arguments fail.
    def start_observation(self, *, name, as_type, model=None, input=None,
                          output=None, usage_details=None, metadata=None):
        fields = {"name": name, "as_type": as_type, "metadata": metadata or {}}
        for key, value in {"model": model, "input": input, "output": output,
                           "usage_details": usage_details}.items():
            if value is not None:
                fields[key] = value
        obs = getattr(self, "observation_type", Observation)(self.clock, fields)
        self.observations.append(obs)
        return obs


class DummyLog:
    def __getattr__(self, name):
        return lambda *args, **kwargs: None


class Response:
    def __init__(self, status=200, data=None, raises=False):
        self.status_code = status
        self.data = data
        self.text = "DUMMY_SECRET_DO_NOT_EXPORT"
        self.raises = raises

    def json(self):
        if self.raises:
            raise ValueError(self.text)
        return self.data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise ValueError(self.text)


class Provider:
    def __init__(self, clock, response, timeout=False):
        self.clock, self.response, self.timeout = clock, response, timeout
        self.posts = 0

    def post(self, *args, **kwargs):
        self.posts += 1
        self.clock.value += 2.5
        if self.timeout:
            raise TimeoutError("DUMMY_SECRET_DO_NOT_EXPORT")
        return self.response

    def get(self, *args, **kwargs):
        self.clock.value += 0.25
        return Response(503)


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class TracingTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.sdk = SDK(self.clock)
        self.feedback = load(ROOT / "llm/feedback_loop.py", "offline_feedback")
        self.feedback.time = self.clock
        self.feedback._get_langfuse = lambda: self.sdk
        self.fake_db_calls = 0

        def fake_db():
            self.fake_db_calls += 1
            raise RuntimeError("dummy DB unavailable")

        self.feedback._get_conn = fake_db
        knowledge = types.ModuleType("knowledge.loader")
        knowledge.inject_knowledge = lambda messages, *args: messages
        knowledge.load_all_knowledge_docs = lambda *args: ""
        knowledge.load_knowledge_doc = lambda *args: None
        self.modules = patch.dict(sys.modules, {
            "llm": types.ModuleType("llm"),
            "llm.feedback_loop": self.feedback,
            "loguru": types.SimpleNamespace(logger=DummyLog()),
            "knowledge": types.ModuleType("knowledge"),
            "knowledge.loader": knowledge,
        })
        self.modules.start()

    def tearDown(self):
        self.modules.stop()

    def client(self, response, *, base=False, timeout=False, available=True):
        module = load(ROOT / "llamacpp/client.py", "offline_llama")
        if base:
            tree = ast.parse((ROOT / "llamacpp/client.py").read_text())
            tree.body = [n for n in tree.body if not (isinstance(n, ast.ImportFrom) and n.module == "llm.feedback_loop")]
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    node.decorator_list = [d for d in node.decorator_list if not (isinstance(d, ast.Call) and isinstance(d.func, ast.Name) and d.func.id == "trace_llm_call")]
            assert hashlib.sha256(ast.dump(tree).encode()).hexdigest() == BASE_AST_SHA256["llamacpp/client.py"]
            exec(compile(tree, "pinned_baseline_llama", "exec"), module.__dict__)
        module.time = self.clock
        module.requests = Provider(self.clock, response, timeout)
        client = module.LlamaCppClient.__new__(module.LlamaCppClient)
        client.is_available = available
        client._backoff_key = "dummy"
        client.base_url = "http://dummy.invalid"
        client.model = "dummy-model"
        client.embed_model = "dummy-embed"
        client.timeout = 9
        client.default_num_predict = 20
        client.min_num_predict = 0
        client.reasoning_headroom = 0
        client._ctx_size = 4096
        client._knowledge_cache = {}
        return module, client

    @staticmethod
    def success(usage=True):
        data = {"choices": [{"message": {"content": "dummy answer"}}], "model": "dummy-model"}
        if usage:
            data["usage"] = {"prompt_tokens": 12, "completion_tokens": 4}
        return Response(data=data)

    def chat(self, client):
        return client.chat([{"role": "user", "content": "DUMMY_SECRET_DO_NOT_EXPORT"}])

    def test_baseline_posthoc_duration_is_zero(self):
        baseline = load(ROOT / "tests/fixtures/langfuse_posthoc_emitter.py", "offline_baseline_feedback")
        baseline._get_langfuse = lambda: self.sdk
        self.clock.value = 2.5
        baseline._emit_to_langfuse(call_id="dummy", module="llamacpp", tier="unknown",
            model="dummy", provider="llamacpp", system_prompt="", user_prompt="dummy",
            output="dummy", context_tokens=1, output_tokens=1, latency_ms=2500, metadata={})
        obs = self.sdk.observations[0]
        self.assertEqual(obs.finish - obs.start, 0)
        self.assertEqual(obs.fields["metadata"]["latency_ms"], 2500)

    def test_chat_duration_counts_provider_and_no_duplicate_success(self):
        _, client = self.client(self.success())
        self.assertEqual(self.chat(client), "dummy answer")
        self.assertEqual(len(self.sdk.observations), 1)
        obs = self.sdk.observations[0]
        self.assertEqual(obs.finish - obs.start, 2.5)
        self.assertEqual(obs.fields["usage_details"], {"input": 12, "output": 4})
        self.assertEqual(obs.fields["metadata"]["call_status"], "returned")
        self.assertEqual(obs.ends, 1)
        self.assertEqual(self.fake_db_calls, 1)

    def test_replacement_metadata_keeps_reported_and_estimated_provenance(self):
        self.sdk.observation_type = ReplacingObservation
        for reported in (True, False):
            with self.subTest(reported=reported):
                _, client = self.client(self.success(usage=reported))
                self.assertEqual(self.chat(client), "dummy answer")
                obs = self.sdk.observations[-1]
                final_metadata = obs.fields["metadata"]
                for key, expected in {
                    "module": "llamacpp", "provider": "llamacpp", "tier": "unknown",
                    "operation": "chat", "call_status": "returned",
                    "latency_ms": 2500, "elapsed_ms": 2500,
                }.items():
                    self.assertEqual(final_metadata.get(key), expected, key)
                provenance = "reported" if reported else "estimated"
                self.assertEqual(final_metadata.get("usage_provenance"),
                                 {"input": provenance, "output": provenance})
                self.assertTrue(final_metadata.get("call_id"))
                self.assertEqual(obs.ends, 1)
                self.assertEqual(obs.finish - obs.start, 2.5)
        self.assertEqual(len(self.sdk.observations), 2)

    def test_baseline_http_failure_has_no_trace(self):
        _, client = self.client(Response(503), base=True)
        self.assertIsNone(self.chat(client))
        self.assertEqual(self.sdk.observations, [])

    def assert_safe_failure(self, response, **kwargs):
        _, client = self.client(response, **kwargs)
        self.assertIsNone(self.chat(client))
        self.assertEqual(len(self.sdk.observations), 1)
        obs = self.sdk.observations[0]
        self.assertEqual(obs.fields["status_message"], "unavailable")
        self.assertEqual(obs.fields["level"], "WARNING")
        self.assertNotIn("DUMMY_SECRET_DO_NOT_EXPORT", json.dumps(obs.fields))
        for key in ["input", "output", "usage_details", "cost_details", "score"]:
            self.assertNotIn(key, obs.fields)
        self.assertEqual(obs.ends, 1)
        self.assertEqual(self.fake_db_calls, 0)

    def test_http_400(self):
        self.assert_safe_failure(Response(400))

    def test_http_503(self):
        self.assert_safe_failure(Response(503))

    def test_timeout(self):
        self.assert_safe_failure(Response(), timeout=True)

    def test_decode_error(self):
        self.assert_safe_failure(Response(raises=True))

    def test_bad_envelope(self):
        self.assert_safe_failure(Response(data={}))

    def test_reasoning_only(self):
        self.assert_safe_failure(Response(data={"choices": [{"message": {"reasoning_content": "private"}}]}))

    def test_think_only(self):
        self.assert_safe_failure(Response(data={"choices": [{"message": {"content": "<think>private</think>"}}]}))

    def test_failed_health_recheck(self):
        self.assert_safe_failure(Response(), available=False)

    def test_active_backoff_has_no_provider_request(self):
        module, client = self.client(Response(), available=False)
        module._ENDPOINT_BACKOFF_UNTIL["dummy"] = 100
        self.assertIsNone(self.chat(client))
        self.assertEqual(module.requests.posts, 0)
        self.assertEqual(self.sdk.observations[0].ends, 1)

    def test_embedding_no_text_or_vectors_exported(self):
        _, client = self.client(Response(data={"data": [{"embedding": [1.0, 2.0]}]}))
        self.assertEqual(client.embed(["DUMMY_SECRET_DO_NOT_EXPORT"]), [[1.0, 2.0]])
        obs = self.sdk.observations[0]
        self.assertEqual(obs.fields["as_type"], "embedding")
        self.assertEqual(obs.finish - obs.start, 2.5)
        self.assertNotIn("input", obs.fields)
        self.assertNotIn("output", obs.fields)
        self.assertNotIn("cost_details", obs.fields)

    def test_embedding_http_failure(self):
        _, client = self.client(Response(503))
        self.assertIsNone(client.embed(["DUMMY_SECRET_DO_NOT_EXPORT"]))
        self.assertEqual(self.sdk.observations[0].fields["status_message"], "unavailable")

    def test_embedding_unavailable(self):
        _, client = self.client(Response(), available=False)
        self.assertIsNone(client.embed(["DUMMY_SECRET_DO_NOT_EXPORT"]))
        self.assertEqual(self.sdk.observations[0].ends, 1)

    def test_unknown_usage_not_filled_with_zero(self):
        self.feedback._emit_to_langfuse(call_id="dummy", module="dummy", tier="unknown",
            model="", provider="dummy", system_prompt="", user_prompt="", output="",
            context_tokens=0, output_tokens=0, latency_ms=0, metadata={})
        fields = self.sdk.observations[0].fields
        self.assertEqual(fields["usage_details"], {})
        self.assertNotIn("cost_details", fields)

    def test_estimate_provenance_keeps_caller_metadata_immutable(self):
        metadata = {"dummy_tag": "fixture"}
        self.feedback.log_llm_call("dummy", "unknown", "", "dummy text", "dummy answer", metadata=metadata)
        fields = self.sdk.observations[0].fields
        self.assertEqual(fields["metadata"]["usage_provenance"], {"input": "estimated", "output": "estimated"})
        self.assertEqual(metadata, {"dummy_tag": "fixture"})

    def test_sdk_start_failure_preserves_return(self):
        self.sdk.start_observation = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("dummy"))
        _, client = self.client(self.success())
        self.assertEqual(self.chat(client), "dummy answer")

    def test_sdk_update_failure_still_ends(self):
        start = self.sdk.start_observation
        def failing(**kwargs):
            obs = start(**kwargs)
            obs.update = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("dummy"))
            return obs
        self.sdk.start_observation = failing
        _, client = self.client(self.success())
        self.assertEqual(self.chat(client), "dummy answer")
        self.assertEqual(self.sdk.observations[0].ends, 1)

    def test_unhandled_exception_keeps_identity_no_raw_cause(self):
        error = ValueError("DUMMY_SECRET_DO_NOT_EXPORT")
        @self.feedback.trace_llm_call(module="dummy", provider="dummy")
        def fail():
            raise error
        with self.assertRaises(ValueError) as caught:
            fail()
        self.assertIs(caught.exception, error)
        self.assertEqual(self.sdk.observations[0].fields["status_message"], "exception")
        self.assertNotIn("DUMMY_SECRET_DO_NOT_EXPORT", json.dumps(self.sdk.observations[0].fields))

    def test_nested_calls_do_not_update_parent_or_duplicate(self):
        @self.feedback.trace_llm_call(module="embedding_router", provider="router", operation="embed")
        def parent():
            _, child = self.client(self.success())
            return self.chat(child)
        self.assertEqual(parent(), "dummy answer")
        self.assertEqual(len(self.sdk.observations), 2)
        outer, inner = self.sdk.observations
        self.assertNotIn("call_id", outer.fields["metadata"])
        self.assertIn("call_id", inner.fields["metadata"])
        self.assertEqual(outer.ends, 1)
        self.assertEqual(inner.ends, 1)

    def test_gemini_http_error_and_gate_unchanged(self):
        module = load(ROOT / "llm/router.py", "offline_router")
        module.time = self.clock
        module.requests = Provider(self.clock, Response(429))
        client = module.GeminiClient(api_key="dummy-private")
        self.assertIsNone(self.chat(client))
        self.assertEqual(self.sdk.observations[0].fields["status_message"], "unavailable")
        self.assertNotIn("dummy-private", json.dumps(self.sdk.observations[0].fields))
        self.assertFalse(client.is_available)
        for name in ["_paid_llm_allowed", "_create_client", "_create_gemini_client", "_fallback_chain"]:
            new = next(n for n in ast.parse((ROOT / "llm/router.py").read_text()).body if isinstance(n, ast.FunctionDef) and n.name == name)
            self.assertEqual(BASE_GATE_SHA256[name], hashlib.sha256(ast.dump(new).encode()).hexdigest())

    def test_provider_method_bodies_are_unchanged(self):
        for path in ["llamacpp/client.py", "llm/router.py", "ollama/client.py", "gemma/client.py", "gemma/micro.py"]:
            after = ast.parse((ROOT / path).read_text())
            after.body = [n for n in after.body if not (isinstance(n, ast.ImportFrom) and n.module == "llm.feedback_loop")]
            for node in ast.walk(after):
                if isinstance(node, ast.FunctionDef):
                    node.decorator_list = [d for d in node.decorator_list if not (isinstance(d, ast.Call) and isinstance(d.func, ast.Name) and d.func.id == "trace_llm_call")]
            self.assertEqual(BASE_AST_SHA256[path], hashlib.sha256(ast.dump(after).encode()).hexdigest(), path)

    def test_gemma_micro_success_and_failure(self):
        module = load(ROOT / "gemma/micro.py", "offline_micro")
        module.time = self.clock
        module.requests = Provider(self.clock, self.success())
        client = module.GemmaMicroClient.__new__(module.GemmaMicroClient)
        client.config = module.MicroModelConfig("dummy_micro", "http://dummy.invalid", "dummy-model", "dummy system")
        client.timeout = 9
        client.is_available = True
        self.assertEqual(client.run("dummy input"), "dummy answer")
        self.assertEqual(len(self.sdk.observations), 1)
        self.assertEqual(self.sdk.observations[0].finish - self.sdk.observations[0].start, 2.5)
        module.requests = Provider(self.clock, Response(503))
        self.assertIsNone(client.run("DUMMY_SECRET_DO_NOT_EXPORT"))
        self.assertEqual(len(self.sdk.observations), 2)
        self.assertNotIn("DUMMY_SECRET_DO_NOT_EXPORT", json.dumps(self.sdk.observations[1].fields))

    def test_ollama_embedding_batch_duration_no_vectors_or_text(self):
        module = load(ROOT / "ollama/client.py", "offline_ollama")
        module.time = self.clock
        module.requests = Provider(self.clock, Response(data={"embedding": [1.0]}))
        client = module.OllamaClient.__new__(module.OllamaClient)
        client.is_available = True
        client.base_url = "http://dummy.invalid"
        client.embed_model = "dummy-model"
        client.timeout = 9
        self.assertEqual(client.embed(["private one", "private two"]), [[1.0], [1.0]])
        self.assertEqual(len(self.sdk.observations), 1)
        obs = self.sdk.observations[0]
        self.assertEqual(obs.finish - obs.start, 5.0)
        self.assertNotIn("private", json.dumps(obs.fields))

    def test_router_embedding_fallback_and_empty_call(self):
        module = load(ROOT / "llm/router.py", "offline_router_embed")
        module.time = self.clock
        module._embed_chain = lambda: ["dummy_failed", "dummy_ok"]
        def fail(texts, model=None):
            self.clock.value += 1
            return None
        def success(texts, model=None):
            self.clock.value += 2
            return [[1.0]]
        module._client_cache = {
            "dummy_failed": types.SimpleNamespace(is_available=True, embed=fail),
            "dummy_ok": types.SimpleNamespace(is_available=True, embed=success),
        }
        self.assertEqual(module.embed(["DUMMY_SECRET_DO_NOT_EXPORT"]), [[1.0]])
        obs = self.sdk.observations[0]
        self.assertEqual(obs.finish - obs.start, 3.0)
        self.assertNotIn("DUMMY_SECRET_DO_NOT_EXPORT", json.dumps(obs.fields))
        self.assertEqual(module.embed([]), [])
        self.assertEqual(self.sdk.observations[1].fields["status_message"], "returned")

    def test_no_telemetry_client_preserves_result(self):
        self.feedback._get_langfuse = lambda: None
        _, client = self.client(self.success())
        self.assertEqual(self.chat(client), "dummy answer")
        self.assertEqual(self.sdk.observations, [])

    def test_sdk_end_failure_preserves_result(self):
        start = self.sdk.start_observation
        def failing(**kwargs):
            obs = start(**kwargs)
            obs.end = lambda: (_ for _ in ()).throw(RuntimeError("dummy"))
            return obs
        self.sdk.start_observation = failing
        _, client = self.client(self.success())
        self.assertEqual(self.chat(client), "dummy answer")

    def test_failed_nested_start_cannot_relabel_parent(self):
        outer_start = self.sdk.start_observation
        @self.feedback.trace_llm_call(module="embedding_router", provider="router")
        def parent():
            self.sdk.start_observation = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("dummy"))
            _, client = self.client(self.success())
            return self.chat(client)
        self.assertEqual(parent(), "dummy answer")
        self.sdk.start_observation = outer_start
        self.assertEqual(len(self.sdk.observations), 1)
        self.assertNotIn("input", self.sdk.observations[0].fields)
        self.assertEqual(self.sdk.observations[0].fields["metadata"]["module"], "embedding_router")

    def test_poisoned_success_is_metadata_only_and_training_payload_unchanged(self):
        poisons = ["DUMMY_API_KEY_42", "SYNTHETIC_PATIENT_42", "SYNTHETIC_PERSON_42", "SYNTHETIC_POSITION_42"]
        private = " ".join(poisons)
        response = self.success()
        response.data["model"] = private
        response.data["choices"][0]["message"]["content"] = private
        _, client = self.client(response)
        extra = {
            "module": private, "provider": private, "tier": private,
            "model": private, "operation": private, "call_status": private,
            "call_id": "0" * 16, "latency_ms": private,
            "usage_provenance": {"input": private, "output": private},
            "nested": {"patient": poisons[1], "position": [poisons[3]]},
            "endpoint": "https://dummy.invalid/" + poisons[0],
        }
        inserts = []
        cursor = types.SimpleNamespace(execute=lambda sql, params: inserts.append(params), close=lambda: None)
        connection = types.SimpleNamespace(cursor=lambda: cursor, commit=lambda: None, close=lambda: None)
        self.feedback._get_conn = lambda: connection
        messages = [{"role": "system", "content": private}, {"role": "user", "content": private}]
        self.assertEqual(client.chat(messages, extra_metadata=extra), private)
        self.assertEqual(len(self.sdk.observations), 1)
        obs = self.sdk.observations[0]
        for poison in poisons:
            self.assertNotIn(poison, json.dumps(obs.fields))
        for forbidden in ["input", "output", "cost_details", "score"]:
            self.assertNotIn(forbidden, obs.fields)
        metadata = obs.fields["metadata"]
        self.assertEqual(metadata["module"], "llamacpp")
        self.assertEqual(metadata["provider"], "llamacpp")
        self.assertEqual(metadata["tier"], "unknown")
        self.assertEqual(metadata["operation"], "chat")
        self.assertEqual(metadata["call_status"], "returned")
        self.assertEqual(metadata["latency_ms"], 2500)
        self.assertEqual(metadata["elapsed_ms"], 2500)
        self.assertNotEqual(metadata["call_id"], "0" * 16)
        self.assertEqual(obs.fields["model"], "unknown")
        self.assertEqual(obs.fields["usage_details"], {"input": 12, "output": 4})
        self.assertEqual(metadata["usage_provenance"], {"input": "reported", "output": "reported"})
        self.assertEqual(obs.ends, 1)
        self.assertEqual(len(inserts), 1)
        self.assertEqual(inserts[0][5], private)
        self.assertEqual(inserts[0][6], private)
        self.assertEqual(inserts[0][8], private)
        self.assertEqual(json.loads(inserts[0][12])["nested"], extra["nested"])

    def test_unknown_identifiers_and_invalid_metrics_are_not_exported(self):
        private = "SYNTHETIC_PERSON_POSITION_AND_KEY"
        self.feedback._emit_to_langfuse(call_id=private, module=private, tier=private,
            model=private, provider=private, system_prompt=private, user_prompt=private,
            output=private, context_tokens=True, output_tokens=-1, latency_ms=private,
            metadata={"module": "llamacpp", "raw": {"private": private},
                      "usage_provenance": {"input": private, "output": [private]}})
        obs = self.sdk.observations[0]
        self.assertNotIn(private, json.dumps(obs.fields))
        self.assertEqual(obs.fields["name"], "unknown")
        self.assertEqual(obs.fields["model"], "unknown")
        self.assertEqual(obs.fields["usage_details"], {})
        self.assertEqual(obs.fields["metadata"]["usage_provenance"], {"input": "unknown", "output": "unknown"})
        self.assertNotIn("call_id", obs.fields["metadata"])
        self.assertNotIn("latency_ms", obs.fields["metadata"])
        for key in ["module", "provider", "tier", "operation", "call_status"]:
            self.assertEqual(obs.fields["metadata"][key], "unknown")

    def test_known_identifiers_counts_and_bounds_remain_useful(self):
        self.feedback._emit_to_langfuse(call_id="0123456789abcdef", module="gemini", tier="ORACLE",
            model="gemini-2.5-flash", provider="gemini", system_prompt="private", user_prompt="private",
            output="private", context_tokens=100, output_tokens=5, latency_ms=1234,
            metadata={"usage_provenance": {"input": "reported", "output": "estimated"},
                      "endpoint": "private", "operation": "private"})
        fields = self.sdk.observations[0].fields
        self.assertEqual(fields["name"], "gemini")
        self.assertEqual(fields["model"], "gemini-2.5-flash")
        self.assertEqual(fields["usage_details"], {"input": 100, "output": 5})
        self.assertEqual(fields["metadata"]["call_id"], "0123456789abcdef")
        self.assertEqual(fields["metadata"]["latency_ms"], 1234)
        self.assertNotIn("private", json.dumps(fields))
        values = {"module": "llamacpp", "latency_ms": True, "elapsed_ms": 86_400_001,
                  "call_status": "SYNTHETIC_SECRET", "raw": "private"}
        result = self.feedback._trace_metadata(values)
        self.assertNotIn("latency_ms", result)
        self.assertNotIn("elapsed_ms", result)
        self.assertNotIn("private", json.dumps(result))
        self.assertEqual(result["call_status"], "unknown")


if __name__ == "__main__":
    unittest.main(verbosity=2)
