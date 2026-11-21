"""Session isolation, persistent modes, and one-attempt recovery regression tests."""

from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import httpx

import test_proxy
import proxy
from session_modes import SessionModeStore, extract_session_id


MISMATCH_MESSAGE = (
    "The requested item was created under a different Azure OpenAI resource. "
    "Use the same resource that created the item to access it."
)
SESSION_A = "ses_test_a"
SESSION_B = "ses_test_b"


def mismatch_response() -> httpx.Response:
    return httpx.Response(400, json={"error": {"message": MISMATCH_MESSAGE}})


def completed_response() -> httpx.Response:
    return httpx.Response(200, json={"status": "completed", "output": []})


def sample_payload() -> dict:
    return {
        "model": "gpt-6.1-sol",
        "store": False,
        "input": [
            {"role": "developer", "content": "Use the local tools when needed."},
            {"type": "message", "role": "user", "content": "Inspect the file."},
            {"type": "reasoning", "id": "rs_old", "encrypted_content": "opaque-old-reasoning"},
            {
                "type": "message", "id": "msg_old", "status": "completed", "role": "assistant",
                "phase": "commentary", "content": [{"type": "output_text", "text": "I will inspect it."}],
            },
            {"type": "function_call", "id": "fc_old", "call_id": "call_1", "name": "read", "arguments": "{}"},
            {"type": "function_call_output", "call_id": "call_1", "output": "file content"},
            {"type": "message", "role": "user", "content": "Now explain it."},
        ],
        "tools": [{"type": "function", "name": "read", "parameters": {"type": "object"}}],
    }


class SessionModeStoreTests(unittest.TestCase):
    def test_real_session_id_wins_over_shared_parent_and_affinity(self):
        self.assertEqual(extract_session_id({
            "X-OpenCode-Session-Id": SESSION_A,
            "x-opencode-parent-session-id": SESSION_B,
            "x-session-id": "shared-affinity",
        }), SESSION_A)
        self.assertIsNone(extract_session_id({"x-session-id": SESSION_A}))
        self.assertIsNone(extract_session_id({"x-opencode-session-id": "not-an-opencode-session"}))
        self.assertIsNone(extract_session_id({"x-opencode-session-id": "ses_bad\nheader"}))

    def test_persistence_is_upstream_scoped_and_contains_no_raw_session_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session_modes.json"
            store = SessionModeStore(path, "https://upstream.test/")
            self.assertFalse(store.should_sanitize(SESSION_A))
            self.assertTrue(store.mark_sanitized(SESSION_A))
            self.assertFalse(store.mark_sanitized(SESSION_A))
            self.assertFalse(store.should_sanitize(SESSION_B))
            persisted = path.read_text(encoding="utf-8")
            self.assertNotIn(SESSION_A, persisted)
            self.assertNotIn("upstream.test", persisted)
            restored = SessionModeStore(path, "https://upstream.test")
            self.assertTrue(restored.should_sanitize(SESSION_A))
            self.assertFalse(SessionModeStore(path, "https://another.test").should_sanitize(SESSION_A))
            self.assertTrue(restored.reset(SESSION_A))
            self.assertFalse(SessionModeStore(path, "https://upstream.test").should_sanitize(SESSION_A))

    def test_concurrent_marks_are_not_lost(self):
        from concurrent.futures import ThreadPoolExecutor

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session_modes.json"
            store = SessionModeStore(path, "https://upstream.test")
            ids = [f"ses_worker_{i}" for i in range(12)]
            with ThreadPoolExecutor(max_workers=4) as workers:
                self.assertTrue(all(workers.map(store.mark_sanitized, ids)))
            restored = SessionModeStore(path, "https://upstream.test")
            self.assertEqual(restored.count, len(ids))
            self.assertTrue(all(restored.should_sanitize(session_id) for session_id in ids))

    def test_persistence_failure_keeps_runtime_flag_and_reports_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = SessionModeStore(Path(tmp) / "session_modes.json", "https://upstream.test")
            with patch("session_modes.os.replace", side_effect=OSError("disk not writable")):
                self.assertTrue(store.mark_sanitized(SESSION_A))
            self.assertTrue(store.should_sanitize(SESSION_A))
            self.assertFalse(store.persistence_ok)

    def test_corrupt_file_does_not_stop_proxy(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session_modes.json"
            path.write_text("not JSON", encoding="utf-8")
            store = SessionModeStore(path, "https://upstream.test")
            self.assertEqual(store.count, 0)
            self.assertFalse(store.persistence_ok)

    def test_excessively_nested_state_does_not_stop_proxy(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session_modes.json"
            path.write_text("[" * 2000 + "0" + "]" * 2000, encoding="utf-8")
            store = SessionModeStore(path, "https://upstream.test")
            self.assertEqual(store.count, 0)
            self.assertFalse(store.persistence_ok)


class SessionModeRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state_path = Path(self.tmp.name) / "session_modes.json"
        self.store = SessionModeStore(self.state_path, proxy.UPSTREAM)
        self.saved_store = proxy.SESSION_MODES
        self.saved_enabled = proxy.SESSION_MODE_MEMORY
        proxy.SESSION_MODES = self.store
        proxy.SESSION_MODE_MEMORY = True
        self.addCleanup(setattr, proxy, "SESSION_MODES", self.saved_store)
        self.addCleanup(setattr, proxy, "SESSION_MODE_MEMORY", self.saved_enabled)

    async def asyncTearDown(self):
        if proxy._SHARED_CLIENT is not None:
            await proxy._SHARED_CLIENT.aclose()
        proxy._SHARED_CLIENT = None

    def client(self, responders):
        mock = test_proxy._QueuedMockTransport(responders)
        test_proxy._reset_proxy_for_test(mock)
        transport = httpx.ASGITransport(app=proxy.app)
        return mock, httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8788")

    def assert_sanitized_history(self, payload):
        items = payload["input"]
        self.assertFalse(any(item.get("type") == "reasoning" for item in items))
        self.assertFalse(any("id" in item for item in items))
        self.assertEqual(items[2]["phase"], "commentary")
        calls = [item for item in items if item.get("type") in ("function_call", "function_call_output")]
        self.assertEqual([item["call_id"] for item in calls], ["call_1", "call_1"])
        self.assertEqual(payload["tools"], sample_payload()["tools"])
        self.assertFalse(payload["store"])

    async def test_first_mismatch_retries_then_only_that_session_is_preemptive(self):
        mock, client = self.client([
            completed_response(), mismatch_response(), completed_response(),
            completed_response(), completed_response(), completed_response(),
        ])
        payload = sample_payload()
        original = copy.deepcopy(payload)
        async with client:
            self.assertEqual((await client.post("/v1/responses", json=payload, headers={"x-opencode-session-id": SESSION_A})).status_code, 200)
            self.assertEqual(mock.calls[0]["body"], original)
            self.assertFalse(self.store.should_sanitize(SESSION_A))

            self.assertEqual((await client.post("/v1/responses", json=payload, headers={"x-opencode-session-id": SESSION_A})).status_code, 200)
            self.assertEqual(mock.calls[1]["body"], original)
            self.assert_sanitized_history(mock.calls[2]["body"])
            self.assertTrue(self.store.should_sanitize(SESSION_A))

            # Simulate a service restart by loading a fresh store from disk.
            proxy.SESSION_MODES = SessionModeStore(self.state_path, proxy.UPSTREAM)
            self.assertEqual((await client.post("/v1/responses", json=payload, headers={
                "x-opencode-session-id": SESSION_A, "authorization": "Bearer test-not-a-real-key",
            })).status_code, 200)
            self.assert_sanitized_history(mock.calls[3]["body"])
            self.assertEqual(mock.calls[3]["headers"]["authorization"], "Bearer test-not-a-real-key")
            self.assertEqual(mock.calls[3]["headers"]["x-opencode-session-id"], SESSION_A)

            # Different child sessions MUST NOT share the parent's mode.
            self.assertEqual((await client.post("/v1/responses", json=payload, headers={
                "x-opencode-session-id": SESSION_B, "x-opencode-parent-session-id": SESSION_A,
                "x-session-affinity": SESSION_A,
            })).status_code, 200)
            self.assertEqual(mock.calls[4]["body"], original)
            self.assertFalse(proxy.SESSION_MODES.should_sanitize(SESSION_B))

            # Missing identity must not cause all callers to share a global flag.
            self.assertEqual((await client.post("/v1/responses", json=payload)).status_code, 200)
            self.assertEqual(mock.calls[5]["body"], original)

        self.assertEqual(payload, original)
        self.assertEqual(proxy.STATS["session_modes_activated"], 1)
        self.assertEqual(proxy.STATS["preemptive_sanitizations"], 1)
        self.assertEqual(proxy.STATS["stateless_fallbacks_applied"], 1)
        self.assertEqual(proxy.STATS["successful_recoveries"], 1)
        self.assertEqual(proxy.STATS["responses_upstream_attempts"], 6)

    async def test_sse_precommit_error_marks_session_and_preserves_stream(self):
        error = b"event: response.failed\ndata: " + json.dumps({
            "type": "response.failed", "response": {"status": "failed", "error": {"message": MISMATCH_MESSAGE}},
        }).encode() + b"\n\n"
        good = b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"OK"}\n\n'
        good += b'event: response.completed\ndata: {"type":"response.completed"}\n\n'
        mock, client = self.client([
            test_proxy._sse_response([(error, 0)]), test_proxy._sse_response([(good, 0)]),
            test_proxy._sse_response([(good, 0)]),
        ])
        payload = {**sample_payload(), "stream": True}
        async with client:
            first = await client.post("/v1/responses", json=payload, headers={"x-opencode-session-id": SESSION_A})
            second = await client.post("/v1/responses", json=payload, headers={"x-opencode-session-id": SESSION_A})
        self.assertEqual(first.status_code, 200)
        self.assertNotIn("response.failed", first.text)
        self.assertEqual(first.content, good)
        self.assertEqual(second.content, good)
        self.assertEqual(len(mock.calls), 3)
        self.assert_sanitized_history(mock.calls[-1]["body"])

    async def test_json_200_failed_response_also_marks_session(self):
        failed = httpx.Response(200, json={"status": "failed", "error": {"message": MISMATCH_MESSAGE}})
        mock, client = self.client([failed, completed_response(), completed_response()])
        async with client:
            await client.post("/v1/responses", json=sample_payload(), headers={"x-opencode-session-id": SESSION_A})
            await client.post("/v1/responses", json=sample_payload(), headers={"x-opencode-session-id": SESSION_A})
        self.assertEqual(len(mock.calls), 3)
        self.assertTrue(self.store.should_sanitize(SESSION_A))
        self.assert_sanitized_history(mock.calls[-1]["body"])

    async def test_late_split_sse_error_is_forwarded_but_remembered_for_next_call(self):
        start = b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"partial"}\n\n'
        late_error = b"event: response.failed\ndata: " + json.dumps({
            "type": "response.failed", "response": {"status": "failed", "error": {"message": MISMATCH_MESSAGE}},
        }).encode() + b"\n\n"
        mock, client = self.client([
            test_proxy._sse_response([(start, 0), (late_error[:30], 0), (late_error[30:], 0)]),
            completed_response(),
        ])
        async with client:
            first = await client.post("/v1/responses", json=sample_payload(), headers={"x-opencode-session-id": SESSION_A})
            self.assertEqual(first.content, start + late_error)
            self.assertEqual(len(mock.calls), 1)  # Never replay already-visible output.
            await client.post("/v1/responses", json=sample_payload(), headers={"x-opencode-session-id": SESSION_A})
        self.assertEqual(len(mock.calls), 2)
        self.assertTrue(self.store.should_sanitize(SESSION_A))
        self.assert_sanitized_history(mock.calls[-1]["body"])
        self.assertEqual(proxy.STATS["successful_recoveries"], 0)

    async def test_429_402_and_image_limit_do_not_permanently_mark_session(self):
        mock, client = self.client([
            httpx.Response(429, json={"error": {"message": "Too many requests"}}), completed_response(),
            httpx.Response(402, json={"error": {"message": "Budget pool quota has been exhausted"}}),
            httpx.Response(400, json={"error": {"message": "Exceeded maximum number of images"}}), completed_response(),
            completed_response(),
        ])
        async with client:
            self.assertEqual((await client.post("/v1/responses", json=sample_payload(), headers={"x-opencode-session-id": SESSION_A})).status_code, 200)
            self.assertFalse(self.store.should_sanitize(SESSION_A))
            self.assertEqual((await client.post("/v1/responses", json=sample_payload(), headers={"x-opencode-session-id": SESSION_A})).status_code, 402)
            self.assertFalse(self.store.should_sanitize(SESSION_A))
            self.assertEqual((await client.post("/v1/responses", json=sample_payload(), headers={"x-opencode-session-id": SESSION_A})).status_code, 200)
            self.assertFalse(self.store.should_sanitize(SESSION_A))
            await client.post("/v1/responses", json=sample_payload(), headers={"x-opencode-session-id": SESSION_A})
        self.assertEqual(mock.calls[-1]["body"], sample_payload())
        self.assertEqual(proxy.STATS["session_modes_activated"], 0)
        self.assertEqual(proxy.STATS["preemptive_sanitizations"], 0)

    async def test_id_only_history_is_not_blindly_sanitized(self):
        self.store.mark_sanitized(SESSION_A)
        payload = {"model": "gpt-6.1-sol", "previous_response_id": "resp_old", "input": "follow up"}
        mock, client = self.client([completed_response()])
        async with client:
            r = await client.post("/v1/responses", json=payload, headers={"x-opencode-session-id": SESSION_A})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(mock.calls[0]["body"], payload)
        self.assertEqual(proxy.STATS["preemptive_sanitization_skipped"], 1)

    async def test_marked_mode_rechecked_after_concurrent_request_waits_at_gate(self):
        mock, client = self.client([completed_response()])

        async def gate(_request):
            self.store.mark_sanitized(SESSION_A)
            return True

        with patch.object(proxy, "_wait_for_rate_limit_gate", gate):
            async with client:
                await client.post("/v1/responses", json=sample_payload(), headers={"x-opencode-session-id": SESSION_A})
        self.assertEqual(len(mock.calls), 1)
        self.assert_sanitized_history(mock.calls[0]["body"])

    async def test_disable_switch_restores_original_reactive_behavior(self):
        self.store.mark_sanitized(SESSION_A)
        proxy.SESSION_MODE_MEMORY = False
        mock, client = self.client([completed_response()])
        async with client:
            await client.post("/v1/responses", json=sample_payload(), headers={"x-opencode-session-id": SESSION_A})
        self.assertEqual(mock.calls[0]["body"], sample_payload())
        self.assertEqual(proxy.STATS["preemptive_sanitizations"], 0)

    async def test_local_mode_inspection_reset_and_no_upstream_admin_requests(self):
        self.store.mark_sanitized(SESSION_A)
        mock, client = self.client([completed_response()])
        async with client:
            mode = await client.post("/_session/mode", json={"session_id": SESSION_A})
            self.assertEqual(mode.json()["mode"], "sanitize")
            reset = await client.post("/_session/reset", json={"session_id": SESSION_A})
            self.assertEqual(reset.json()["mode"], "passthrough")
            self.assertTrue(reset.json()["reset"])
            self.assertEqual((await client.post("/_session/reset", json={})).status_code, 400)
            await client.post("/v1/responses", json=sample_payload(), headers={"x-opencode-session-id": SESSION_A})
        self.assertEqual(len(mock.calls), 1)
        self.assertEqual(mock.calls[0]["body"], sample_payload())

        remote = httpx.ASGITransport(app=proxy.app, client=("192.168.1.20", 12345))
        async with httpx.AsyncClient(transport=remote, base_url="http://127.0.0.1:8788") as remote_client:
            r = await remote_client.post("/_session/reset", json={"session_id": SESSION_A})
        self.assertEqual(r.status_code, 403)


if __name__ == "__main__":
    unittest.main()
