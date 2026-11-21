"""Multi-tool wire fixtures and bounded compressed-body regression tests."""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zlib

import httpx
from starlette.datastructures import Headers
import zstandard

import proxy
from request_payloads import RequestPayloadError, parse_responses_payload
from session_modes import SessionIdentity, SessionModeStore, detect_client, extract_session_identity
from test_proxy import _QueuedMockTransport, _reset_proxy_for_test, _sse_response
from test_session_modes import MISMATCH_MESSAGE, completed_response, mismatch_response, sample_payload


THREAD_A = "0198e88a-149f-73de-b116-aeb842539bb5"
THREAD_B = "0198e88a-149f-73de-b116-aeb842539bb6"
DSH_AGENT = "deepseek-harness/0.2.2-rc.3 (+https://github.com/deepseek-ai/deepseek-harness) dsh-profile/web"
CODEX_AGENT = "codex_cli_rs/0.134.0"


def headers_for(client: str, session: str = THREAD_A) -> dict[str, str]:
    if client == "opencode":
        return {"x-opencode-session-id": "ses_" + session}
    if client == "codex":
        return {"user-agent": CODEX_AGENT, "thread-id": session, "session-id": "shared-cache-affinity"}
    if client == "dsh":
        return {"user-agent": DSH_AGENT, "x-client-request-id": session}
    return {"x-agentrouter-client": "generic", "x-agentrouter-session-id": session}


class ClientIdentityTests(unittest.TestCase):
    def test_native_headers_and_metadata(self):
        cases = [
            ({"x-opencode-session-id": "ses_original", "thread-id": THREAD_A}, None, "opencode", "ses_original"),
            (headers_for("codex"), None, "codex", THREAD_A),
            ({"user-agent": CODEX_AGENT, "session-id": THREAD_A}, None, "codex", THREAD_A),
            ({"user-agent": "Codex Desktop", "session-id": THREAD_A}, None, "codex", THREAD_A),
            ({"user-agent": CODEX_AGENT, "x-codex-turn-metadata": json.dumps({"thread_id": THREAD_A, "parent_thread_id": THREAD_B})}, None, "codex", THREAD_A),
            ({}, {"client_metadata": {"thread_id": THREAD_A, "x-codex-installation-id": "installation"}}, "codex", THREAD_A),
            ({}, {"client_metadata": {"x-codex-turn-metadata": json.dumps({"thread_id": THREAD_A})}}, "codex", THREAD_A),
            ({"user-agent": DSH_AGENT, "session_id": "dsh-request-1", "x-client-request-id": "dsh-request-1"}, None, "dsh", "dsh-request-1"),
            ({"user-agent": DSH_AGENT, "x-session-id": "dsh-openrouter-1"}, None, "dsh", "dsh-openrouter-1"),
            ({"user-agent": DSH_AGENT, "session-id": "dsh-codex-transport", "x-client-request-id": "dsh-codex-transport"}, None, "dsh", "dsh-codex-transport"),
            (headers_for("generic"), None, "generic", THREAD_A),
        ]
        for headers, payload, client, session_id in cases:
            with self.subTest(client=client, session_id=session_id):
                identity = extract_session_identity(headers, payload)
                self.assertIsNotNone(identity)
                self.assertEqual((identity.client, identity.session_id), (client, session_id))

    def test_codex_thread_is_preferred_over_cache_affinity_and_turn_id(self):
        h = {
            "user-agent": CODEX_AGENT, "thread-id": THREAD_A.upper(),
            "session-id": THREAD_B, "x-codex-parent-thread-id": THREAD_B,
            "x-codex-turn-metadata": json.dumps({"thread_id": THREAD_A, "turn_id": "turn-1"}),
        }
        first = extract_session_identity(h)
        second = extract_session_identity({**h, "session-id": "another-affinity", "x-codex-turn-metadata": json.dumps({"thread_id": THREAD_A, "turn_id": "turn-2"})})
        self.assertEqual(first.session_id, THREAD_A)
        self.assertEqual((first.client, first.session_id), (second.client, second.session_id))

    def test_unknown_cache_parent_and_request_ids_never_create_a_session(self):
        cases = [
            ({"x-client-request-id": THREAD_A}, {"prompt_cache_key": THREAD_A}),
            ({"x-session-id": THREAD_A}, None),
            ({"user-agent": CODEX_AGENT, "x-client-request-id": THREAD_A}, {"prompt_cache_key": THREAD_A}),
            ({"user-agent": CODEX_AGENT, "x-codex-parent-thread-id": THREAD_A}, None),
            ({"user-agent": DSH_AGENT}, {"prompt_cache_key": "shared-key"}),
            ({}, {"metadata": {"session_id": THREAD_A}}),
            ({"user-agent": "curl/8.0", "session-id": THREAD_A}, None),
        ]
        for headers, payload in cases:
            with self.subTest(headers=headers):
                self.assertIsNone(extract_session_identity(headers, payload))

    def test_invalid_conflicting_or_oversized_identity_is_not_guessed(self):
        cases = [
            {"x-opencode-session-id": "ses_first", "X-OpenCode-Session-Id": "ses_second"},
            {"user-agent": CODEX_AGENT, "thread-id": "not-a-thread", "session-id": THREAD_A},
            {"user-agent": CODEX_AGENT, "thread-id": THREAD_A, "x-codex-turn-metadata": json.dumps({"thread_id": THREAD_B})},
            {"user-agent": CODEX_AGENT, "x-codex-turn-metadata": "{" * 17000},
            {"user-agent": DSH_AGENT, "session_id": "one", "x-client-request-id": "two"},
            {"user-agent": DSH_AGENT, "x-client-request-id": "unsafe\nvalue"},
            {"user-agent": DSH_AGENT, "x-client-request-id": "x" * 300},
            {"x-agentrouter-client": "unsupported", "x-agentrouter-session-id": "one"},
            {"x-agentrouter-client": "generic"},
        ]
        for headers in cases:
            with self.subTest(headers=list(headers)):
                self.assertIsNone(extract_session_identity(headers))

    def test_dsh_codex_transport_is_not_misclassified_as_codex(self):
        self.assertEqual(detect_client({"user-agent": DSH_AGENT, "originator": "codex_cli_rs"}), "dsh")
        identity = extract_session_identity({"user-agent": DSH_AGENT, "session-id": THREAD_A})
        self.assertEqual(identity.client, "dsh")

    def test_duplicate_identity_or_product_headers_are_not_guessed(self):
        for name, values in (
            ("thread-id", (THREAD_A, THREAD_B)),
            ("x-codex-turn-metadata", (json.dumps({"thread_id": THREAD_A}), json.dumps({"thread_id": THREAD_B}))),
            ("user-agent", (CODEX_AGENT, DSH_AGENT)),
            ("originator", ("codex_cli_rs", "another-product")),
        ):
            raw = [(b"user-agent", CODEX_AGENT.encode()), (b"thread-id", THREAD_A.encode())]
            raw.extend((name.encode(), value.encode()) for value in values)
            with self.subTest(name=name):
                self.assertIsNone(extract_session_identity(Headers(raw=raw)))

    def test_conflicting_or_null_legacy_codex_aliases_do_not_create_a_flag(self):
        for payload in (
            {"client_metadata": {"session_id": THREAD_B}},
            {"client_metadata": {"thread_id": None}},
            {"client_metadata": {"x-codex-turn-metadata": json.dumps({"session_id": THREAD_B})}},
        ):
            with self.subTest(payload=payload):
                self.assertIsNone(extract_session_identity({"user-agent": CODEX_AGENT, "session-id": THREAD_A}, payload))
        identity = extract_session_identity(
            {"user-agent": CODEX_AGENT, "session-id": THREAD_A.upper()},
            {"client_metadata": {"session_id": THREAD_A}},
        )
        self.assertEqual(identity.session_id, THREAD_A)

    def test_explicit_codex_uuid_is_validated_and_canonicalized(self):
        identity = extract_session_identity({"x-agentrouter-client": "codex", "x-agentrouter-session-id": THREAD_A.upper()})
        self.assertEqual(identity.session_id, THREAD_A)
        self.assertEqual(SessionIdentity("codex", THREAD_A.upper()).session_id, THREAD_A)
        self.assertIsNone(extract_session_identity({"x-agentrouter-client": "codex", "x-agentrouter-session-id": "shared-cache-affinity"}))

    def test_client_namespace_and_legacy_opencode_flag_compatibility(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session_modes.json"
            upstream = "https://upstream.test"
            old_key = hashlib.sha256(f"agentrouter-session-mode-v1\0{upstream}\0ses_existing".encode()).hexdigest()
            path.write_text(json.dumps({"version": 1, "sessions": {old_key: {"mode": "sanitize", "marked_at": "2026-11-14T00:00:00Z"}}}), encoding="utf-8")
            store = SessionModeStore(path, upstream)
            self.assertTrue(store.should_sanitize("ses_existing"))
            self.assertFalse(store.should_sanitize(SessionIdentity("dsh", "ses_existing")))
            codex = SessionIdentity("codex", THREAD_A)
            dsh = SessionIdentity("dsh", THREAD_A)
            generic = SessionIdentity("generic", THREAD_A)
            self.assertTrue(store.mark_sanitized(codex))
            self.assertFalse(store.should_sanitize(dsh))
            self.assertFalse(store.should_sanitize(generic))
            self.assertTrue(store.mark_sanitized(dsh))
            restored = SessionModeStore(path, upstream)
            self.assertTrue(restored.should_sanitize(codex))
            self.assertTrue(restored.should_sanitize(dsh))
            self.assertTrue(restored.should_sanitize("ses_existing"))
            self.assertTrue(restored.reset(codex))
            self.assertTrue(restored.should_sanitize(dsh))
            serialized = path.read_text(encoding="utf-8")
            self.assertNotIn(THREAD_A, serialized)
            self.assertNotIn("ses_existing", serialized)
            self.assertNotIn(upstream, serialized)


class RequestEncodingTests(unittest.TestCase):
    def test_identity_gzip_deflate_zstd_and_unknown_size_frames(self):
        payload = {"model": "test", "input": "中文", "store": False}
        body = json.dumps(payload, ensure_ascii=False).encode()
        for encoding, encoded in (
            ("", body), ("identity", body), ("gzip", gzip.compress(body)),
            ("deflate", zlib.compress(body)), ("zstd", zstandard.ZstdCompressor().compress(body)),
            ("zstd", zstandard.ZstdCompressor(write_content_size=False).compress(body)),
        ):
            with self.subTest(encoding=encoding):
                self.assertEqual(parse_responses_payload(encoded, encoding, 4096), payload)

    def test_decompression_size_limit_and_invalid_json_fail_safely(self):
        large = json.dumps({"input": "x" * 20000}).encode()
        for encoding, body in (
            ("gzip", gzip.compress(large)), ("deflate", zlib.compress(large)),
            ("zstd", zstandard.ZstdCompressor().compress(large)),
            ("zstd", zstandard.ZstdCompressor(write_content_size=False).compress(large)),
        ):
            with self.subTest(encoding=encoding), self.assertRaises(RequestPayloadError) as caught:
                parse_responses_payload(body, encoding, 1024)
            self.assertEqual(caught.exception.status_code, 413)
        for body in (b"[]", b"null", b"42", b"{", b'{"input": NaN}'):
            with self.subTest(body=body), self.assertRaises(RequestPayloadError) as caught:
                parse_responses_payload(body, "", 1024)
            self.assertEqual(caught.exception.status_code, 400)

    def test_unsupported_and_corrupt_compression_does_not_echo_body(self):
        for encoding, body, status in (
            ("br", b"secret-prompt", 415), ("gzip", b"secret-prompt", 400),
            ("zstd", b"secret-prompt", 400), ("gzip, zstd", b"secret-prompt", 415),
        ):
            with self.subTest(encoding=encoding), self.assertRaises(RequestPayloadError) as caught:
                parse_responses_payload(body, encoding, 1024)
            self.assertEqual(caught.exception.status_code, status)
            self.assertNotIn("secret-prompt", str(caught.exception))

    def test_trailing_and_concatenated_compressed_frames_are_rejected(self):
        raw = b'{"input":"test"}'
        for encoding, body in (("gzip", gzip.compress(raw)), ("zstd", zstandard.ZstdCompressor().compress(raw))):
            for extra in (b"extra", body):
                with self.subTest(encoding=encoding), self.assertRaises(RequestPayloadError) as caught:
                    parse_responses_payload(body + extra, encoding, 4096)
                self.assertEqual(caught.exception.status_code, 400)

    def test_missing_zstd_dependency_reports_unsupported_not_silent_passthrough(self):
        with patch("request_payloads.zstandard", None), self.assertRaises(RequestPayloadError) as caught:
            parse_responses_payload(b"test", "zstd", 4096)
        self.assertEqual(caught.exception.status_code, 415)

    def test_truncated_zstd_is_invalid_not_oversized(self):
        raw = b'{"input":"test"}'
        for known_size in (True, False):
            frame = zstandard.ZstdCompressor(write_content_size=known_size).compress(raw)
            for lost in (1, 3, 8):
                with self.subTest(known_size=known_size, lost=lost), self.assertRaises(RequestPayloadError) as caught:
                    parse_responses_payload(frame[:-lost], "zstd", 4096)
                self.assertEqual(caught.exception.status_code, 400)

    def test_unknown_size_small_window_bomb_is_still_bounded(self):
        raw = json.dumps({"input": "x" * 20000}).encode()
        params = zstandard.ZstdCompressionParameters.from_level(1, window_log=10, write_content_size=0)
        frame = zstandard.ZstdCompressor(compression_params=params).compress(raw)
        self.assertLessEqual(zstandard.get_frame_parameters(frame).window_size, 4096)
        with self.assertRaises(RequestPayloadError) as caught:
            parse_responses_payload(frame, "zstd", 4096)
        self.assertEqual(caught.exception.status_code, 413)


class MultiClientProxyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = SessionModeStore(Path(self.tmp.name) / "session_modes.json", proxy.UPSTREAM)
        self.original_store = proxy.SESSION_MODES
        self.original_enabled = proxy.SESSION_MODE_MEMORY
        proxy.SESSION_MODES = self.store
        proxy.SESSION_MODE_MEMORY = True
        self.addCleanup(setattr, proxy, "SESSION_MODES", self.original_store)
        self.addCleanup(setattr, proxy, "SESSION_MODE_MEMORY", self.original_enabled)

    async def asyncTearDown(self):
        if proxy._SHARED_CLIENT is not None:
            await proxy._SHARED_CLIENT.aclose()
        proxy._SHARED_CLIENT = None

    def client(self, responses):
        mock = _QueuedMockTransport(responses)
        _reset_proxy_for_test(mock)
        return mock, httpx.AsyncClient(transport=httpx.ASGITransport(app=proxy.app), base_url="http://127.0.0.1:8788")

    async def test_each_tool_is_passthrough_then_remembers_its_own_first_mismatch(self):
        clients = ("codex", "dsh", "generic", "opencode")
        responses = [response for _ in clients for response in (
            completed_response(), mismatch_response(), completed_response(), completed_response(),
        )]
        mock, client = self.client(responses)
        payload = sample_payload()
        async with client:
            for i, tool in enumerate(clients):
                headers = headers_for(tool)
                identity = extract_session_identity(headers)
                await client.post("/v1/responses", json=payload, headers=headers)
                self.assertEqual(mock.calls[4 * i]["body"], payload)
                self.assertFalse(self.store.should_sanitize(identity))
                await client.post("/v1/responses", json=payload, headers=headers)
                self.assertTrue(self.store.should_sanitize(identity))
                await client.post("/v1/responses", json=payload, headers=headers)
                self.assertFalse(any(item.get("type") == "reasoning" for item in mock.calls[4 * i + 3]["body"]["input"]))
        self.assertEqual(len(mock.calls), 16)
        self.assertEqual(proxy.STATS["session_modes_activated"], 4)
        self.assertEqual(proxy.STATS["preemptive_sanitizations"], 4)
        for tool in clients:
            self.assertEqual(proxy.STATS["clients"][tool], {
                "requests": 3, "identified_requests": 3, "preemptive_sanitizations": 1, "modes_activated": 1,
            })

    async def test_overlapping_clients_do_not_exchange_modes_or_credentials(self):
        async def responder(_idx, body):
            if body["metadata"]["fixture"] == "codex" and any(item.get("type") == "reasoning" for item in body["input"]):
                await asyncio.sleep(0.02)
                return mismatch_response()
            return completed_response()

        mock, client = self.client([responder] * 4)
        codex_payload = {**sample_payload(), "metadata": {"fixture": "codex"}}
        dsh_payload = {**sample_payload(), "metadata": {"fixture": "dsh"}}
        codex_headers = {**headers_for("codex"), "authorization": "Bearer codex-fixture"}
        dsh_headers = {**headers_for("dsh"), "authorization": "Bearer dsh-fixture"}
        async with client:
            first, second = await asyncio.gather(
                client.post("/v1/responses", json=codex_payload, headers=codex_headers),
                client.post("/v1/responses", json=dsh_payload, headers=dsh_headers),
            )
            self.assertEqual((first.status_code, second.status_code), (200, 200))
            await client.post("/v1/responses", json=dsh_payload, headers=dsh_headers)
        self.assertEqual(len(mock.calls), 4)
        self.assertTrue(self.store.should_sanitize(SessionIdentity("codex", THREAD_A)))
        self.assertFalse(self.store.should_sanitize(SessionIdentity("dsh", THREAD_A)))
        for call in mock.calls:
            fixture = call["body"]["metadata"]["fixture"]
            self.assertEqual(call["headers"]["authorization"], f"Bearer {fixture}-fixture")
            if fixture == "dsh":
                self.assertEqual(call["body"], dsh_payload)

    async def test_healthy_compressed_bytes_are_unchanged_and_retries_are_plain_json(self):
        responses = [response for _ in range(2) for response in (
            completed_response(), mismatch_response(), completed_response(), completed_response(),
        )]
        mock, client = self.client(responses)
        body = json.dumps(sample_payload(), separators=(",", ":")).encode()
        async with client:
            for index, (encoding, encoded) in enumerate((
                ("gzip", gzip.compress(body)), ("zstd", zstandard.ZstdCompressor().compress(body)),
            )):
                headers = {**headers_for("codex", THREAD_A if index == 0 else THREAD_B), "content-encoding": encoding, "content-type": "application/json"}
                await client.post("/v1/responses", content=encoded, headers=headers)
                self.assertEqual(mock.calls[4 * index]["content"], encoded)
                self.assertEqual(mock.calls[4 * index]["headers"]["content-encoding"], encoding)
                await client.post("/v1/responses", content=encoded, headers=headers)
                await client.post("/v1/responses", content=encoded, headers=headers)
                for call in mock.calls[4 * index + 2:4 * index + 4]:
                    self.assertNotIn("content-encoding", call["headers"])
                    self.assertIsInstance(call["body"], dict)
                    self.assertFalse(any(item.get("type") == "reasoning" for item in call["body"]["input"]))
        self.assertEqual(len(mock.calls), 8)

    async def test_unidentified_requests_recover_but_do_not_share_memory(self):
        mock, client = self.client([mismatch_response(), completed_response(), mismatch_response(), completed_response()])
        async with client:
            for _ in range(2):
                result = await client.post("/v1/responses", json=sample_payload(), headers={"user-agent": DSH_AGENT})
                self.assertEqual(result.status_code, 200)
        self.assertEqual(len(mock.calls), 4)
        self.assertEqual(self.store.count, 0)
        self.assertEqual(proxy.STATS["requests_without_session_id"], 2)

    async def test_reference_only_recovery_never_discards_missing_history(self):
        mock, client = self.client([mismatch_response()] * 3)
        bad_inputs = [
            {"model": "test", "previous_response_id": "resp_only_on_server", "input": "continue"},
            {"model": "test", "input": [{"type": "item_reference", "id": "item_on_server"}, {"role": "user", "content": "continue"}]},
            {"model": "test", "input": [{"type": "function_call_output", "call_id": "unavailable_call", "output": "tool result"}]},
        ]
        async with client:
            for payload in bad_inputs:
                r = await client.post("/v1/responses", json=payload, headers=headers_for("codex"))
                self.assertEqual(r.status_code, 400)
                self.assertIn(MISMATCH_MESSAGE, r.text)
        self.assertEqual(len(mock.calls), 3)
        self.assertEqual(proxy.STATS["unsafe_recovery_skipped"], 3)
        self.assertEqual(proxy.STATS["stateless_fallbacks_applied"], 0)
        self.assertEqual([call["body"] for call in mock.calls], bad_inputs)

    async def test_json_200_unsafe_error_is_not_counted_as_successful_recovery(self):
        failed = httpx.Response(200, json={"status": "failed", "error": {"message": MISMATCH_MESSAGE}})
        mock, client = self.client([failed])
        async with client:
            r = await client.post("/v1/responses", json={"previous_response_id": "resp_old", "input": "continue"}, headers=headers_for("codex"))
        self.assertEqual(r.json()["status"], "failed")
        self.assertEqual(len(mock.calls), 1)
        self.assertEqual(proxy.STATS["successful_recoveries"], 0)

    async def test_unsafe_precommit_sse_error_is_json_without_destructive_retry(self):
        failed = b'event: response.failed\ndata: ' + json.dumps({
            "type": "response.failed", "response": {"status": "failed", "error": {"message": MISMATCH_MESSAGE}},
        }).encode() + b"\n\n"
        mock, client = self.client([_sse_response([(failed, 0)])])
        payload = {"model": "test", "previous_response_id": "resp_server_history", "input": "continue", "stream": True}
        async with client:
            r = await client.post("/v1/responses", json=payload, headers=headers_for("codex"))
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.headers["content-type"], "application/json")
        self.assertEqual(r.json()["error"]["message"], MISMATCH_MESSAGE)
        self.assertEqual(len(mock.calls), 1)
        self.assertEqual(mock.calls[0]["body"], payload)
        self.assertEqual(proxy.STATS["stateless_fallbacks_applied"], 0)
        self.assertTrue(self.store.should_sanitize(SessionIdentity("codex", THREAD_A)))

    async def test_sanitized_json_preserves_escaped_unicode_without_codec_errors(self):
        self.store.mark_sanitized(SessionIdentity("dsh", THREAD_A))
        payload = sample_payload()
        payload["input"][-1]["content"] = "escaped-surrogate-\ud800"
        mock, client = self.client([completed_response()])
        async with client:
            r = await client.post("/v1/responses", content=json.dumps(payload).encode(), headers={
                **headers_for("dsh"), "content-type": "application/json",
            })
        self.assertEqual(r.status_code, 200)
        self.assertEqual(mock.calls[0]["body"]["input"][-1]["content"], payload["input"][-1]["content"])
        self.assertEqual(len(mock.calls), 1)

    async def test_local_reset_chooses_client_namespace(self):
        self.store.mark_sanitized(SessionIdentity("codex", THREAD_A))
        self.store.mark_sanitized(SessionIdentity("dsh", THREAD_A))
        mock, client = self.client([])
        async with client:
            r = await client.post("/_session/reset", json={"client": "codex", "session_id": THREAD_A})
            self.assertEqual(r.json()["mode"], "passthrough")
            dsh = await client.post("/_session/mode", json={"client": "dsh", "session_id": THREAD_A})
            self.assertEqual(dsh.json()["mode"], "sanitize")
        self.assertEqual(len(mock.calls), 0)

    async def test_local_codex_mode_and_reset_accept_canonical_uuid_only(self):
        self.store.mark_sanitized(SessionIdentity("codex", THREAD_A))
        mock, client = self.client([])
        async with client:
            mode = await client.post("/_session/mode", json={"client": "codex", "session_id": THREAD_A.upper()})
            self.assertEqual(mode.json()["mode"], "sanitize")
            bad = await client.post("/_session/reset", json={"client": "codex", "session_id": "not-a-thread"})
            self.assertEqual(bad.status_code, 400)
            reset = await client.post("/_session/reset", json={"client": "codex", "session_id": THREAD_A.upper()})
            self.assertEqual(reset.json()["mode"], "passthrough")
            self.assertTrue(reset.json()["reset"])
        self.assertEqual(len(mock.calls), 0)

    async def test_late_split_sse_error_marks_only_the_correct_non_opencode_client(self):
        partial = b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"partial"}\n\n'
        failed = b'event: response.failed\ndata: ' + json.dumps({
            "type": "response.failed", "response": {"status": "failed", "error": {"message": MISMATCH_MESSAGE}},
        }).encode() + b"\n\n"
        mock, client = self.client([
            _sse_response([(partial, 0), (failed[:17], 0), (failed[17:], 0)]), completed_response(), completed_response(),
        ])
        async with client:
            first = await client.post("/v1/responses", json=sample_payload(), headers=headers_for("dsh"))
            self.assertEqual(first.content, partial + failed)
            await client.post("/v1/responses", json=sample_payload(), headers=headers_for("dsh"))
            await client.post("/v1/responses", json=sample_payload(), headers=headers_for("codex"))
        self.assertEqual(len(mock.calls), 3)
        self.assertFalse(any(item.get("type") == "reasoning" for item in mock.calls[1]["body"]["input"]))
        self.assertEqual(mock.calls[2]["body"], sample_payload())
        self.assertTrue(self.store.should_sanitize(SessionIdentity("dsh", THREAD_A)))
        self.assertFalse(self.store.should_sanitize(SessionIdentity("codex", THREAD_A)))

    async def test_preemptive_mode_survives_changing_tools_model_or_api_key(self):
        identity = SessionIdentity("dsh", THREAD_A)
        self.store.mark_sanitized(identity)
        mock, client = self.client([completed_response(), completed_response()])
        async with client:
            for i in range(2):
                payload = {**sample_payload(), "model": f"model-{i}", "tools": [{"type": "function", "name": f"tool_{i}"}]}
                r = await client.post("/v1/responses", json=payload, headers={**headers_for("dsh"), "authorization": f"Bearer key-fixture-{i}"})
                self.assertEqual(r.status_code, 200)
                self.assertEqual(mock.calls[i]["body"]["tools"], payload["tools"])
                self.assertEqual(mock.calls[i]["headers"]["authorization"], f"Bearer key-fixture-{i}")
        self.assertEqual(proxy.STATS["preemptive_sanitizations"], 2)
        self.assertEqual(len(mock.calls), 2)

    async def test_unknown_client_byte_preserving_recovery_and_no_global_mode(self):
        mock, client = self.client([completed_response(), mismatch_response(), completed_response()])
        raw = json.dumps(sample_payload(), indent=3).encode()
        async with client:
            first = await client.post("/responses", content=raw, headers={"content-type": "application/json"})
            second = await client.post("/responses", content=raw, headers={"content-type": "application/json"})
        self.assertEqual((first.status_code, second.status_code), (200, 200))
        self.assertEqual(mock.calls[0]["content"], raw)
        self.assertEqual(mock.calls[1]["content"], raw)
        self.assertFalse(any(item.get("type") == "reasoning" for item in mock.calls[2]["body"]["input"]))
        self.assertEqual(self.store.count, 0)

    def test_recovery_gate_rejects_compaction_and_malformed_tool_links(self):
        for items in (
            [{"type": "compaction", "encrypted_content": "server-checkpoint"}],
            [{"type": "function_call_output", "call_id": ["invalid"], "output": "result"}],
            [{"type": "function_call_output", "call_id": "call_1", "output": "result"}, {"type": "function_call", "call_id": "call_1"}],
        ):
            with self.subTest(items=items):
                self.assertFalse(proxy.can_preemptively_sanitize({"input": items}))

    async def test_invalid_bodies_rejected_locally_without_any_upstream_attempt(self):
        mock, client = self.client([])
        async with client:
            for encoding, body, status in (("", b"[]", 400), ("gzip", b"corrupt", 400), ("br", b"data", 415)):
                r = await client.post("/v1/responses", content=body, headers={"content-encoding": encoding})
                self.assertEqual(r.status_code, status)
            with patch.object(proxy, "MAX_RESPONSES_REQUEST_BYTES", 128):
                r = await client.post("/v1/responses", content=b"x" * 129)
            self.assertEqual(r.status_code, 413)
        self.assertEqual(len(mock.calls), 0)


if __name__ == "__main__":
    unittest.main()
