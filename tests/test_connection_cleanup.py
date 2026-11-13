"""Connection cleanup regressions using a real one-slot HTTPX connection pool."""

from __future__ import annotations

import asyncio
import contextlib
import json
from pathlib import Path
import sys
import unittest
from unittest import mock

import anyio
import httpx
from starlette.requests import Request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import proxy


class ConnectionCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.writers = []
        self.server = await asyncio.start_server(self.serve, "127.0.0.1", 0)
        self.url = f"http://127.0.0.1:{self.server.sockets[0].getsockname()[1]}"
        self.client = httpx.AsyncClient(
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=0),
            timeout=httpx.Timeout(2, pool=0.08),
        )
        self.previous_client = proxy._SHARED_CLIENT

    async def asyncTearDown(self):
        proxy._SHARED_CLIENT = self.previous_client
        await self.client.aclose()
        self.server.close()
        await self.server.wait_closed()
        for writer in self.writers:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()

    async def serve(self, reader, writer):
        self.writers.append(writer)
        try:
            request = await reader.readuntil(b"\r\n\r\n")
            if request.startswith(b"GET /ok "):
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
                await writer.drain()
                return
            content_type = b"text/event-stream" if not request.startswith(b"GET /json ") else b"application/json"
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: " + content_type
                + b"\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n"
            )
            await writer.drain()
            # Hold this upstream response open until the client closes it.
            await reader.read()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()

    async def open_response(self, path="/stream"):
        return await self.client.send(self.client.build_request("GET", self.url + path), stream=True)

    async def assert_pool_available(self):
        result = await self.client.get(self.url + "/ok")
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.text, "OK")

    async def test_cancelled_cleanup_releases_real_pool_slot(self):
        response = await self.open_response()
        producer = asyncio.create_task(asyncio.sleep(100))
        with anyio.CancelScope() as scope:
            scope.cancel()
            await proxy._close_sse_producer(response, producer)
        self.assertTrue(response.is_closed)
        self.assertTrue(producer.done())
        await self.assert_pool_available()

    async def test_cleanup_without_producer_releases_slot_after_cancelled_json_read(self):
        response = await self.open_response("/json")
        with anyio.CancelScope() as scope:
            scope.cancel()
            await proxy._close_sse_producer(response, None)
        self.assertTrue(response.is_closed)
        await self.assert_pool_available()

    async def test_fifty_five_cancelled_streams_do_not_exhaust_pool(self):
        # More cancellations than the production pool's 50-connection limit.
        for _ in range(55):
            response = await self.open_response()
            producer = asyncio.create_task(asyncio.sleep(100))
            with anyio.CancelScope() as scope:
                scope.cancel()
                await proxy._close_sse_producer(response, producer)
            self.assertTrue(response.is_closed)
        await self.assert_pool_available()

    async def test_downstream_send_failure_before_iteration_closes_upstream(self):
        response = await self.open_response()
        producer = asyncio.create_task(asyncio.sleep(100))
        downstream = proxy.UpstreamStreamingResponse(
            proxy._stream_generator(response), upstream_response=response, producer_task=producer,
        )

        async def receive():
            await asyncio.sleep(100)

        async def send(_message):
            raise OSError("downstream closed before response start")

        with self.assertRaises(Exception) as raised:
            await downstream({"type": "http", "method": "POST"}, receive, send)
        error = raised.exception
        # AnyIO 4 wraps task-group failures; AnyIO 3 may raise OSError directly.
        while hasattr(error, "exceptions"):
            self.assertEqual(len(error.exceptions), 1)
            error = error.exceptions[0]
        self.assertIsInstance(error, OSError)
        self.assertEqual(str(error), "downstream closed before response start")
        self.assertTrue(response.is_closed)
        self.assertTrue(producer.done())
        await self.assert_pool_available()

    async def test_asgi_client_disconnect_releases_upstream_connection(self):
        response = await self.open_response()
        queue = asyncio.Queue()
        producer = asyncio.create_task(asyncio.sleep(100))
        downstream = proxy.UpstreamStreamingResponse(
            proxy._stream_generator(
                response, primed_chunks=[b": primed\n\n"], queue=queue, producer_task=producer,
            ),
            upstream_response=response,
            producer_task=producer,
        )
        body_sent = asyncio.Event()

        async def receive():
            await body_sent.wait()
            return {"type": "http.disconnect"}

        async def send(message):
            if message["type"] == "http.response.body":
                body_sent.set()

        await asyncio.wait_for(
            downstream({"type": "http", "method": "POST"}, receive, send), timeout=2,
        )
        self.assertTrue(body_sent.is_set())
        self.assertTrue(response.is_closed)
        self.assertTrue(producer.done())
        await self.assert_pool_available()

    async def test_stream_cancellation_releases_response_after_first_yield(self):
        response = await self.open_response()
        queue = asyncio.Queue()
        producer = asyncio.create_task(asyncio.sleep(100))
        stream = proxy._stream_generator(
            response, primed_chunks=[b": primed\n\n"], queue=queue, producer_task=producer,
        )
        self.assertEqual(await anext(stream), b": primed\n\n")
        with anyio.CancelScope() as scope:
            scope.cancel()
            await stream.aclose()
        self.assertTrue(response.is_closed)
        self.assertTrue(producer.done())
        await self.assert_pool_available()

    async def test_cancelling_priming_closes_producer_and_response(self):
        response = await self.open_response()
        request = self.request()
        producer_started = asyncio.Event()
        original_producer = proxy._sse_producer

        async def start_producer(*args):
            producer_started.set()
            await original_producer(*args)

        with mock.patch.object(proxy, "_sse_producer", side_effect=start_producer):
            task = asyncio.create_task(proxy._prime_responses_sse(response, request))
            await asyncio.wait_for(producer_started.wait(), timeout=2)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(response.is_closed)
        await self.assert_pool_available()

    def request(self):
        body = json.dumps({"model": "test-model", "input": "synthetic", "stream": True}).encode()
        first = True

        async def receive():
            nonlocal first
            if first:
                first = False
                return {"type": "http.request", "body": body, "more_body": False}
            await asyncio.sleep(100)

        return Request({
            "type": "http", "method": "POST", "path": "/v1/responses",
            "headers": [], "query_string": b"",
        }, receive)

    async def test_handler_finally_closes_response_when_json_body_read_is_cancelled(self):
        response = await self.open_response("/json")
        request = self.request()
        read_started = asyncio.Event()
        original_read = response.aread

        async def read_body():
            read_started.set()
            return await original_read()

        with (
            mock.patch.object(proxy, "get_shared_client", return_value=self.client),
            mock.patch.object(proxy, "_send_with_transport_retry", return_value=response),
            mock.patch.object(response, "aread", side_effect=read_body),
        ):
            task = asyncio.create_task(proxy.handle_responses(request))
            await asyncio.wait_for(read_started.wait(), timeout=2)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(response.is_closed)
        await self.assert_pool_available()

    async def test_normal_inflight_stream_is_not_closed_by_new_request_failure(self):
        response = await self.open_response()
        # No global client reset: an unrelated pool timeout must not kill this stream.
        with self.assertRaises(httpx.PoolTimeout):
            await self.client.get(self.url + "/ok")
        self.assertFalse(response.is_closed)
        await proxy._close_sse_producer(response, None)
        await self.assert_pool_available()

    def test_pool_timeout_description_is_explicit_and_redacts_tokens(self):
        self.assertIn("PoolTimeout", proxy.describe_upstream_error(httpx.PoolTimeout("")))
        self.assertIn("available upstream connection", proxy.describe_upstream_error(httpx.PoolTimeout("")))
        description = proxy.describe_upstream_error(RuntimeError("Bearer fake-token sk-abcdefghijklmno"))
        self.assertNotIn("fake-token", description)
        self.assertNotIn("sk-abcdefghijklmno", description)


if __name__ == "__main__":
    unittest.main()
