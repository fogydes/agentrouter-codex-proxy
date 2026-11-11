"""
proxy.py -- Local OpenAI Responses API compatibility proxy for Codex & AgentRouter.

Purpose
-------
When using Codex with AgentRouter, multi-turn sessions often fail with:
"OpenAI Responses bad request: The requested item was created under a different
**** OpenAI resource. Use the same resource that created the item to access it."

This proxy eliminates this issue by:
  1. Intercepting requests to `/v1/responses` (and `/responses`).
  2. Providing resilient DNS resolution with static fallback IPs (8.214.160.125 / 8.214.161.192)
     so local network DNS drops or rate-limits never cause [Errno 11001] getaddrinfo failed.
  3. Reusing persistent HTTP keep-alive connection pools to minimize connection overhead and
     eliminate redundant DNS queries.
  4. Automatic transport-level retries on transient connection or server disconnect errors.
  5. Attempting the original request transparently first to leverage upstream KV cache.
  6. When an upstream resource mismatch or state error occurs (HTTP 400), activating a
     guaranteed recovery phase with a sanitized stateless payload (stripping previous_response_id,
     server-minted item IDs, and un-decryptable reasoning tokens).
  7. Streaming SSE events (`text/event-stream`) directly to Codex once upstream commits.
"""

from __future__ import annotations

import asyncio
import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
import logging
import os
import random
import re
import socket
import sys
import time
from typing import Any, AsyncIterator, Dict, List, Mapping, Optional, Tuple

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

# ----------------------------------------------------------------------------
# Logging Setup
# ----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("agentrouter-codex-proxy")

# ----------------------------------------------------------------------------
# Resilient DNS Resolver (Fixes [Errno 11001] getaddrinfo failed permanently)
# ----------------------------------------------------------------------------
_ORIG_GETADDRINFO = socket.getaddrinfo

# Pre-seeded fallback IP addresses for upstream hosts
STATIC_DNS_FALLBACKS: Dict[str, List[str]] = {
    "agentrouter.org": ["8.214.160.125", "8.214.161.192"],
    "api.agentrouter.org": ["8.214.160.125", "8.214.161.192"],
}

# Dynamic in-memory DNS cache: updated whenever system DNS succeeds
DYNAMIC_DNS_CACHE: Dict[str, List[tuple]] = {}

def resilient_getaddrinfo(host, port, *args, **kwargs):
    """
    Bulletproof getaddrinfo hook:
    1. Attempts standard OS getaddrinfo first.
    2. On success, updates DYNAMIC_DNS_CACHE.
    3. On failure (socket.gaierror), falls back to DYNAMIC_DNS_CACHE or STATIC_DNS_FALLBACKS.
    """
    if isinstance(host, (bytes, bytearray)):
        host_str = host.decode("utf-8", errors="ignore")
    else:
        host_str = str(host)

    try:
        results = _ORIG_GETADDRINFO(host, port, *args, **kwargs)
        if results and host_str in STATIC_DNS_FALLBACKS:
            DYNAMIC_DNS_CACHE[host_str] = results
        return results
    except socket.gaierror as exc:
        # Check if we have cached results from earlier successful queries
        if host_str in DYNAMIC_DNS_CACHE:
            cached = []
            for item in DYNAMIC_DNS_CACHE[host_str]:
                cached.append((item[0], item[1], item[2], item[3], (item[4][0], port)))
            logger.warning("[DNS Fallback] getaddrinfo failed for %s. Using cached IPs: %s", host_str, [x[4][0] for x in cached])
            return cached

        # Check if we have static fallback IPs
        if host_str in STATIC_DNS_FALLBACKS:
            fallback_ips = STATIC_DNS_FALLBACKS[host_str]
            logger.warning("[DNS Fallback] getaddrinfo failed for %s. Using static fallback IPs: %s", host_str, fallback_ips)
            results = []
            for ip in fallback_ips:
                results.append((socket.AF_INET, socket.SocketKind.SOCK_STREAM, 6, "", (ip, port)))
            return results

        raise

# Install resilient DNS resolver globally
socket.getaddrinfo = resilient_getaddrinfo

# ----------------------------------------------------------------------------
# Resilient TCP Socket Keep-Alive Hook
# Proactively prevents intermediate load balancers (Alibaba ALB) and local NAT
# from closing idle TCP sockets during long reasoning / thinking pauses.
# ----------------------------------------------------------------------------
_ORIG_SOCKET_INIT = socket.socket.__init__

def resilient_socket_init(self, *args, **kwargs):
    _ORIG_SOCKET_INIT(self, *args, **kwargs)
    try:
        if self.family in (socket.AF_INET, socket.AF_INET6) and self.type == socket.SOCK_STREAM:
            self.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if hasattr(socket, "SIO_KEEPALIVE_VALS"):
                # Windows: probe after 15s idle, then every 2s
                self.ioctl(socket.SIO_KEEPALIVE_VALS, (1, 15000, 2000))
            else:
                if hasattr(socket, "TCP_KEEPIDLE"):
                    self.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 15)
                if hasattr(socket, "TCP_KEEPINTVL"):
                    self.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 2)
                if hasattr(socket, "TCP_KEEPCNT"):
                    self.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
    except Exception:
        pass

socket.socket.__init__ = resilient_socket_init

# ----------------------------------------------------------------------------
# Windows Proactor Socket Teardown Guard
# CPython 3.12's _ProactorBasePipeTransport._call_connection_lost calls
# self._sock.shutdown(SHUT_RDWR) without guarding OSError, causing benign
# WinError 10054 tracebacks and skipping close/detach when peers reset early.
# ----------------------------------------------------------------------------
def _install_proactor_teardown_guard() -> None:
    """Guard Windows Proactor socket teardown against OSError during shutdown()."""
    if sys.platform != "win32":
        return
    try:
        import asyncio.proactor_events as _proactor_events
    except ImportError:
        return

    transport_cls = getattr(_proactor_events, "_ProactorBasePipeTransport", None)
    if transport_cls is None or getattr(transport_cls, "_agentrouter_teardown_guarded", False):
        return

    orig_call_connection_lost = transport_cls._call_connection_lost

    def _safe_call_connection_lost(self, exc):
        if getattr(self, "_called_connection_lost", False):
            return
        try:
            orig_call_connection_lost(self, exc)
        except OSError:
            if getattr(self, "_called_connection_lost", False):
                raise
            sock = getattr(self, "_sock", None)
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
            self._sock = None
            server = getattr(self, "_server", None)
            if server is not None:
                server._detach()
                self._server = None
            self._called_connection_lost = True

    transport_cls._call_connection_lost = _safe_call_connection_lost
    transport_cls._agentrouter_teardown_guarded = True

_install_proactor_teardown_guard()

# ----------------------------------------------------------------------------
# Environment & Configuration
# ----------------------------------------------------------------------------
def _load_env():
    env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_file):
        with open(env_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip("'\"")
                    if k not in os.environ:
                        os.environ[k] = v

_load_env()

UPSTREAM = os.environ.get("AGENTROUTER_UPSTREAM", "https://agentrouter.org").rstrip("/")
LISTEN_HOST = "127.0.0.1"
PORT = int(os.environ.get("PROXY_PORT", "8788"))
RETRY_DELAY_MS = int(os.environ.get("PROXY_RETRY_DELAY_MS", "300"))
VERBOSE = os.environ.get("PROXY_VERBOSE", "1") not in ("0", "", "false", "False")

# Rate-limit / SSE priming configuration
RATE_LIMIT_BASE_DELAY_S = float(os.environ.get("PROXY_RATE_LIMIT_BASE_S", "2.0"))
RATE_LIMIT_MAX_DELAY_S = float(os.environ.get("PROXY_RATE_LIMIT_MAX_DELAY_S", "15.0"))
MAX_RATE_LIMIT_WAIT_S = float(os.environ.get("PROXY_MAX_RATE_LIMIT_WAIT_S", "45.0"))
MAX_TRANSIENT_RETRIES = int(os.environ.get("PROXY_MAX_TRANSIENT_RETRIES", "6"))
RECOVERY_STAGGER_S = float(os.environ.get("PROXY_RECOVERY_STAGGER_S", "0.6"))
SSE_PRIME_FIRST_TIMEOUT_S = float(os.environ.get("PROXY_SSE_PRIME_FIRST_TIMEOUT_S", "45.0"))
SSE_PRIME_CREATED_GRACE_S = float(os.environ.get("PROXY_SSE_PRIME_CREATED_GRACE_S", "3.0"))

CONNECT_TIMEOUT_S = float(os.environ.get("PROXY_CONNECT_TIMEOUT_S", "20.0"))
READ_TIMEOUT_S = float(os.environ.get("PROXY_READ_TIMEOUT_S", "600.0"))

CLIENT_TIMEOUT = httpx.Timeout(
    timeout=READ_TIMEOUT_S,
    connect=CONNECT_TIMEOUT_S,
    read=READ_TIMEOUT_S,
    write=60.0,
    pool=30.0,
)

# Match patterns for cross-resource Azure OpenAI error & invalid state references
RESOURCE_MISMATCH_PATTERNS = [
    re.compile(r"created under a different.*OpenAI resource", re.IGNORECASE),
    re.compile(r"Use the same resource that created the item", re.IGNORECASE),
    re.compile(r"requested item was created under a different", re.IGNORECASE),
    re.compile(r"previous_response_id", re.IGNORECASE),
    re.compile(r"Response.*not found", re.IGNORECASE),
    re.compile(r"No response found", re.IGNORECASE),
    re.compile(r"No item found", re.IGNORECASE),
    re.compile(r"item.*not found", re.IGNORECASE),
    re.compile(r"Invalid.*\.id", re.IGNORECASE),
    re.compile(r"Exceeded maximum number of images", re.IGNORECASE),
    re.compile(r"maximum number of images", re.IGNORECASE),
    re.compile(r"too many images", re.IGNORECASE),
]

# Hop-by-hop headers to strip
HOP_BY_HOP = frozenset({
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
    "accept-encoding",
})

STRIP_RESPONSE_HEADERS = HOP_BY_HOP | {"content-encoding", "date"}

RATE_LIMIT_CODES = frozenset({
    "rate_limit_exceeded",
    "tokens_limit_exceeded",
    "requests_limit_exceeded",
    "too_many_requests",
    "token_rate_limit_exceeded",
})

TRANSIENT_ERROR_CODES = RATE_LIMIT_CODES | frozenset({
    "server_error",
    "overloaded",
    "service_unavailable",
    "upstream_error",
    "timeout",
    "internal_error",
})

RATE_LIMIT_TEXT_RE = re.compile(
    r"rate[_\s-]*limit|exceeded.*token|tokens?\s+per\s+min|too many requests|quota.*exceeded",
    re.IGNORECASE,
)

TRANSIENT_CAPACITY_TEXT_RE = re.compile(
    r"overloaded|temporarily unavailable|capacity|high demand|try again in|server had an error|bad gateway|gateway timeout",
    re.IGNORECASE,
)

RETRY_AFTER_TEXT_RE = re.compile(
    r"(?:retry\s+after|try\s+again\s+in|wait)\s+(\d+(?:\.\d+)?)\s*(ms|milliseconds?|s|sec|seconds?)",
    re.IGNORECASE,
)

RESET_DURATION_RE = re.compile(r"(?:(\d+)m)?(\d+(?:\.\d+)?)s|(\d+(?:\.\d+)?)ms", re.IGNORECASE)
SSE_DELIM_RE = re.compile(rb"(?:\r\n\r\n|\n\n|\r\r)")
SSE_SETUP_EVENTS = frozenset({"response.created", "response.queued", "response.in_progress", "ping", "keepalive"})

# ----------------------------------------------------------------------------
# Image Capping Configuration & Utilities
# OpenAI Responses API enforces a hard limit of 50 images per request.
# In long sessions with browser/computer use, screenshots accumulate and break sessions.
# ----------------------------------------------------------------------------
MAX_IMAGES_PER_REQUEST = int(os.environ.get("PROXY_MAX_IMAGES", "35"))

def is_image_part(part: Any) -> bool:
    if not isinstance(part, dict):
        return False
    t = str(part.get("type", "")).lower()
    return t in ("input_image", "computer_screenshot", "image_url") or "image_url" in part

def prune_excess_images(payload: dict, max_images: int = MAX_IMAGES_PER_REQUEST) -> tuple[dict, int]:
    """
    Finds all image parts across messages, tool calls, and tool outputs.
    If total images > max_images, replaces the oldest images with compact text placeholders,
    preserving the latest max_images and valid schema.
    """
    clean_data = copy.deepcopy(payload)
    raw_input = clean_data.get("input")
    if not isinstance(raw_input, list):
        return clean_data, 0

    image_refs = []  # list of (kind, item_idx, field, part_idx)
    for item_idx, item in enumerate(raw_input):
        if not isinstance(item, dict):
            continue

        # Check top-level item
        t = str(item.get("type", "")).lower()
        if t in ("input_image", "computer_screenshot"):
            image_refs.append(("top", item_idx, None, None))
            continue

        # Check content and output fields
        for field in ("content", "output"):
            parts = item.get(field)
            if isinstance(parts, list):
                for part_idx, part in enumerate(parts):
                    if is_image_part(part):
                        image_refs.append(("part", item_idx, field, part_idx))

    total_images = len(image_refs)
    if total_images <= max_images:
        return clean_data, 0

    to_prune = total_images - max_images
    for i in range(to_prune):
        ref = image_refs[i]
        kind = ref[0]
        item_idx = ref[1]
        if kind == "top":
            clean_data["input"][item_idx] = {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "[Historical screenshot omitted: turn exceeded 50-image limit]"}]
            }
        else:
            field = ref[2]
            part_idx = ref[3]
            clean_data["input"][item_idx][field][part_idx] = {
                "type": "input_text",
                "text": "[Historical screenshot omitted: turn exceeded 50-image limit]"
            }

    return clean_data, to_prune

# ----------------------------------------------------------------------------
# Runtime Stats
# ----------------------------------------------------------------------------
STATS = {
    "requests_total": 0,
    "responses_requests": 0,
    "resource_mismatches_caught": 0,
    "stateless_fallbacks_applied": 0,
    "successful_recoveries": 0,
    "transport_retries": 0,
    "rate_limits_caught": 0,
    "stream_precommit_retries": 0,
    "rate_limit_wait_seconds_total": 0.0,
    "images_pruned_total": 0,
    "start_time": time.time(),
}

# ----------------------------------------------------------------------------
# Shared Rate-Limit Cooldown & Recovery Stagger (Anti-Stampede)
# When concurrent subagents share AgentRouter's upstream TPM pool, a 429 or SSE
# rate_limit_exceeded pauses and staggers launches across all active callers.
# ----------------------------------------------------------------------------
_RATE_LIMIT_STATE: Dict[str, Any] = {
    "cooldown_until": 0.0,
    "streak": 0,
    "last_limit_at": 0.0,
    "next_launch_after": 0.0,
}
_LAUNCH_LOCKS: Dict[int, asyncio.Lock] = {}

def _get_launch_lock() -> asyncio.Lock:
    loop_id = id(asyncio.get_running_loop())
    lock = _LAUNCH_LOCKS.get(loop_id)
    if lock is None:
        lock = asyncio.Lock()
        _LAUNCH_LOCKS[loop_id] = lock
    return lock

def reset_rate_limit_state() -> None:
    """Resets in-memory rate-limit cooldown state (primarily for tests)."""
    _RATE_LIMIT_STATE["cooldown_until"] = 0.0
    _RATE_LIMIT_STATE["streak"] = 0
    _RATE_LIMIT_STATE["last_limit_at"] = 0.0
    _RATE_LIMIT_STATE["next_launch_after"] = 0.0

def _parse_reset_duration(raw: str) -> Optional[float]:
    m = RESET_DURATION_RE.search(raw.strip())
    if not m:
        return None
    if m.group(3) is not None:
        return float(m.group(3)) / 1000.0
    mins = float(m.group(1) or 0.0)
    secs = float(m.group(2) or 0.0)
    return mins * 60.0 + secs

def _extract_retry_delay_s(
    headers: Optional[Mapping[str, str]],
    text: str,
    streak: int,
) -> Tuple[float, Optional[int]]:
    """Returns (sleep_delay_with_jitter_s, parsed_hint_seconds_or_none)."""
    hint_s: Optional[float] = None
    if headers:
        h = {k.lower(): v for k, v in headers.items()}
        if "retry-after-ms" in h:
            try:
                hint_s = float(h["retry-after-ms"].strip()) / 1000.0
            except ValueError:
                pass
        if hint_s is None and "retry-after" in h:
            raw_ra = h["retry-after"].strip()
            try:
                hint_s = float(raw_ra)
            except ValueError:
                try:
                    dt = parsedate_to_datetime(raw_ra)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    hint_s = max(0.5, (dt - datetime.now(timezone.utc)).total_seconds())
                except Exception:
                    pass
        if hint_s is None:
            for hdr_name in ("x-ratelimit-reset-tokens", "x-ratelimit-reset-requests"):
                if hdr_name in h:
                    parsed = _parse_reset_duration(h[hdr_name])
                    if parsed is not None and parsed > 0:
                        hint_s = parsed
                        break

    if hint_s is None and text:
        m = RETRY_AFTER_TEXT_RE.search(text)
        if m:
            val = float(m.group(1))
            unit = m.group(2).lower()
            hint_s = (val / 1000.0) if unit.startswith("ms") else val

    if hint_s is not None:
        delay = min(max(0.5, hint_s) + random.uniform(0.15, 0.65), RATE_LIMIT_MAX_DELAY_S)
        return delay, max(1, min(int(round(hint_s)), int(RATE_LIMIT_MAX_DELAY_S)))

    exp = RATE_LIMIT_BASE_DELAY_S * (2 ** min(max(streak - 1, 0), 3))
    delay = min(exp + random.uniform(0.2, 0.8), RATE_LIMIT_MAX_DELAY_S)
    return delay, None

def _record_rate_limit_cooldown(delay_s: float) -> int:
    now = time.monotonic()
    new_streak = min(int(_RATE_LIMIT_STATE["streak"]) + 1, 4)
    _RATE_LIMIT_STATE["streak"] = new_streak
    _RATE_LIMIT_STATE["last_limit_at"] = now
    _RATE_LIMIT_STATE["cooldown_until"] = max(float(_RATE_LIMIT_STATE["cooldown_until"]), now + delay_s)
    return new_streak

def _clear_rate_limit_cooldown() -> None:
    _RATE_LIMIT_STATE["streak"] = 0
    _RATE_LIMIT_STATE["cooldown_until"] = 0.0

async def _client_disconnected(request: Request) -> bool:
    try:
        return await request.is_disconnected()
    except Exception:
        return False

async def _sleep_unless_disconnected(delay_s: float, request: Request) -> bool:
    """Sleeps up to delay_s in small slices; returns False if client disconnected."""
    if delay_s <= 0:
        return True
    end = time.monotonic() + delay_s
    STATS["rate_limit_wait_seconds_total"] = round(
        float(STATS["rate_limit_wait_seconds_total"]) + delay_s, 2
    )
    while True:
        if await _client_disconnected(request):
            return False
        rem = end - time.monotonic()
        if rem <= 0:
            return True
        await asyncio.sleep(min(rem, 0.25))

async def _wait_for_rate_limit_gate(request: Request) -> bool:
    """Honors active shared rate-limit cooldown and staggers post-limit bursts."""
    while True:
        if await _client_disconnected(request):
            return False
        now = time.monotonic()
        cooldown_until = float(_RATE_LIMIT_STATE["cooldown_until"])
        if now < cooldown_until:
            await asyncio.sleep(min(cooldown_until - now, 0.25))
            continue

        recently_limited = (
            _RATE_LIMIT_STATE["streak"] > 0
            or (now - float(_RATE_LIMIT_STATE["last_limit_at"])) < 10.0
        )
        if not recently_limited or RECOVERY_STAGGER_S <= 0:
            return True

        lock = _get_launch_lock()
        async with lock:
            now = time.monotonic()
            if now < float(_RATE_LIMIT_STATE["cooldown_until"]):
                continue
            slot = float(_RATE_LIMIT_STATE["next_launch_after"])
            if now < slot:
                wait_for_slot = slot - now
                _RATE_LIMIT_STATE["next_launch_after"] = slot + RECOVERY_STAGGER_S
            else:
                wait_for_slot = 0.0
                _RATE_LIMIT_STATE["next_launch_after"] = now + RECOVERY_STAGGER_S

        if wait_for_slot > 0:
            if not await _sleep_unless_disconnected(wait_for_slot, request):
                return False
        return True

# ----------------------------------------------------------------------------
# Shared Connection Pool
# ----------------------------------------------------------------------------
_SHARED_CLIENT: Optional[httpx.AsyncClient] = None

def get_shared_client() -> httpx.AsyncClient:
    """Returns a shared persistent AsyncClient with connection pooling."""
    global _SHARED_CLIENT
    if _SHARED_CLIENT is None or _SHARED_CLIENT.is_closed:
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=50, keepalive_expiry=600.0)
        _SHARED_CLIENT = httpx.AsyncClient(timeout=CLIENT_TIMEOUT, limits=limits, follow_redirects=False)
    return _SHARED_CLIENT

# ----------------------------------------------------------------------------
# Payload Sanitization (The Bulletproof Fix)
# ----------------------------------------------------------------------------
def is_resource_mismatch_text(text: str) -> bool:
    """Checks if error text matches cross-resource item mismatch or image limit patterns."""
    return any(pat.search(text) for pat in RESOURCE_MISMATCH_PATTERNS)

def is_resource_mismatch_error(status_code: int, body_text: str) -> bool:
    """Checks if an HTTP error response is due to cross-resource item mismatch or image limit."""
    return status_code == 400 and is_resource_mismatch_text(body_text)

def _is_rate_limit_error(code: str, message: str) -> bool:
    c = (code or "").strip().lower()
    return c in RATE_LIMIT_CODES or bool(message and RATE_LIMIT_TEXT_RE.search(message))

def _is_transient_error(code: str, message: str) -> bool:
    c = (code or "").strip().lower()
    return c in TRANSIENT_ERROR_CODES or bool(
        message and (RATE_LIMIT_TEXT_RE.search(message) or TRANSIENT_CAPACITY_TEXT_RE.search(message))
    )

def sanitize_responses_payload(payload: dict) -> dict:
    """
    Transforms a stateful Responses API payload into a self-contained, stateless
    payload that ANY upstream Azure OpenAI / OpenAI resource can process.
    Also prunes accumulated images to safely fit under the 50-image ceiling.
    """
    clean_data = copy.deepcopy(payload)

    # 1. Remove all server-side session pointers
    for state_key in ("previous_response_id", "response_id", "parent_id", "parent_response_id"):
        if state_key in clean_data:
            logger.info("[Sanitizer] Stripped '%s' (%s)", state_key, clean_data[state_key])
            del clean_data[state_key]

    # 2. Sanitize input items
    raw_input = clean_data.get("input")
    if isinstance(raw_input, list):
        sanitized_input = []
        for item in raw_input:
            if not isinstance(item, dict):
                sanitized_input.append(item)
                continue

            # Drop reasoning blocks and item references completely.
            # Past turn reasoning (CoT) is server-minted, contains encrypted tokens,
            # and is rejected as stateless input by foreign Azure resources.
            t = item.get("type")
            if t in ("reasoning", "item_reference"):
                logger.info("[Sanitizer] Dropped server-state item of type '%s'", t)
                continue

            cleaned_item = dict(item)

            # Strip server-minted item ID so the receiving resource mints its own fresh ID
            # Note: call_id is preserved for tool call / tool output correlation
            cleaned_item.pop("id", None)
            cleaned_item.pop("status", None)
            cleaned_item.pop("internal_chat_message_metadata_passthrough", None)

            # Clean any encrypted_content inside content and output parts
            for field in ("content", "output"):
                parts = cleaned_item.get(field)
                if isinstance(parts, list):
                    new_parts = []
                    for part in parts:
                        if isinstance(part, dict):
                            if part.get("type") == "encrypted_content" or "encrypted_content" in part:
                                continue
                            if part.get("type") == "summary_text":
                                continue
                            new_parts.append(part)
                        else:
                            new_parts.append(part)
                    cleaned_item[field] = new_parts

            sanitized_input.append(cleaned_item)

        clean_data["input"] = sanitized_input

    # 3. Prune historical images to well below the 50-image hard limit
    clean_data, pruned_count = prune_excess_images(clean_data, max_images=MAX_IMAGES_PER_REQUEST)
    if pruned_count > 0:
        STATS["images_pruned_total"] += pruned_count
        logger.warning(
            "[Sanitizer] Pruned %d historical images (capped at %d) to guarantee <= 50 images",
            pruned_count, MAX_IMAGES_PER_REQUEST
        )

    return clean_data

# ----------------------------------------------------------------------------
# Transport Helper with Automatic Network Reconnects
# ----------------------------------------------------------------------------
async def _send_with_transport_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: Dict[str, str],
    content: Optional[bytes] = None,
    params: Optional[dict] = None,
    stream: bool = False,
    max_retries: int = 5,
) -> httpx.Response:
    """
    Sends an HTTP request with dedicated retries on connection, DNS, and transport errors.
    This guarantees that momentary WiFi drops, DNS timeouts, or server disconnects
    do not kill the session or exhaust application recovery attempts.
    """
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            req = client.build_request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                content=content,
            )
            return await client.send(req, stream=stream)
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.RemoteProtocolError, httpx.ReadTimeout) as exc:
            last_exc = exc
            STATS["transport_retries"] += 1
            backoff_s = min(attempt * 0.75, 4.0)
            logger.warning(
                "[Transport Retry %d/%d] %s: %s. Reconnecting in %.1fs...",
                attempt, max_retries, type(exc).__name__, str(exc), backoff_s
            )
            if attempt < max_retries:
                await asyncio.sleep(backoff_s)
            else:
                raise
        except Exception as exc:
            last_exc = exc
            STATS["transport_retries"] += 1
            backoff_s = min(attempt * 0.75, 4.0)
            logger.error("[Transport Error %d/%d] %s. Reconnecting in %.1fs...", attempt, max_retries, str(exc), backoff_s)
            if attempt < max_retries:
                await asyncio.sleep(backoff_s)
            else:
                raise
    raise last_exc

# ----------------------------------------------------------------------------
# FastAPI Application
# ----------------------------------------------------------------------------
app = FastAPI(
    title="agentrouter-codex-proxy",
    description="Local compatibility proxy fixing OpenAI Responses cross-resource errors for Codex",
    version="1.5.0",
)

@app.on_event("shutdown")
async def shutdown_event():
    global _SHARED_CLIENT
    if _SHARED_CLIENT and not _SHARED_CLIENT.is_closed:
        await _SHARED_CLIENT.aclose()

def _filter_headers(headers: Dict[str, str]) -> Dict[str, str]:
    """Filters client headers for proxy forwarding and injects fallback auth if needed."""
    out = {}
    has_auth = False
    for k, v in headers.items():
        k_lower = k.lower()
        if k_lower == "authorization":
            has_auth = True
        if k_lower not in HOP_BY_HOP:
            out[k] = v

    if not has_auth:
        fallback_key = os.environ.get("CODEX_GATEWAY_API_KEY") or os.environ.get("AGENTROUTER_API_KEY")
        if fallback_key:
            out["authorization"] = f"Bearer {fallback_key}"

    # Ensure a compatible User-Agent for AgentRouter if missing or generic
    ua = out.get("user-agent") or out.get("User-Agent")
    if not ua or "python" in ua.lower() or "curl" in ua.lower():
        out["user-agent"] = "Codex Desktop"

    return out

@app.get("/_health")
async def health():
    uptime_s = round(time.time() - STATS["start_time"], 1)
    return {
        "status": "ok",
        "proxy": "agentrouter-codex-proxy",
        "version": "1.5.0",
        "dns_fallback_active": True,
        "max_images_per_request": MAX_IMAGES_PER_REQUEST,
        "listen": f"{LISTEN_HOST}:{PORT}",
        "uptime_seconds": uptime_s,
        "upstream": UPSTREAM,
        "stats": STATS,
    }

@app.get("/_stats")
async def stats():
    return STATS

@dataclass
class SSEPrimeResult:
    """Result of pre-commit inspection on an HTTP 200 text/event-stream response."""

    status: str  # "commit", "retry_rate_limit", "retry_transient", "resource_mismatch", "client_disconnected"
    primed_chunks: List[bytes] = field(default_factory=list)
    remainder: bytes = b""
    upstream_response: Optional[httpx.Response] = None
    queue: Optional[asyncio.Queue] = None
    producer_task: Optional[asyncio.Task] = None
    producer_exc: List[Exception] = field(default_factory=list)
    error_code: str = ""
    error_message: str = ""
    error_payload: Optional[Dict[str, Any]] = None


async def _sse_producer(
    upstream_response: httpx.Response,
    queue: asyncio.Queue,
    producer_exc: List[Exception],
) -> None:
    try:
        async for chunk in upstream_response.aiter_bytes():
            await queue.put(chunk)
        await queue.put(None)
    except Exception as exc:
        producer_exc.append(exc)
        await queue.put(None)


async def _close_sse_producer(
    upstream_response: Optional[httpx.Response],
    producer_task: Optional[asyncio.Task],
) -> None:
    if producer_task is not None and not producer_task.done():
        producer_task.cancel()
        await asyncio.gather(producer_task, return_exceptions=True)
    if upstream_response is not None:
        try:
            await upstream_response.aclose()
        except Exception:
            pass


def _parse_sse_frame(raw_frame: bytes) -> Tuple[str, Optional[Dict[str, Any]], str]:
    text = raw_frame.decode("utf-8", errors="replace")
    event_name = ""
    data_lines: List[str] = []
    has_non_comment = False

    for raw_line in text.splitlines():
        line = raw_line.rstrip("\r")
        if not line or line.startswith(":"):
            continue
        has_non_comment = True
        if line.startswith("event:"):
            event_name = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())

    if not has_non_comment:
        return "", None, ""

    data_str = "\n".join(data_lines).strip()
    if not data_str or data_str == "[DONE]":
        return event_name, None, data_str

    parsed: Optional[Dict[str, Any]] = None
    try:
        obj = json.loads(data_str)
        if isinstance(obj, dict):
            parsed = obj
    except Exception:
        pass

    if not event_name and parsed is not None:
        t = parsed.get("type")
        if isinstance(t, str):
            event_name = t

    return event_name, parsed, data_str


def _extract_error_fields(
    obj: Optional[Dict[str, Any]],
    fallback_text: str = "",
) -> Tuple[str, str, Dict[str, Any]]:
    err_dict: Optional[Dict[str, Any]] = None
    if isinstance(obj, dict):
        if isinstance(obj.get("error"), dict):
            err_dict = obj["error"]
        elif isinstance(obj.get("error"), str):
            err_dict = {"message": obj["error"]}
        elif isinstance(obj.get("response"), dict) and isinstance(obj["response"].get("error"), dict):
            err_dict = obj["response"]["error"]
        elif obj.get("type") == "error" or obj.get("code") or obj.get("message"):
            err_dict = obj

    if err_dict is not None:
        code = str(err_dict.get("code") or err_dict.get("type") or "")
        msg = str(err_dict.get("message") or err_dict.get("detail") or fallback_text or "")
        return code, msg, err_dict

    return "", fallback_text, {"message": fallback_text}


def _classify_sse_frame(raw_frame: bytes) -> Tuple[str, str, str, Optional[Dict[str, Any]]]:
    """Classifies one complete SSE frame into ignore, setup, error, or commit."""
    event_name, parsed, data_str = _parse_sse_frame(raw_frame)
    if not event_name and parsed is None and not data_str:
        return "ignore", "", "", None

    ev_lower = event_name.lower()
    embedded_failed = isinstance(parsed, dict) and (
        bool(parsed.get("error"))
        or (
            isinstance(parsed.get("response"), dict)
            and (parsed["response"].get("status") == "failed" or bool(parsed["response"].get("error")))
        )
    )

    if ev_lower in ("error", "response.failed") or embedded_failed:
        code, msg, err_obj = _extract_error_fields(parsed, data_str)
        return "error", code, msg, err_obj

    if ev_lower in SSE_SETUP_EVENTS:
        return "setup", "", "", None

    if data_str == "[DONE]" and not ev_lower:
        return "ignore", "", "", None

    return "commit", "", "", None


async def _prime_responses_sse(
    upstream_response: httpx.Response,
    request: Request,
    *,
    first_frame_timeout_s: float = SSE_PRIME_FIRST_TIMEOUT_S,
    created_grace_s: float = SSE_PRIME_CREATED_GRACE_S,
) -> SSEPrimeResult:
    """
    Buffers initial SSE frames before committing HTTP 200 headers to Codex.
    Catches early HTTP-200 rate_limit_exceeded / response.failed / empty streams
    so the proxy can retry silently without tripping Codex reconnect banners.
    """
    queue: asyncio.Queue[Optional[bytes]] = asyncio.Queue()
    producer_exc: List[Exception] = []
    producer_task = asyncio.create_task(_sse_producer(upstream_response, queue, producer_exc))

    buf = bytearray()
    primed_chunks: List[bytes] = []
    saw_setup = False
    overall_deadline = time.monotonic() + first_frame_timeout_s
    grace_deadline: Optional[float] = None

    def commit_result(remainder: bytes) -> SSEPrimeResult:
        return SSEPrimeResult(
            status="commit",
            primed_chunks=primed_chunks,
            remainder=remainder,
            upstream_response=upstream_response,
            queue=queue,
            producer_task=producer_task,
            producer_exc=producer_exc,
        )

    async def abort_result(status: str, code: str = "", msg: str = "", payload: Optional[Dict[str, Any]] = None) -> SSEPrimeResult:
        await _close_sse_producer(upstream_response, producer_task)
        return SSEPrimeResult(status=status, error_code=code, error_message=msg, error_payload=payload)

    async def handle_frame(raw_frame: bytes, trailing_after: bytes) -> Optional[SSEPrimeResult]:
        nonlocal saw_setup, grace_deadline
        kind, err_code, err_msg, err_obj = _classify_sse_frame(raw_frame)
        if kind == "ignore":
            primed_chunks.append(raw_frame)
            return None
        if kind == "setup":
            primed_chunks.append(raw_frame)
            if not saw_setup:
                saw_setup = True
                grace_deadline = time.monotonic() + created_grace_s
            return None
        if kind == "error":
            primed_chunks.append(raw_frame)
            if is_resource_mismatch_text(err_msg):
                return await abort_result("resource_mismatch", err_code, err_msg, err_obj)
            if _is_rate_limit_error(err_code, err_msg):
                return await abort_result("retry_rate_limit", err_code, err_msg, err_obj)
            if _is_transient_error(err_code, err_msg):
                return await abort_result("retry_transient", err_code, err_msg, err_obj)
            # Non-retryable upstream SSE rejection: commit & forward verbatim.
            return commit_result(trailing_after)

        primed_chunks.append(raw_frame)
        return commit_result(trailing_after)

    try:
        while True:
            if await _client_disconnected(request):
                return await abort_result("client_disconnected")

            now = time.monotonic()
            active_deadline = grace_deadline if grace_deadline is not None else overall_deadline
            remaining = active_deadline - now
            if remaining <= 0:
                if grace_deadline is not None and saw_setup:
                    return commit_result(bytes(buf))
                return await abort_result("retry_transient", "timeout", "SSE prime timeout waiting for upstream event")

            try:
                chunk = await asyncio.wait_for(queue.get(), timeout=min(remaining, 0.35))
            except asyncio.TimeoutError:
                continue

            if chunk is None:
                if buf:
                    res = await handle_frame(bytes(buf), b"")
                    if res is not None:
                        if res.status == "commit":
                            await queue.put(None)
                        return res
                err_note = str(producer_exc[0]) if producer_exc else "Upstream SSE stream closed before emitting content"
                return await abort_result("retry_transient", "empty_stream", err_note)

            buf.extend(chunk)
            pos = 0
            for m in SSE_DELIM_RE.finditer(buf):
                raw_frame = bytes(buf[pos:m.end()])
                pos = m.end()
                res = await handle_frame(raw_frame, bytes(buf[pos:]))
                if res is not None:
                    return res
            if pos > 0:
                del buf[:pos]
    except asyncio.CancelledError:
        await _close_sse_producer(upstream_response, producer_task)
        raise
    except Exception as exc:
        return await abort_result("retry_transient", type(exc).__name__, str(exc))


async def _stream_generator(
    upstream_response: httpx.Response,
    *,
    primed_chunks: Optional[List[bytes]] = None,
    remainder: bytes = b"",
    queue: Optional[asyncio.Queue] = None,
    producer_task: Optional[asyncio.Task] = None,
    producer_exc: Optional[List[Exception]] = None,
    ping_interval: float = 15.0,
) -> AsyncIterator[bytes]:
    """
    Streams SSE chunks from upstream back to Codex with periodic keep-alive heartbeat comments.
    Replays any pre-commit primed frames verbatim before continuing live chunks.
    """
    if queue is None or producer_task is None:
        queue = asyncio.Queue()
        producer_exc = []
        producer_task = asyncio.create_task(_sse_producer(upstream_response, queue, producer_exc))

    last_event_seen = False

    try:
        for pre in primed_chunks or ():
            if pre:
                if b"response.completed" in pre:
                    last_event_seen = True
                yield pre
        if remainder:
            if b"response.completed" in remainder:
                last_event_seen = True
            yield remainder

        while True:
            try:
                chunk = await asyncio.wait_for(queue.get(), timeout=ping_interval)
                if chunk is None:
                    break
                if b"response.completed" in chunk:
                    last_event_seen = True
                yield chunk
            except asyncio.TimeoutError:
                yield b": ping\n\n"
    except Exception as exc:
        logger.warning("[Stream Exception] Upstream SSE connection interrupted: %s", str(exc))
    finally:
        await _close_sse_producer(upstream_response, producer_task)
        if not last_event_seen:
            logger.info("[Stream Finished] Upstream stream ended.")


def _clean_response_headers(headers: Mapping[str, str]) -> Dict[str, str]:
    return {k: v for k, v in headers.items() if k.lower() not in STRIP_RESPONSE_HEADERS}


def _rate_limit_error_response(
    err_payload: Optional[Dict[str, Any]],
    message: str,
    code: str,
    retry_after_s: int,
    headers: Optional[Dict[str, str]] = None,
) -> JSONResponse:
    out_headers = dict(headers or {})
    out_headers.pop("content-type", None)
    if not any(k.lower() == "retry-after" for k in out_headers):
        out_headers["retry-after"] = str(max(1, retry_after_s))
    body_err = dict(err_payload) if isinstance(err_payload, dict) else {"message": message or "Upstream rate limit exceeded"}
    if code and "code" not in body_err:
        body_err["code"] = code
    if "type" not in body_err:
        body_err["type"] = "rate_limit_error"
    return JSONResponse(status_code=429, content={"error": body_err}, headers=out_headers)


@app.post("/v1/responses")
@app.post("/responses")
async def handle_responses(request: Request):
    """
    Main handler for Codex /v1/responses.
    Performs pre-commit SSE priming, staggered rate-limit recovery, and stateless
    sanitization fallback on cross-resource mismatches.
    """
    STATS["requests_total"] += 1
    STATS["responses_requests"] += 1

    body_bytes = await request.body()
    try:
        body_json = json.loads(body_bytes.decode("utf-8"))
    except Exception:
        body_json = None

    headers = _filter_headers(dict(request.headers))
    upstream_url = f"{UPSTREAM}/v1/responses"

    active_payload = body_json
    fallback_active = False

    # Preemptive check: prune excess historical images before sending upstream.
    if active_payload and isinstance(active_payload.get("input"), list):
        active_payload, pre_pruned = prune_excess_images(active_payload, max_images=MAX_IMAGES_PER_REQUEST)
        if pre_pruned > 0:
            STATS["images_pruned_total"] += pre_pruned
            logger.warning(
                "[Preemptive Image Pruning] Pruned %d historical images (kept %d latest) to prevent 50-image ceiling rejection",
                pre_pruned, MAX_IMAGES_PER_REQUEST
            )

    retry_start = time.monotonic()
    transient_retries = 0
    mismatch_attempts = 0
    max_mismatch_retries = 3
    last_failure_response: Optional[Response] = None

    def activate_stateless_fallback(source_label: str, detail: str) -> bool:
        nonlocal fallback_active, active_payload, mismatch_attempts
        STATS["resource_mismatches_caught"] += 1
        logger.warning("[Resource Mismatch] %s: %s", source_label, detail[:160])
        if not fallback_active and body_json is not None:
            fallback_active = True
            STATS["stateless_fallbacks_applied"] += 1
            active_payload = sanitize_responses_payload(
                active_payload if active_payload is not None else body_json
            )
            logger.warning("[Recovery] Activating sanitized stateless payload to bypass cross-resource pinning!")
            return True
        mismatch_attempts += 1
        return mismatch_attempts <= max_mismatch_retries

    async def schedule_transient_wait(
        is_rate_limit: bool,
        resp_hdrs: Mapping[str, str],
        err_msg: str,
        label: str,
    ) -> Tuple[bool, int]:
        nonlocal transient_retries
        if is_rate_limit:
            STATS["rate_limits_caught"] += 1
            streak = min(int(_RATE_LIMIT_STATE["streak"]) + 1, 4)
            delay_s, hint_s = _extract_retry_delay_s(resp_hdrs, err_msg, streak)
            suggested_ra = hint_s if hint_s is not None else max(1, min(int(round(delay_s)), int(RATE_LIMIT_MAX_DELAY_S)))
        else:
            delay_s = min(1.0 * (2 ** min(transient_retries, 3)) + random.uniform(0.1, 0.4), RATE_LIMIT_MAX_DELAY_S)
            suggested_ra = max(1, int(round(delay_s)))

        remaining_budget = MAX_RATE_LIMIT_WAIT_S - (time.monotonic() - retry_start)
        if transient_retries >= MAX_TRANSIENT_RETRIES or remaining_budget <= 0.25:
            return False, suggested_ra

        wait_s = min(delay_s, remaining_budget)
        if is_rate_limit:
            _record_rate_limit_cooldown(wait_s)
        transient_retries += 1
        logger.warning(
            "[%s] Retrying (%d/%d) after %.1fs: %s",
            label, transient_retries, MAX_TRANSIENT_RETRIES, wait_s, (err_msg or "")[:140]
        )
        ok = await _sleep_unless_disconnected(wait_s, request)
        return ok, suggested_ra

    while True:
        if not await _wait_for_rate_limit_gate(request):
            return Response(status_code=499)

        req_content = json.dumps(active_payload).encode("utf-8") if active_payload is not None else body_bytes

        try:
            client = get_shared_client()
            resp = await _send_with_transport_retry(
                client,
                method="POST",
                url=upstream_url,
                headers=headers,
                content=req_content,
                stream=True,
                max_retries=5,
            )

            resp_headers = _clean_response_headers(resp.headers)
            content_type = resp.headers.get("content-type", "")

            if resp.status_code == 200 and "text/event-stream" in content_type.lower():
                prime = await _prime_responses_sse(resp, request)
                if prime.status == "commit":
                    _clear_rate_limit_cooldown()
                    if fallback_active or transient_retries > 0 or mismatch_attempts > 0:
                        STATS["successful_recoveries"] += 1
                        logger.info(
                            "[Success] Request recovered (fallback=%s, retries=%d)!",
                            fallback_active, transient_retries + mismatch_attempts
                        )
                    return StreamingResponse(
                        _stream_generator(
                            resp,
                            primed_chunks=prime.primed_chunks,
                            remainder=prime.remainder,
                            queue=prime.queue,
                            producer_task=prime.producer_task,
                            producer_exc=prime.producer_exc,
                        ),
                        status_code=200,
                        headers=resp_headers,
                        media_type="text/event-stream",
                    )

                if prime.status == "client_disconnected":
                    return Response(status_code=499)

                if prime.status == "resource_mismatch":
                    if activate_stateless_fallback("SSE 200 error", prime.error_message):
                        await asyncio.sleep((RETRY_DELAY_MS / 1000.0) * max(1, mismatch_attempts))
                        continue
                    return JSONResponse(
                        status_code=400,
                        content={"error": prime.error_payload or {"message": prime.error_message, "code": prime.error_code}},
                        headers=resp_headers,
                    )

                is_rl = prime.status == "retry_rate_limit"
                STATS["stream_precommit_retries"] += 1
                should_retry, suggested_ra = await schedule_transient_wait(
                    is_rl,
                    resp.headers,
                    prime.error_message,
                    "Rate Limit / SSE" if is_rl else "Stream Pre-Commit",
                )
                if should_retry:
                    continue
                if await _client_disconnected(request):
                    return Response(status_code=499)
                if is_rl:
                    logger.error("[Forwarding Rate Limit 429] Exhausted retries: %s", prime.error_message[:200])
                    return _rate_limit_error_response(
                        prime.error_payload, prime.error_message, prime.error_code, suggested_ra, resp_headers
                    )
                logger.error("[Forwarding Stream Error 502] %s", prime.error_message[:200])
                return JSONResponse(
                    status_code=502,
                    content={"error": {"message": prime.error_message or "Upstream stream failed before emitting output", "type": "proxy_error"}},
                )

            if resp.status_code == 200:
                body = await resp.aread()
                await resp.aclose()
                parsed_body: Optional[Dict[str, Any]] = None
                try:
                    obj = json.loads(body.decode("utf-8", errors="replace"))
                    if isinstance(obj, dict):
                        parsed_body = obj
                except Exception:
                    pass

                if parsed_body is not None and (
                    bool(parsed_body.get("error")) or parsed_body.get("status") == "failed"
                ):
                    code, msg, err_obj = _extract_error_fields(parsed_body, "")
                    if is_resource_mismatch_text(msg):
                        if activate_stateless_fallback("JSON 200 error", msg):
                            continue
                    elif _is_rate_limit_error(code, msg) or _is_transient_error(code, msg):
                        is_rl = _is_rate_limit_error(code, msg)
                        should_retry, suggested_ra = await schedule_transient_wait(
                            is_rl, resp.headers, msg, "Rate Limit / JSON" if is_rl else "JSON Error"
                        )
                        if should_retry:
                            continue
                        if is_rl:
                            return _rate_limit_error_response(err_obj, msg, code, suggested_ra, resp_headers)

                _clear_rate_limit_cooldown()
                if fallback_active or transient_retries > 0 or mismatch_attempts > 0:
                    STATS["successful_recoveries"] += 1
                    logger.info("[Success] Request recovered!")
                return Response(content=body, status_code=200, headers=resp_headers)

            # Non-200 HTTP status
            err_body = await resp.aread()
            await resp.aclose()
            err_text = err_body.decode("utf-8", errors="replace")

            if is_resource_mismatch_error(resp.status_code, err_text):
                if activate_stateless_fallback("Upstream 400", err_text):
                    if mismatch_attempts > 0:
                        await asyncio.sleep((RETRY_DELAY_MS / 1000.0) * mismatch_attempts)
                    continue
                logger.error("[Forwarding Error %d] %s", resp.status_code, err_text[:200])
                return Response(content=err_body, status_code=resp.status_code, headers=resp_headers)

            is_http_rl = resp.status_code == 429 or _is_rate_limit_error("", err_text)
            if resp.status_code in (429, 502, 503, 504):
                should_retry, suggested_ra = await schedule_transient_wait(
                    is_http_rl,
                    resp.headers,
                    err_text,
                    f"Gateway {resp.status_code}",
                )
                if should_retry:
                    continue
                if await _client_disconnected(request):
                    return Response(status_code=499)
                if resp.status_code == 429 and not any(k.lower() == "retry-after" for k in resp_headers):
                    resp_headers["retry-after"] = str(suggested_ra)
                logger.error("[Forwarding Error %d] %s", resp.status_code, err_text[:200])
                return Response(content=err_body, status_code=resp.status_code, headers=resp_headers)

            logger.error("[Forwarding Error %d] %s", resp.status_code, err_text[:200])
            return Response(content=err_body, status_code=resp.status_code, headers=resp_headers)

        except Exception as exc:
            logger.error("[Attempt Exception] %s", str(exc))
            last_failure_response = JSONResponse(
                status_code=502,
                content={"error": {"message": f"Proxy upstream communication error: {str(exc)}", "type": "proxy_error"}},
            )
            should_retry, _ = await schedule_transient_wait(False, {}, str(exc), "Attempt Exception")
            if should_retry:
                continue
            return last_failure_response

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
async def passthrough_handler(request: Request, path: str):
    """Passthrough for models listing or any other API routes."""
    STATS["requests_total"] += 1

    headers = _filter_headers(dict(request.headers))
    body = await request.body()

    clean_path = path.lstrip("/")
    if not clean_path.startswith("v1/"):
        target_url = f"{UPSTREAM}/v1/{clean_path}"
    else:
        target_url = f"{UPSTREAM}/{clean_path}"

    try:
        client = get_shared_client()
        resp = await _send_with_transport_retry(
            client,
            method=request.method,
            url=target_url,
            headers=headers,
            params=dict(request.query_params),
            content=body,
            stream=False,
            max_retries=5,
        )
        resp_headers = {
            k: v for k, v in resp.headers.items()
            if k.lower() not in STRIP_RESPONSE_HEADERS
        }
        return Response(content=resp.content, status_code=resp.status_code, headers=resp_headers)
    except Exception as exc:
        logger.error("[Passthrough Error] %s -> %s: %s", request.method, target_url, str(exc))
        return JSONResponse(
            status_code=502,
            content={"error": {"message": f"Proxy passthrough error: {str(exc)}", "type": "proxy_error"}}
        )

# ----------------------------------------------------------------------------
# Main entrypoint
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    print(f"[*] Starting agentrouter-codex-proxy on http://{LISTEN_HOST}:{PORT}")
    print(f"[*] Forwarding to upstream: {UPSTREAM}")
    uvicorn.run(app, host=LISTEN_HOST, port=PORT, timeout_keep_alive=600, log_level="info")
