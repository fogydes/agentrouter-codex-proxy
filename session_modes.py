"""Client-aware session identity and durable flags; no prompts or raw IDs stored."""

from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import dataclass
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import threading
from typing import Any, Mapping
from uuid import UUID


logger = logging.getLogger("agentrouter-codex-proxy")
SESSION_ID_HEADER = "x-opencode-session-id"
_SESSION_ID_RE = re.compile(r"ses_[A-Za-z0-9_-]{1,200}\Z")
_STATE_KEY_RE = re.compile(r"[0-9a-f]{64}\Z")
_OPAQUE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,200}\Z")
CLIENTS = ("opencode", "codex", "dsh", "generic")


@dataclass(frozen=True)
class SessionIdentity:
    client: str
    session_id: str
    source: str = "explicit"

    def __post_init__(self) -> None:
        if self.client not in CLIENTS or not valid_client_session_id(self.client, self.session_id):
            raise ValueError("Invalid client/session identity")
        if self.client == "codex":
            # Native UUIDs and explicit/admin UUIDs must address the same flag.
            object.__setattr__(self, "session_id", str(UUID(self.session_id)))


def valid_session_id(value: Any) -> bool:
    return isinstance(value, str) and bool(_SESSION_ID_RE.fullmatch(value))


def valid_client_session_id(client: Any, value: Any) -> bool:
    if client == "opencode":
        return valid_session_id(value)
    if client == "codex":
        return _uuid(value) is not None
    return client in CLIENTS and isinstance(value, str) and bool(_OPAQUE_ID_RE.fullmatch(value))


def _header_values(headers: Mapping[str, str]) -> dict[str, str] | None:
    result: dict[str, str] = {}
    for key, value in headers.items():
        key = key.lower()
        if key in result and result[key] != value:
            # Conflicting case aliases / duplicate identity headers are ambiguous.
            if key in {
                SESSION_ID_HEADER, "thread-id", "session-id", "session_id", "x-session-id",
                "x-client-request-id", "x-agentrouter-client", "x-agentrouter-session-id",
                "x-codex-turn-metadata", "user-agent", "originator",
            }:
                return None
        result[key] = value
    return result


def _object(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and len(value) <= 16384:
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (ValueError, RecursionError):
            pass
    return {}


def _uuid(value: Any) -> str | None:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", value):
        return None
    try:
        return str(UUID(value))
    except ValueError:
        return None


def detect_client(headers: Mapping[str, str], payload: Any = None) -> str:
    """Product attribution alone identifies a tool, never one of its sessions."""
    h = _header_values(headers)
    if h is None:
        return "unknown"
    if SESSION_ID_HEADER in h:
        return "opencode"
    user_agent = h.get("user-agent", "").lower()
    if re.search(r"(?:^|\s)deepseek-harness(?:/|\s|$)", user_agent):
        # DSH may itself use the Codex wire adapter; it remains a DSH client.
        return "dsh"
    metadata = _object(payload.get("client_metadata")) if isinstance(payload, dict) else {}
    if (
        "thread-id" in h or "x-codex-turn-metadata" in h
        or "x-codex-turn-metadata" in metadata
        or "x-codex-installation-id" in metadata
        or re.search(r"(?:^|\s)codex(?:[_/-]|\s|$)", user_agent)
        or h.get("originator", "").lower().startswith(("codex_", "codex-"))
    ):
        return "codex"
    if h.get("x-agentrouter-client") in CLIENTS:
        return h["x-agentrouter-client"]
    return "unknown"


def extract_session_identity(headers: Mapping[str, str], payload: Any = None) -> SessionIdentity | None:
    """Recognize documented wire identities, not generic cache/parent/turn IDs.

    DSH's pi-ai 1.3 Responses transport sends session_id or x-client-request-id
    from its sessionId option (x-session-id on OpenRouter-compatible routes).
    Only trust those overloaded headers with DeepSeek Harness attribution.
    Codex's real thread-id/turn metadata wins over its session/cache affinity.
    """
    h = _header_values(headers)
    if h is None:
        return None
    client = detect_client(h, payload)
    if client == "opencode" and SESSION_ID_HEADER in h:
        value = h[SESSION_ID_HEADER]
        return SessionIdentity(client, value, SESSION_ID_HEADER) if valid_session_id(value) else None

    if "x-agentrouter-client" in h or "x-agentrouter-session-id" in h:
        explicit_client = h.get("x-agentrouter-client")
        value = h.get("x-agentrouter-session-id")
        if not valid_client_session_id(explicit_client, value):
            return None
        return SessionIdentity(explicit_client, value, "x-agentrouter-session-id")

    if client == "dsh":
        supplied = [(name, h[name]) for name in (
            "session_id", "session-id", "x-session-id", "x-client-request-id"
        ) if name in h]
        if supplied:
            if any(not valid_client_session_id(client, value) for _, value in supplied):
                return None
            if len({value for _, value in supplied}) != 1:
                return None
            return SessionIdentity(client, supplied[0][1], supplied[0][0])
        # No session ID is sent when the adapter disables cache/session metadata.
        return None

    if client == "codex":
        metadata = _object(payload.get("client_metadata")) if isinstance(payload, dict) else {}
        turn = _object(h.get("x-codex-turn-metadata"))
        body_turn = _object(metadata.get("x-codex-turn-metadata"))
        supplied = [(name, source[key]) for name, source, key in (
            ("thread-id", h, "thread-id"),
            ("x-codex-turn-metadata.thread_id", turn, "thread_id"),
            ("client_metadata.thread_id", metadata, "thread_id"),
            ("client_metadata.x-codex-turn-metadata.thread_id", body_turn, "thread_id"),
        ) if key in source]
        if supplied:
            canonical = [_uuid(value) for _, value in supplied]
            if any(value is None for value in canonical) or len(set(canonical)) != 1:
                return None
            return SessionIdentity(client, canonical[0], supplied[0][0])
        # Older Codex versions send only a UUID-valued session-id. Never infer
        # identity from x-client-request-id or prompt_cache_key on Codex calls.
        supplied = [(name, source[key]) for name, source, key in (
            ("client_metadata.session_id", metadata, "session_id"),
            ("x-codex-turn-metadata.session_id", turn, "session_id"),
            ("client_metadata.x-codex-turn-metadata.session_id", body_turn, "session_id"),
            ("session-id", h, "session-id"),
        ) if key in source]
        if supplied:
            canonical = [_uuid(value) for _, value in supplied]
            if any(value is None for value in canonical) or len(set(canonical)) != 1:
                return None
            return SessionIdentity(client, canonical[0], supplied[0][0])
    return None


def extract_session_id(headers: Mapping[str, str]) -> str | None:
    """Backward-compatible OpenCode-only accessor."""
    identity = extract_session_identity(headers)
    return identity.session_id if identity and identity.client == "opencode" else None


class SessionModeStore:
    """Remember only sessions that encountered a cross-resource error.

    One proxy worker owns this file. Thread locking and atomic replacement keep
    concurrent requests from losing flags. A write failure retains the in-memory
    flag and is exposed in health diagnostics rather than breaking model calls.
    """

    def __init__(self, path: Path, upstream: str) -> None:
        self.path = Path(path)
        self.upstream = upstream.rstrip("/")
        self._lock = threading.RLock()
        self._sessions: dict[str, dict[str, str]] = {}
        self.persistence_ok = True
        self._load()

    @staticmethod
    def _identity(value: SessionIdentity | str) -> SessionIdentity:
        return value if isinstance(value, SessionIdentity) else SessionIdentity("opencode", value)

    def key_for(self, session: SessionIdentity | str) -> str:
        session = self._identity(session)
        if session.client == "opencode":
            # Preserve already-saved OpenCode flags without access to raw IDs.
            identity = f"agentrouter-session-mode-v1\0{self.upstream}\0{session.session_id}"
        else:
            identity = f"agentrouter-session-mode-v2\0{self.upstream}\0{session.client}\0{session.session_id}"
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()

    def _load(self) -> None:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return
        except (OSError, ValueError, RecursionError):
            self.persistence_ok = False
            logger.warning("[Session Mode] Cannot read session flags; using in-memory state")
            return

        if not isinstance(data, dict) or data.get("version") != 1 or not isinstance(data.get("sessions"), dict):
            self.persistence_ok = False
            logger.warning("[Session Mode] Invalid session flag file; using in-memory state")
            return
        for key, entry in data["sessions"].items():
            if (
                isinstance(key, str)
                and _STATE_KEY_RE.fullmatch(key)
                and isinstance(entry, dict)
                and entry.get("mode") == "sanitize"
                and isinstance(entry.get("marked_at"), str)
            ):
                self._sessions[key] = {
                    "mode": "sanitize", "marked_at": entry["marked_at"],
                    "client": entry.get("client") if entry.get("client") in CLIENTS else "opencode",
                }

    def _save_locked(self) -> None:
        staging = self.path.with_name(f"{self.path.name}.{os.getpid()}.tmp")
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with staging.open("w", encoding="utf-8") as fh:
                json.dump({"version": 1, "sessions": self._sessions}, fh, indent=2, sort_keys=True)
                fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(staging, self.path)
            self.persistence_ok = True
        except OSError:
            self.persistence_ok = False
            logger.warning("[Session Mode] Cannot persist session flags; keeping them in memory")
        finally:
            try:
                staging.unlink(missing_ok=True)
            except OSError:
                pass

    def should_sanitize(self, session_id: SessionIdentity | str) -> bool:
        with self._lock:
            return self.key_for(session_id) in self._sessions

    def mark_sanitized(self, session_id: SessionIdentity | str) -> bool:
        """Return True only on the first activation for this session/upstream."""
        with self._lock:
            key = self.key_for(session_id)
            if key in self._sessions:
                return False
            self._sessions[key] = {
                "mode": "sanitize",
                "marked_at": datetime.now(timezone.utc).isoformat(),
                "client": self._identity(session_id).client,
            }
            self._save_locked()
            return True

    def reset(self, session_id: SessionIdentity | str) -> bool:
        with self._lock:
            key = self.key_for(session_id)
            if key not in self._sessions:
                return False
            del self._sessions[key]
            self._save_locked()
            return True

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._sessions)
