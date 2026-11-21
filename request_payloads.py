"""Bounded request decoding for JSON Responses, including Codex zstd bodies."""

from __future__ import annotations

import json
import zlib
from typing import Any

try:
    import zstandard
except ImportError:
    zstandard = None


class RequestPayloadError(ValueError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


def available_request_encodings() -> list[str]:
    return ["identity", "gzip", "deflate"] + (["zstd"] if zstandard is not None else [])


def _inflate(body: bytes, limit: int, wbits: int) -> bytes:
    decoder = zlib.decompressobj(wbits)
    decoded = decoder.decompress(body, limit + 1)
    if len(decoded) > limit or decoder.unconsumed_tail:
        raise RequestPayloadError(413, "Decoded Responses request exceeds the size limit")
    if not decoder.eof or decoder.unused_data:
        raise RequestPayloadError(400, "Invalid compressed Responses request")
    return decoded


def _unzstd(body: bytes, limit: int) -> bytes:
    if zstandard is None:
        raise RequestPayloadError(415, "Install the proxy zstandard dependency to accept zstd requests")
    params = zstandard.get_frame_parameters(body)
    known_size = params.content_size not in (zstandard.CONTENTSIZE_UNKNOWN, zstandard.CONTENTSIZE_ERROR)
    if (known_size and params.content_size > limit) or params.window_size > limit:
        raise RequestPayloadError(413, "Decoded Responses request exceeds the size limit")
    # The single frame's actual window was bounded above before constructing
    # the decoder; this also avoids backend-specific max_window_size units.
    decoder = zstandard.ZstdDecompressor()
    try:
        decoded = decoder.decompress(body, max_output_size=limit + 1, allow_extra_data=False)
    except zstandard.ZstdError as exc:
        # The backend uses the same error for truncated frames and unknown-size
        # frames that overflow max_output_size. Only report 413 after measuring
        # a bounded prefix; a truncated frame that fits remains a 400. Never
        # accept this diagnostic reader's partial output as a valid frame.
        if not known_size and (
            "too small" in str(exc).lower() or "did not decompress full frame" in str(exc).lower()
        ):
            with zstandard.ZstdDecompressor().stream_reader(body, read_across_frames=False) as reader:
                if len(reader.read(limit + 1)) > limit:
                    raise RequestPayloadError(413, "Decoded Responses request exceeds the size limit") from exc
        raise
    if len(decoded) > limit:
        raise RequestPayloadError(413, "Decoded Responses request exceeds the size limit")
    return decoded


def parse_responses_payload(body: bytes, content_encoding: str, max_bytes: int) -> dict[str, Any]:
    """Decode for inspection; the caller can still forward ORIGINAL bytes untouched."""
    if len(body) > max_bytes:
        raise RequestPayloadError(413, "Responses request exceeds the size limit")
    encoding = content_encoding.strip().lower()
    try:
        if encoding in ("", "identity"):
            decoded = body
        elif encoding == "gzip":
            decoded = _inflate(body, max_bytes, 16 + zlib.MAX_WBITS)
        elif encoding == "deflate":
            decoded = _inflate(body, max_bytes, zlib.MAX_WBITS)
        elif encoding == "zstd":
            decoded = _unzstd(body, max_bytes)
        else:
            raise RequestPayloadError(415, "Unsupported Responses request content-encoding")
        parsed = json.loads(decoded.decode("utf-8-sig"), parse_constant=_reject_constant)
    except RequestPayloadError:
        raise
    except (ValueError, UnicodeError, RecursionError, zlib.error) as exc:
        raise RequestPayloadError(400, "Invalid JSON or compressed Responses request") from exc
    except Exception as exc:
        # zstd errors are intentionally not echoed: they can contain body details.
        if zstandard is not None and isinstance(exc, zstandard.ZstdError):
            raise RequestPayloadError(400, "Invalid compressed Responses request") from exc
        raise
    if not isinstance(parsed, dict):
        raise RequestPayloadError(400, "Responses request must be a JSON object")
    return parsed


def _reject_constant(_value: str) -> None:
    raise ValueError("Non-finite JSON numbers are not supported")
