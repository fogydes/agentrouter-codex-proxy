# Multi-client session-aware Responses sanitization

This local modification keeps the original request path for healthy sessions.
After a cross-Azure-resource error, the proxy retries with sanitized input and
remembers that exact tool/session. Later requests from that session are
sanitized before the first upstream attempt. Other sessions remain unchanged.

## Identity and storage

- OpenCode 2.3 sends the real `x-opencode-session-id` natively. No plugin or
  OpenCode configuration change is required for this installation.
- Codex is recognized by its native `thread-id`, `x-codex-turn-metadata`, or
  Codex-owned `client_metadata` with the real UUID thread ID. The thread wins over
  session/cache affinity. Older Codex clients with only a UUID `session-id` are
  supported when product attribution identifies them as Codex.
- DeepSeek Harness is recognized by `User-Agent: deepseek-harness/...`. Its pi-ai
  1.3.0 Responses adapter sends `session_id` and/or `x-client-request-id` from
  `sessionId`, or `x-session-id` on OpenRouter-style routes. The pi-ai Codex SSE
  adapter uses `session-id`. Conflicting aliases are treated as unidentified.
- The otherwise-overloaded `x-client-request-id` is used ONLY for the verified
  DSH adapter contract, never for Codex or unidentified tools. A cache key, parent
  thread, turn ID, process ID, or connection is not a session identity.
- A client without native identifiers can explicitly send
  `x-agentrouter-client: generic` and `x-agentrouter-session-id: <session-id>`.
  Other supported explicit client names are `opencode`, `codex`, and `dsh`.
  OpenCode IDs must use the native `ses_...` format; Codex IDs must be thread
  UUIDs (case is normalized). DSH/generic IDs must be short nonempty ASCII
  identifiers, not prompts or keys. Do not use one fixed session header for
  every conversation.
- Flags are isolated by upstream + client namespace + session ID. Different tools
  or child threads do not inherit one another's flags, even if IDs coincide.
- Flags survive proxy restarts in `logs/session_modes.json`. The file contains
  only an upstream/client-scoped SHA-256 session identifier, product name, mode,
  and activation time; no raw session IDs, prompts, or API keys are stored.
  Existing OpenCode flags keep their original keys and remain valid.
- Missing, invalid, or ambiguous identity retains per-request recovery and never
  shares a global remembered mode. DSH can omit identifiers when cache retention
  is disabled; the proxy cannot reliably invent identity in that case.
- Only the cross-resource error activates a persistent flag. Rate limits,
  payment/quota errors, and image limits do not permanently change the mode.
- Stored-history pointers, `item_reference`, encrypted compaction checkpoints,
  and tool outputs without their corresponding call are not blindly stripped.
  Those requests pass through, but an error is forwarded without a destructive
  retry if the proxy cannot reconstruct the missing history. This is an explicit
  safety limit, not a promise of compatibility with incremental clients. Normal
  OpenCode, Codex HTTP, and DSH Responses requests replay full input history.
- A late SSE error after visible output cannot safely be retried by the proxy,
  but it still marks this session for subsequent requests.

Sanitization removes opaque historical reasoning and server-minted IDs while
preserving visible conversation, assistant `phase`, tools, and `call_id` links.
This trades historical reasoning continuity for compatibility, as the original
reactive fallback already did. It does not guarantee zero errors or reduce the
number of normal tool-driven model calls.

## HTTP protocol and compression

The compatibility handler accepts `POST /v1/responses` and `POST /responses`.
This is not a Chat Completions-to-Responses converter and not a WebSocket proxy.
Use the HTTP/SSE Responses route when requests are directed here. Tool config
files are not automatically edited by the proxy.

Plain JSON, gzip, deflate, and zstd requests are decoded for inspection. Healthy
requests keep their exact original body bytes and content encoding. Rewritten
requests use plain UTF-8 JSON with the obsolete encoding/digest headers removed,
preventing compressed clients from misinterpreting a sanitized retry. Encoded and
decoded bodies are limited to 64 MiB by default (`PROXY_MAX_REQUEST_BYTES`).
Malformed/unsupported encoding is rejected locally rather than sent repeatedly
upstream. The system Python needs `zstandard==0.25.0` for Codex zstd requests.
No venv is required.

Authorization and existing routing headers remain request-local and are passed
through. Rate-limit cooldown remains shared by this single-upstream service; this
change does not implement per-account rate-limit isolation or multi-upstream
routing. Concurrent tools sharing the same upstream quota can still be throttled.

## Diagnostics

`GET http://127.0.0.1:8788/_health` includes `session_mode_memory` with activation,
supported client/identity sources, number of marked sessions, persistence status,
request encodings, and request-size limit.

`GET http://127.0.0.1:8788/_stats` adds:

- `requests_with_session_id` / `requests_without_session_id`
- `session_modes_activated`
- `preemptive_sanitizations` / `preemptive_sanitization_skipped`
- `responses_upstream_attempts` (physical HTTP sends, including retries)
- `unsafe_recovery_skipped` (requests whose missing history cannot be reconstructed)
- `clients` (bounded product counters: requests, identified requests, activations,
  and preemptive sanitizations; never session IDs or secrets)

Preemptively successful requests do not increment `successful_recoveries`, which
remains a counter for requests that actually needed recovery. Session mode log
messages use only a shortened hash.

Inspect or reset ONE session via local-only JSON POST endpoints:

```powershell
$body = @{ session_id = 'ses_YOUR_REAL_SESSION_ID' } | ConvertTo-Json
Invoke-RestMethod http://127.0.0.1:8788/_session/mode -Method Post -ContentType application/json -Body $body
Invoke-RestMethod http://127.0.0.1:8788/_session/reset -Method Post -ContentType application/json -Body $body
# For other tools include the client namespace:
$body = @{ client = 'codex'; session_id = 'YOUR_REAL_THREAD_UUID' } | ConvertTo-Json
# Or client = 'dsh', session_id = the original DSH sessionId.
```

Reset returns the session to passthrough until it encounters this error again.
These endpoints reject non-loopback callers and never forward to AgentRouter.

## Disable the local feature

Set `PROXY_SESSION_MODE_MEMORY=0` in `.env`, then restart the proxy's scheduled
task. This restores reactive-only behavior without deleting the saved flags.
The default is enabled. Automatic code updates should remain disabled to retain
this local modification.

## Tests

Run the session tests using the system Python 3.11 (no venv needed):

```powershell
python -m unittest discover -s tests -p test_session_modes.py -v
python -m unittest discover -s tests -p test_multi_client.py -v
```

The tests use mock upstream responses and temporary flag files, not real API
calls or the production session state.

## Reviewed transport contracts

- OpenCode 2.3 native requests: `x-opencode-session-id` was verified in the local
  runtime and real proxy requests; published provider HTTP docs are at
  <https://opencode.ai/docs/providers>.
- Codex: <https://github.com/openai/codex/blob/main/codex-rs/codex-api/src/requests/headers.rs>
  and <https://github.com/openai/codex/blob/main/codex-rs/core/src/responses_metadata.rs>.
- DSH adapter snapshot `39163dd30f703a79079e2d2446545cff3c453738`:
  `packages/llm/llm-pi-ai/src/adapter.ts` and `packages/llm/llm/src/attribution.ts`.
- Its installed-contract dependency `@earendil-works/pi-ai@1.3.0`:
  `dist/api/openai-responses.js` and `dist/api/openai-codex-responses.js`.

These contracts do not establish that every future version or custom plugin
uses the same headers. Unidentified requests safely fall back to reactive-only
recovery. Only OpenCode has been exercised live on this machine; Codex and DSH
compatibility is covered by source-matched mock wire fixtures, not by editing or
launching those tools.
