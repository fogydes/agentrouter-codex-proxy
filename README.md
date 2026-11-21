# AgentRouter Codex Responses Compatibility Proxy

A lightweight local compatibility proxy for clients using the OpenAI Responses API through AgentRouter. It recovers from cross-Azure-resource state errors for **Codex**, **OpenCode**, **DeepSeek Harness (DSH)**, and explicitly identified generic clients.

```text
Codex ─────┐
OpenCode ──┼──> local proxy ──> AgentRouter ──> Azure OpenAI resources
DSH ───────┘    127.0.0.1:8788
```

> [!IMPORTANT]
> This proxy works around resource-local Responses state. It does not change AgentRouter's routing and cannot guarantee that every upstream failure will disappear.

## The problem

Multi-turn Responses requests can fail when a gateway load-balances turns across different Azure OpenAI resources:

```text
The requested item was created under a different Azure OpenAI resource.
Use the same resource that created the item to access it.
```

Responses payloads can contain resource-local state such as:

- `previous_response_id`
- server-minted item IDs
- encrypted historical reasoning
- item references and compaction checkpoints

An item created on resource A may not exist or be decryptable on resource B, so a later request routed to B is rejected.

## How recovery works

1. **Transparent fast path** — an unmarked session is sent upstream with its original request body bytes and compression unchanged.
2. **Reactive recovery** — if the upstream reports a cross-resource mismatch and the request contains reconstructable history, the proxy removes resource-local state and retries with a self-contained payload.
3. **Per-session memory** — after the first cross-resource mismatch, that exact upstream/client/session combination is remembered. Later requests from it are sanitized before their first upstream attempt.
4. **Isolation** — OpenCode, Codex, DSH, generic clients, parent/child sessions, and unrelated sessions never share a remembered mode.
5. **Safe fallback** — unidentified requests can still recover per request, but they never receive a shared persistent flag.

Sanitization removes historical reasoning and server-minted IDs while preserving visible messages, assistant phases, tools, and `call_id` links. This trades resource-local reasoning continuity and some cache reuse for cross-resource compatibility.

The proxy also retains the existing behavior for:

- early SSE error inspection before response headers are committed;
- bounded rate-limit/transient retries with `Retry-After` support;
- historical image pruning below the Responses image limit;
- transport reconnects, DNS fallback, TCP keep-alive, and Windows Proactor reset handling.

## Supported clients and session identity

Persistent recovery is enabled only when the proxy can verify a real conversation identity:

| Client | Verified identity source |
| --- | --- |
| OpenCode | `x-opencode-session-id` using the native `ses_...` format |
| Codex | UUID `thread-id`, Codex thread metadata, or legacy UUID `session-id` |
| DeepSeek Harness | `User-Agent: deepseek-harness/...` plus a consistent `session_id`, `session-id`, `x-session-id`, or `x-client-request-id` |
| Generic | Explicit `x-agentrouter-client: generic` and `x-agentrouter-session-id` headers |

Cache keys, parent-thread IDs, turn IDs, model IDs, credentials, and network connections are never treated as session identity. Missing, invalid, duplicate, or conflicting identities fall back to reactive-only recovery.

Supported native clients do not need a plugin or extra identity header once they are configured to use this proxy. Custom transports must preserve the native fields above or use the explicit generic headers.

## Safe handling of unavailable history

The proxy does **not** blindly remove state when the request depends on history that is unavailable locally. A destructive retry is skipped for inputs such as:

- `previous_response_id` without replayed history;
- `item_reference` entries;
- encrypted compaction checkpoints;
- tool outputs whose corresponding calls are not present.

In those cases, the original upstream error is forwarded rather than silently losing conversation context. A late SSE error after output has already been shown also cannot be replayed safely; it is forwarded and only marks future requests from that session.

## HTTP protocol and compression

The compatibility handler accepts:

- `POST /v1/responses`
- `POST /responses`
- JSON responses and `text/event-stream` (SSE)
- identity, gzip, deflate, and zstd request bodies

This project is an HTTP Responses proxy. It is **not** a WebSocket proxy and does not convert Chat Completions into Responses.

### Why `zstandard` is required

Some Codex Responses transports send request bodies with:

```http
Content-Encoding: zstd
```

The proxy must decode the body to identify a session and safely inspect or sanitize its JSON. The `zstandard` package provides this decoder; it is not the cross-resource fix itself.

- Healthy compressed requests retain their exact original body bytes and encoding.
- Rewritten requests are emitted as plain JSON with stale encoding and digest headers removed.
- Both encoded and decoded bodies are bounded to 64 MiB by default to limit decompression abuse.
- Malformed or unsupported encodings are rejected locally.

## Installation

### Requirements

- Python 3.10+
- Packages pinned in `requirements.txt`: FastAPI, Starlette, Uvicorn, HTTPX, and zstandard

A virtual environment is optional. Install dependencies with:

```bash
python -m pip install -r requirements.txt
```

On Windows, `start-proxy.ps1` checks these dependencies and installs missing packages automatically.

### Environment

Copy the example file and set the upstream URL if needed:

```powershell
Copy-Item .env.example .env
```

Common settings:

| Variable | Default | Purpose |
| --- | --- | --- |
| `AGENTROUTER_UPSTREAM` | `https://agentrouter.org` | Upstream gateway base URL |
| `PROXY_DNS_FALLBACKS` | unset | Optional fallback IPs for upstream hosts when DNS resolution fails, e.g. `ps.air-outer.com:8.214.160.125,8.214.161.192` |
| `CODEX_GATEWAY_API_KEY` / `AGENTROUTER_API_KEY` | unset | Fallback authorization only when the incoming request has none |
| `PROXY_PORT` | `8788` | Local listening port |
| `PROXY_SESSION_MODE_MEMORY` | `1` | Enable persistent per-session sanitization flags |
| `PROXY_MAX_REQUEST_BYTES` | `67108864` | Maximum encoded and decoded Responses body size |
| `PROXY_MAX_IMAGES` | `35` | Images retained in one request |
| `PROXY_WRITE_TIMEOUT_S` | `180.0` | Seconds allowed to upload the request body to upstream (was hardcoded at 60s; raise for large payloads on slow links) |
| `PROXY_POOL_TIMEOUT_S` | `30.0` | Seconds allowed to wait for a free upstream connection slot |
| `PROXY_KEEPALIVE_TIMEOUT_S` | `600.0` | Idle keep-alive seconds between client requests (uvicorn `--timeout-keep-alive`) |

See `.env.example` for retry, timeout, logging, and updater settings. The bind host is deliberately a command-line option rather than an environment setting.

### Start the proxy

Windows PowerShell:

```powershell
cd D:\Projects\agentrouter-codex-proxy
.\start-proxy.ps1                    # interactive foreground
.\start-proxy.ps1 -Background        # headless background service
.\start-proxy.ps1 -Update            # apply a safe fast-forward update first
.\start-proxy.ps1 -BindHost 0.0.0.0  # Docker/LAN; firewall protection required
```

Direct Python, Linux, or Docker usage:

```bash
python proxy.py --host 127.0.0.1 --port 8788
# or
python service.py --host 127.0.0.1 --port 8788
uvicorn proxy:app --host 127.0.0.1 --port 8788
```

> [!WARNING]
> The default listener is loopback-only. A non-loopback bind exposes a forwarding proxy and possibly its fallback credential. Use `0.0.0.0` only inside an isolated container or a properly firewalled network.

## Client configuration

Point the client's OpenAI Responses base URL at:

```text
http://127.0.0.1:8788/v1
```

Use the client's HTTP/SSE Responses transport. The proxy does not edit client configuration files.

### Codex example

In `C:\Users\<username>\.codex\config.toml`:

```toml
model = "<your-agentrouter-model>"
model_provider = "Agentrouter"
model_reasoning_effort = "high"

[model_providers.Agentrouter]
name = "Agentrouter"
base_url = "http://127.0.0.1:8788/v1"
wire_api = "responses"
env_key = "CODEX_GATEWAY_API_KEY"
```

For OpenCode, set the relevant provider base URL to the same `/v1` endpoint. For a DeepSeek Harness custom provider, select the **OpenAI Responses** protocol and use the same base URL. Exact configuration fields can vary by client version.

## Windows headless autostart

Register a hidden Task Scheduler logon task using `pythonw.exe`:

```powershell
.\install-autostart.ps1
```

Disable automatic source updates when running local modifications:

```powershell
.\install-autostart.ps1 -NoAutoUpdate
```

Manual update commands and removal:

```powershell
python updater.py --check
python updater.py --apply
.\uninstall-autostart.ps1
```

Service logs are written to `logs/proxy.log` with bounded rotation.

## Persistent session state

Remembered modes survive restarts in `logs/session_modes.json`. The file stores only:

- a SHA-256 key scoped by upstream, client, and session;
- the client namespace;
- mode and activation time.

It does not store raw session IDs, prompts, model output, or API keys. Existing OpenCode state keys from earlier versions remain compatible.

Inspect or reset one session through loopback-only endpoints:

```powershell
# OpenCode is the default namespace
$body = @{ session_id = 'ses_YOUR_SESSION_ID' } | ConvertTo-Json
Invoke-RestMethod http://127.0.0.1:8788/_session/mode -Method Post -ContentType application/json -Body $body
Invoke-RestMethod http://127.0.0.1:8788/_session/reset -Method Post -ContentType application/json -Body $body

# Other clients specify their namespace
$body = @{ client = 'codex'; session_id = 'YOUR_THREAD_UUID' } | ConvertTo-Json
Invoke-RestMethod http://127.0.0.1:8788/_session/mode -Method Post -ContentType application/json -Body $body
```

Reset returns the session to passthrough mode until it encounters another cross-resource mismatch.

## Health and diagnostics

- `GET /_health` — listener, upstream, compression support, session persistence, and bounded client counters
- `GET /_stats` — request, retry, mismatch, recovery, and per-client aggregate counters
- `POST /_session/mode` — inspect one session mode; loopback only
- `POST /_session/reset` — reset one session mode; loopback only

No raw identities or credentials are returned by health and statistics endpoints.

## DNS resilience for custom upstreams

The proxy keeps a bounded in-memory DNS cache for every upstream host it
resolves successfully. If the system resolver later has a transient failure
(`Temporary failure in name resolution`), the last known addresses are reused
so an already-working upstream keeps working.

For a custom or region-specific upstream whose IP is not built in, configure a
fallback explicitly. This avoids depending on the server's DNS at all:

```dotenv
PROXY_DNS_FALLBACKS=ps.air-outer.com:8.214.160.125,8.214.161.192
```

Format is `host:ip,ip;other-host:ip`. Host names are case-insensitive and IPs
are validated; malformed entries are logged and ignored. Fallbacks are only
consulted after both the system resolver and the DNS cache fail. Because this
value is read at request time, it also works when set in `.env` (which the
proxy loads after the DNS module is imported).

## Tests

The tests use mock upstream responses and temporary state files; they do not call a real model API or modify production session state.

```bash
python -m unittest discover -s tests -p "test_session_modes.py" -v
python -m unittest discover -s tests -p "test_multi_client.py" -v
python -m unittest discover -s tests -p "test_proxy.py" -v
```

Additional checks:

```bash
python -m py_compile proxy.py session_modes.py request_payloads.py service.py
git diff --check
```

## Limitations

- Only HTTP Responses/SSE is supported; Responses WebSocket transport is not proxied.
- The first mismatch still occurs upstream. The proxy hides it only when safe recovery is possible.
- Incremental clients that send only unavailable server references cannot be reconstructed locally.
- Output already delivered to a client cannot be safely replayed after a late stream error.
- The rate-limit cooldown is shared by this single-upstream process; there is no per-account isolation or multi-upstream routing.
- Future client versions or custom plugins may use different identity fields. Unidentified requests remain reactive-only rather than guessing.

See [`SESSION-MODES.md`](SESSION-MODES.md) for the detailed identity, storage, recovery, and transport contracts.
