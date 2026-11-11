# AgentRouter Codex Responses Compatibility Proxy

A lightweight local compatibility proxy that fixes cross-resource session errors when using **Codex Desktop** or **Codex CLI** with **AgentRouter**.

---

## The Problem

When running Codex through AgentRouter with `wire_api = "responses"` (the OpenAI Responses API), multi-turn sessions frequently fail with:

```text
OpenAI Responses bad request: The requested item was created under a different **** OpenAI resource.
Use the same resource that created the item to access it. [trace_id=...]
```

### Why does this happen?
1. **Responses API is Stateful**: The OpenAI Responses API (`/v1/responses`) relies on server-stored conversation state (`previous_response_id`, server-minted item IDs, and encrypted reasoning content).
2. **Upstream Load Balancing**: AgentRouter load-balances traffic across multiple Azure OpenAI instances.
3. **Resource Isolation in Azure**: When Turn 1 runs on Azure Resource A, all response IDs and items are stored inside Resource A. When Turn 2 is routed by AgentRouter to Resource B, Resource B checks the IDs against its own store, fails to locate or decrypt them, and rejects the request with HTTP 400.

---

## How This Proxy Solves It

```
┌─────────────────┐       ┌───────────────────────────────┐       ┌───────────────────────┐
│  Codex Desktop  │  -->  │  agentrouter-codex-proxy      │  -->  │  AgentRouter Upstream │
│  or Codex CLI   │  <--  │  (127.0.0.1:8788)             │  <--  │  (https://...)        │
└─────────────────┘       └───────────────────────────────┘       └───────────────────────┘
                                     │
                    ┌────────────────┴────────────────┐
                    │ 1. Transparent Fast-Path        │
                    │    Attempts request as-is to    │
                    │    preserve server KV cache.    │
                    │                                 │
                    │ 2. Smart Fallback on 400:       │
                    │    Detects resource mismatch,   │
                    │    sanitizes payload:           │
                    │    - Strips previous_response_id│
                    │    - Strips server item IDs     │
                    │    - Drops un-decryptable CoT   │
                    │                                 │
                    │ 3. Instant Recovery:            │
                    │    Retries immediately with     │
                    │    sanitized conversation.      │
                    │    ANY upstream instance can    │
                    │    process it without error!    │
                    └─────────────────────────────────┘
```

1. **Zero Latency on Normal Turns**: As long as requests hit the same backend, requests pass through unmodified with maximum performance and KV cache hits.
2. **Instant Self-Healing Fallback**: When Azure OpenAI returns a 400 Resource Mismatch (or an SSE `response.failed` mismatch), the proxy catches the error, converts the payload into a self-contained stateless format (retaining complete conversation history while pruning excess historical screenshots past 35 images), and immediately resubmits.
3. **Pre-Commit SSE Priming & Rate-Limit Recovery**: Buffers initial `200 OK` `text/event-stream` setup frames long enough to catch early `rate_limit_exceeded` / `response.failed` / empty streams before committing headers to Codex, coordinates a shared cooldown + stagger across concurrent subagent calls, and honors `Retry-After` / reset hints before retrying.
4. **Windows & DNS Resilience**: Provides resilient DNS fallback (`8.214.160.125`, `8.214.161.192`), TCP keep-alive hooks, and a Python 3.12 Windows `ProactorEventLoop` `_call_connection_lost` guard (`WinError 10054`).
5. **Codex Keeps Streaming**: Codex receives clean `text/event-stream` tokens and `: ping` heartbeats with far fewer interruptions.

---

## Setup & Usage

### 1. Requirements
- Python 3.10+
- Dependencies: `fastapi`, `uvicorn`, `httpx` (installed automatically by `start-proxy.ps1`)

### 2. Quick Start (Interactive)
```powershell
cd D:\Projects\agentrouter-codex-proxy
.\start-proxy.ps1
```

### 3. Autostart as a Windows Background Task (Recommended)
Registers a scheduled task that starts automatically when you log in to Windows:
```powershell
cd D:\Projects\agentrouter-codex-proxy
.\install-autostart.ps1
```
To remove the background task:
```powershell
.\uninstall-autostart.ps1
```

---

## Configuring Codex

In `C:\Users\<username>\.codex\config.toml`:

```toml
model = "gpt-6-astra"
model_provider = "Agentrouter"
model_reasoning_effort = "high"

[model_providers.Agentrouter]
name = "Agentrouter"
base_url = "http://127.0.0.1:8788/v1"
wire_api = "responses"
env_key = "CODEX_GATEWAY_API_KEY"
```

> [!NOTE]
> Ensure your `CODEX_GATEWAY_API_KEY` (or `AGENTROUTER_API_KEY`) is configured in your environment or in `.codex/.env`.

---

## Health, Diagnostics & Tests

- **Health check**: `http://127.0.0.1:8788/_health`
- **Proxy stats**: `http://127.0.0.1:8788/_stats`
  - Returns counts for total requests, mismatches caught, fallbacks applied, rate limits caught, pre-commit stream retries, and successful recoveries.
- **Logs**: When running as a service (`-Service` or via scheduled task), logs are stored in `logs/proxy.log` (automatically rotates at 1MB).
- **Tests**:
  ```powershell
  .\.venv\Scripts\python.exe -m unittest discover -s tests -v
  ```
