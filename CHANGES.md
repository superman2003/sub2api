# Changes from upstream (Wei-Shaw/sub2api)

This fork adds **Kiro platform support** (Amazon Q Developer / CodeWhisperer) and fixes several issues in the original codebase.

## New Features

### Kiro Platform Integration
- **Full Kiro account lifecycle**: OAuth (Google social login), AWS Builder ID (OIDC Device Grant), and manual token paste
- **AWS Builder ID login** (recommended): One-click device authorization flow — no browser callback URL copying needed
- **Google OAuth login**: Cognito-based flow with automatic CSRF token persistence for reliable token refresh
- **Kiro gateway service**: Forwards Anthropic-style `/v1/messages` requests to CodeWhisperer's `generateAssistantResponse` endpoint
- **Kiro token refresh**: Three auto-selecting paths — OIDC (Builder ID), Desktop Auth (social login, no CSRF), Legacy CBOR (fallback)
- **Kiro model mapping**: Translates Anthropic model IDs (e.g. `claude-opus-4-7`) to Kiro internal IDs (`claude-opus-4.7`)
- **Kiro credit billing**: Converts upstream `meteringEvent` credits to USD ($0.04/credit) for accurate cost tracking
- **Kiro input token estimation**: Approximates prompt tokens from request body character count when upstream doesn't report them
- **Kiro tool use support**: Correctly handles `toolUseEvent` with both string and object `input` formats
- **Kiro account testing**: Dedicated test path that routes through CodeWhisperer instead of Anthropic API
- **Kiro platform colors**: Full indigo color scheme in UI (badges, buttons, gradients)

### Frontend Improvements
- **Kiro integrated into "Add Account" modal**: No separate wizard button — select Kiro platform tab directly
- **Three login tabs**: AWS Builder ID (default) / Google OAuth / Paste Tokens
- **Builder ID auto-polling**: User code displayed, browser auto-opens, polling completes automatically
- **Platform badge fix**: Kiro accounts now display "Kiro" instead of defaulting to "Gemini"

## Bug Fixes

### Kiro WebSearch Unblocking (Claude Code → Kiro "Invalid tool parameters")
- `AnthropicTool` now preserves the `type` field so server-side Anthropic tools
  (`web_search_20250305`, `computer_20250124`, `text_editor_20250124`, ...)
  can be detected and handled before being forwarded to CodeWhisperer.
  Previously the missing discriminator caused Kiro upstream to reject every
  Claude Code session with "Invalid tool parameters" because the CLI ships
  a `WebSearch` server-side tool by default.
- **`BuildKiroPayload` now rewrites** any Anthropic server-side
  `web_search_*` tool into a plain function tool (`name: "web_search"`,
  with a synthesised JSON Schema) so the Kiro model can actually invoke it.
- Other server-side tools (`computer_*`, `text_editor_*`, `bash_*`, ...) are
  still dropped because Kiro CodeWhisperer has no equivalent.
- If every submitted tool is dropped, the whole `tools` array is omitted
  instead of sent as `[]`.

### Kiro MCP Result Caching (Redis, 15 min)
- Added a small `kiroMCPResultCache` helper that stores Kiro /mcp
  web_search responses in Redis for 15 minutes, keyed by
  `kiro:mcp:ws:<account_id>:<sha256(query)>`. Results are account-scoped
  so different Kiro accounts don't share cached hits; query normalisation
  (lowercase + trim) deduplicates trivially different spellings.
- `KiroGatewayService` gains a `SetWebSearchMCPCache(*redis.Client)`
  setter wired up in `wire_gen.go` alongside the existing web-search deps.
  When Redis isn't configured the cache is a silent no-op.
- Saves a full Kiro credit round-trip on repeat searches during the same
  session (e.g. re-asking the same question after a restart).
- Tests: unit coverage for nil-safety, key stability, and account scoping.

### Kiro Thinking / Extended Reasoning (best-effort)
- Kiro CodeWhisperer has no native Anthropic thinking API, so the gateway
  now implements a "fake reasoning" path modelled after jwadow/kiro-gateway:
  - **Request side**: when the client sends a `thinking` field on
    `/v1/messages`, the request transformer prepends a
    `<thinking_mode>enabled</thinking_mode><max_thinking_length>N</max_thinking_length>…`
    directive to the current user turn, asking the model to wrap its
    reasoning in `<thinking>...</thinking>` tags.
  - **Response side**: a new `ThinkingSplitter` processes the streaming
    assistant text, pulling out any `<thinking>...</thinking>` blocks
    across chunk boundaries and emitting them as Anthropic
    `thinking_delta` events. Remaining text streams out as normal
    `text_delta`. The raw `<thinking>` tags never reach the client.
- `AnthropicSSEEncoder` now tracks thinking/text/tool blocks as mutually
  exclusive, opening/closing content blocks correctly as the event
  sequence transitions between them.
- Tests: `thinking_splitter_test.go` covers chunk boundaries, multiple
  thinking blocks in one stream, unterminated thinking on EOF, stray
  `<` characters, and in-feed coalescing; a new end-to-end test in
  `interceptor_test.go` verifies the full stream path rewrites tags
  into proper Anthropic SSE frames.
- Added `kiro.CallMCPWebSearch` which performs a JSON-RPC 2.0 `tools/call`
  against Kiro's own `/mcp` endpoint
  (`https://q.<region>.amazonaws.com/mcp`), using the caller's existing
  Kiro bearer token. Region is parsed from the account's `profileArn`,
  falling back to `us-east-1`. Correctly treats `"error": null` (which
  Kiro returns on every successful call) as success.
- Added `DriveEventStreamToAnthropicWithInterceptor` so the response
  transformer can hook into a tool_use lifecycle. The driver normalises
  Kiro's wire quirk where every `toolUseEvent` frame re-carries the tool
  name (treated as tool_use_start on the first frame, tool_use_delta
  afterwards) and flushes any pending interceptor lifecycle on EOF even
  without an explicit stop frame.
- `kiroWebSearchInterceptor` catches both `web_search` (lowercase,
  injected by the request transformer from Anthropic server-side entries)
  and `WebSearch` (the function-shape tool Claude Code CLI ships by
  default). On match it:
    1. Calls Kiro `/mcp` with the account's bearer token.
    2. Launches a **second** `/generateAssistantResponse` turn whose
       history includes a synthetic `tool_use` + `tool_result` pair
       carrying the MCP output.
    3. Streams the model's natural-language summary back to the client
       as regular assistant text.
  The original `tool_use` SSE never reaches the client, so Claude Code
  sees WebSearch behave like a proper server-side tool. Falls back to the
  raw `FormatSearchSummary` output when the follow-up turn fails.
- End result: Claude Code's WebSearch now works out of the box against a
  Kiro account. No third-party API key, no Brave/Tavily setup, no settings
  to flip. The model writes a summary of the search results instead of
  dumping the raw list at the user.
- Opt-in debug: set `SUB2API_KIRO_DEBUG_DUMP=1` to dump every incoming
  Kiro request body to `%TEMP%/sub2api-kiro-dumps/`. Useful when tracing
  new client-side tool shapes.
- Tests: unit coverage for MCP JSON-RPC encoding/decoding (including the
  `"error": null` case), region extraction, summary formatting, query
  extraction from partial JSON, and three end-to-end scenarios on the
  stream driver — basic interception, multi-frame start accumulation,
  and EOF-without-stop flush.

### Kiro WebSearch Emulation (third-party provider path)
- `GetWebSearchEmulationMode` now allows **Kiro** accounts to participate in
  the three-state feature flag (`enabled` / `disabled` / `default`), not only
  Anthropic API Key accounts. `supportsWebSearchEmulation` remains a strict
  allowlist so OpenAI / Gemini / Antigravity are unaffected.
- `KiroGatewayService.Forward` intercepts web_search-only requests and routes
  them through the existing `websearch.Manager` (Brave / Tavily providers),
  replacing the upstream call with a locally-synthesised Anthropic SSE
  response. Mirrors the long-standing behaviour of the Anthropic path.
- The Anthropic and Kiro flows share a single decision function
  (`evaluateWebSearchEmulation`) and a single response builder
  (`executeWebSearchEmulation`) via a minimal dependency interface. This
  avoids a wire cycle: `KiroGatewayService` gets its `ChannelService`
  reference back-filled in `wire_gen.go` via a setter after both services
  exist.
- UI: Channel-edit page and account-edit modal now expose the web_search
  emulation toggle for Kiro accounts in addition to Anthropic API Key ones.
- Added `kiro_websearch_test.go` + updates to `request_transformer_test.go`.

### Group Creation (400 Bad Request)
- Fixed `v-model.number` empty input producing `""` or `NaN` — now normalized to `null` or default values before sending to backend
- Fixed `optionalLimitField.ToServiceInput()` treating `null` as "set to 0" instead of "unlimited"
- Fixed frontend error display using `error.response?.data?.detail` (wrong path after apiClient interceptor unwraps envelope) — now uses `error.message`

### Redis Lua Scripts (429 Concurrency Errors)
- Removed `redis.call('TIME')` from all Lua scripts (caused "Write commands not allowed after non deterministic commands" on Redis 7+)
- Now passes `time.Now().Unix()` from Go as script argument — fully deterministic, works on all Redis versions

### Kiro OAuth CSRF Fix
- `ExchangeCode` now persists `csrf_token` into account credentials after initial token exchange
- Social login refresh now uses `prod.us-east-1.auth.desktop.kiro.dev/refreshToken` (no CSRF needed) instead of app.kiro.dev CBOR RPC

## Files Added
- `internal/pkg/kiro/desktop_auth.go` — Social login refresh endpoint (no CSRF)
- `internal/pkg/kiro/oidc.go` — AWS OIDC Device Grant (register/authorize/poll/refresh)

## Files Modified
- `internal/pkg/kiro/client.go` — Added `CSRFToken()` getter, `EnsureCSRFToken()` export
- `internal/pkg/kiro/request_transformer.go` — Added Opus 4.6/4.7, Sonnet 4.6 model mappings
- `internal/pkg/kiro/response_transformer.go` — Fixed tool input parsing, added usageEvent/messageMetadataEvent handling
- `internal/service/kiro_token_refresher.go` — Auto-selects OIDC / Desktop Auth / Legacy CBOR refresh path
- `internal/service/kiro_oauth_service.go` — Builder ID session store, CSRF backfill in ExchangeCode
- `internal/service/kiro_gateway_service.go` — Input token estimation, credit→USD billing
- `internal/service/account_test_service.go` — Kiro-specific test connection via CodeWhisperer
- `internal/service/gateway_service.go` — Kiro credit billing ($0.04/credit conversion)
- `internal/handler/admin/kiro_oauth_handler.go` — Builder ID start/poll/create-account endpoints
- `internal/handler/admin/group_handler.go` — Fixed `optionalLimitField` null semantics
- `internal/repository/concurrency_cache.go` — Removed TIME from Lua, pass now as ARGV
- `internal/repository/session_limit_cache.go` — Same Redis TIME fix
- `internal/repository/user_msg_queue_cache.go` — Same Redis TIME fix
- `frontend/src/components/account/KiroAccountWizard.vue` — Builder ID tab, embedded mode, fixed data unwrapping
- `frontend/src/components/account/CreateAccountModal.vue` — Kiro platform integration
- `frontend/src/views/admin/AccountsView.vue` — Removed standalone Kiro button
- `frontend/src/views/admin/GroupsView.vue` — Fixed empty field handling for group creation
- `frontend/src/utils/platformColors.ts` — Added Kiro (indigo) to all color maps
- `frontend/src/components/common/PlatformTypeBadge.vue` — Added Kiro label

## License

This project is licensed under LGPL-3.0, same as the upstream project.
Original work Copyright (C) Wei-Shaw.
Modifications Copyright (C) 2026 superman2003.
