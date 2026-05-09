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
