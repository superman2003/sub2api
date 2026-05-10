package service

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Kiro token lifetime in production is 1 hour. We refresh 3 minutes before
// expiry on the hot path, and the background scheduler uses 30 minutes.
const (
	kiroTokenRefreshSkew = 3 * time.Minute
	kiroTokenCacheSkew   = 5 * time.Minute
	// kiroRequestRefreshTimeout bounds how long the request path is willing
	// to block on a token refresh. Past this point we surface the error,
	// park the account temporarily, and let the background refresher retry.
	kiroRequestRefreshTimeout = 10 * time.Second
)

// KiroTokenCache reuses the shared Gemini-style cache interface.
type KiroTokenCache = GeminiTokenCache

// KiroTokenProvider fetches a valid Kiro accessToken for a given account,
// refreshing through the CBOR RefreshToken RPC when necessary. It plugs into
// the project's shared OAuthRefreshAPI / RefreshExecutor infrastructure so
// that background refreshes and hot-path refreshes do not race.
type KiroTokenProvider struct {
	accountRepo      AccountRepository
	tokenCache       KiroTokenCache
	kiroOAuthService *KiroOAuthService
	refreshAPI       *OAuthRefreshAPI
	executor         OAuthRefreshExecutor
	refreshPolicy    ProviderRefreshPolicy
	tempUnschedCache TempUnschedCache
	// accountProbe lets the provider confirm an account is actually dead
	// before stalling it. Refreshes fail for transient reasons all the
	// time (network blip, AWS 5xx, context deadline) — parking the
	// account on every such blip produces the user-reported "no
	// available accounts" even though a manual test still works. When
	// set, markTempUnschedulable runs a live probe first and only
	// parks the account if the probe also fails.
	accountProbe     KiroAccountProbe
	backfillCooldown sync.Map
}

// KiroAccountProbe verifies an account is actually usable at the moment.
// nil error means the account can serve requests right now.
type KiroAccountProbe interface {
	ProbeAccount(ctx context.Context, account *Account) error
}

// NewKiroTokenProvider constructs the provider with sensible defaults.
func NewKiroTokenProvider(
	accountRepo AccountRepository,
	tokenCache KiroTokenCache,
	kiroOAuthService *KiroOAuthService,
) *KiroTokenProvider {
	return &KiroTokenProvider{
		accountRepo:      accountRepo,
		tokenCache:       tokenCache,
		kiroOAuthService: kiroOAuthService,
		refreshPolicy:    KiroProviderRefreshPolicy(),
	}
}

// SetRefreshAPI wires the unified refresh coordinator + executor.
func (p *KiroTokenProvider) SetRefreshAPI(api *OAuthRefreshAPI, executor OAuthRefreshExecutor) {
	p.refreshAPI = api
	p.executor = executor
}

// SetRefreshPolicy overrides the caller-side policy (for tests).
func (p *KiroTokenProvider) SetRefreshPolicy(policy ProviderRefreshPolicy) {
	p.refreshPolicy = policy
}

// SetTempUnschedCache lets hot-path failures immediately remove the account
// from scheduling consideration, without waiting for the next DB sweep.
func (p *KiroTokenProvider) SetTempUnschedCache(cache TempUnschedCache) {
	p.tempUnschedCache = cache
}

// SetAccountProbe wires a live-probe implementation. When set, the
// provider will verify a seemingly-dead account is actually dead before
// flipping it to temp-unschedulable — refreshes fail for transient
// reasons (network blip, AWS 5xx) all the time, and parking the
// account on every blip leads to "no available accounts" errors even
// when the account is usable.
func (p *KiroTokenProvider) SetAccountProbe(probe KiroAccountProbe) {
	p.accountProbe = probe
}

// GetAccessToken returns a Kiro accessToken that is valid for at least
// kiroTokenRefreshSkew more seconds. Blocks if a refresh is required.
func (p *KiroTokenProvider) GetAccessToken(ctx context.Context, account *Account) (string, error) {
	if account == nil {
		return "", errors.New("account is nil")
	}
	if account.Platform != PlatformKiro {
		return "", errors.New("not a kiro account")
	}
	if account.Type != AccountTypeOAuth {
		return "", errors.New("not a kiro oauth account")
	}

	cacheKey := KiroTokenCacheKey(account)

	// Fast path: cache.
	if p.tokenCache != nil {
		if token, err := p.tokenCache.GetAccessToken(ctx, cacheKey); err == nil && strings.TrimSpace(token) != "" {
			return token, nil
		}
	}

	// Refresh path: trigger only when about to expire.
	expiresAt := account.GetCredentialAsTime("expires_at")
	needsRefresh := expiresAt == nil || time.Until(*expiresAt) <= kiroTokenRefreshSkew
	if needsRefresh && p.refreshAPI != nil && p.executor != nil {
		refreshCtx, cancel := context.WithTimeout(ctx, kiroRequestRefreshTimeout)
		defer cancel()
		result, err := p.refreshAPI.RefreshIfNeeded(refreshCtx, account, p.executor, kiroTokenRefreshSkew)
		if err != nil {
			p.markTempUnschedulable(account, err)
			if p.refreshPolicy.OnRefreshError == ProviderRefreshErrorReturn {
				return "", err
			}
		} else if result.LockHeld {
			if p.refreshPolicy.OnLockHeld == ProviderLockHeldWaitForCache && p.tokenCache != nil {
				if token, cacheErr := p.tokenCache.GetAccessToken(ctx, cacheKey); cacheErr == nil && strings.TrimSpace(token) != "" {
					return token, nil
				}
			}
		} else {
			account = result.Account
			expiresAt = account.GetCredentialAsTime("expires_at")
		}
	} else if needsRefresh && p.tokenCache != nil {
		locked, err := p.tokenCache.AcquireRefreshLock(ctx, cacheKey, 30*time.Second)
		if err == nil && locked {
			defer func() { _ = p.tokenCache.ReleaseRefreshLock(ctx, cacheKey) }()
		}
	}

	accessToken := account.GetCredential("access_token")
	if strings.TrimSpace(accessToken) == "" {
		return "", errors.New("access_token not found in credentials")
	}

	// Cache for most of the remaining lifetime (leaving a safety skew).
	if p.tokenCache != nil {
		latestAccount, isStale := CheckTokenVersion(ctx, account, p.accountRepo)
		if isStale && latestAccount != nil {
			slog.Debug("kiro_token_version_stale_use_latest", "account_id", account.ID)
			accessToken = latestAccount.GetCredential("access_token")
			if strings.TrimSpace(accessToken) == "" {
				return "", errors.New("access_token not found after version check")
			}
		} else {
			ttl := 30 * time.Minute
			if expiresAt != nil {
				until := time.Until(*expiresAt)
				switch {
				case until > kiroTokenCacheSkew:
					ttl = until - kiroTokenCacheSkew
				case until > 0:
					ttl = until
				default:
					ttl = time.Minute
				}
			}
			_ = p.tokenCache.SetAccessToken(ctx, cacheKey, accessToken, ttl)
		}
	}

	return accessToken, nil
}

// ProfileArn returns the profileArn embedded in the account credentials.
// generateAssistantResponse requires it in the payload.
func (p *KiroTokenProvider) ProfileArn(account *Account) string {
	if account == nil {
		return ""
	}
	return strings.TrimSpace(account.GetCredential("profile_arn"))
}

// markTempUnschedulable 在这个 fork 里是 no-op。
//
// 硬约束：Kiro 账号永远不挂 temp-unschedulable。不管请求路径上的
// 刷新错误是什么（AWS 5xx、network blip、context deadline、invalid_grant
// 等），都不再把账号打脏；让请求自然失败或切到下一个账号。
//
// 保留函数签名以兼容既有调用点（`GetAccessToken` 出错路径），只记一条
// 日志便于排查，其他所有副作用（DB 写、Redis 缓存写）都跳过。
func (p *KiroTokenProvider) markTempUnschedulable(account *Account, refreshErr error) {
	if account == nil {
		return
	}
	slog.Info("kiro_token_provider.park_suppressed",
		"account_id", account.ID,
		"refresh_error", func() string {
			if refreshErr == nil {
				return ""
			}
			return refreshErr.Error()
		}(),
	)
}

// KiroTokenCacheKey builds the Redis cache key for the account token.
func KiroTokenCacheKey(account *Account) string {
	if account == nil {
		return "kiro:token:0"
	}
	return "kiro:token:" + strconv.FormatInt(account.ID, 10)
}
