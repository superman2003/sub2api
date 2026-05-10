package service

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// KiroHealthCheckService 独立的 Kiro 账号健康检查 + 状态重置循环。
//
// 和 TokenRefreshService 解耦：
//   - TokenRefreshService 按 token 过期时间判断是否需要刷新，间隔通常 5 分钟。
//   - 这个服务只做 probe + 批量重置状态，间隔默认 60 秒，节奏可独立调优。
//
// 每轮做的事（对每个 active 的 Kiro OAuth 账号）：
//  1. 跑一次 probe（最小 /generateAssistantResponse 请求）
//  2. Probe 成功 → 调 RateLimitService.RecoverAccountState，把账号身上的
//     error / rate_limit / overload / temp_unschedulable / model_rate_limit /
//     antigravity_quota 全部清掉。这和前端"批量重置状态"按钮是同一套路径。
//  3. 顺带同步 Redis temp-unsched 缓存和调度器快照。
//  4. Probe 失败 → 不动，下一轮再探。
//
// 这样用户在前端看到的"自动刷新"虽然只是在拉列表，但服务端有这个独立
// 循环兜底做健康检查 + 自动重置，不用手动点按钮。
type KiroHealthCheckService struct {
	accountRepo      AccountRepository
	accountProbe     KiroAccountProbe
	recovery         AccountStateRecoverer
	schedulerCache   SchedulerCache
	tempUnschedCache TempUnschedCache

	interval time.Duration
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewKiroHealthCheckService 创建一个默认 30s 间隔的健康检查服务。
func NewKiroHealthCheckService(
	accountRepo AccountRepository,
	probe KiroAccountProbe,
	recovery AccountStateRecoverer,
	schedulerCache SchedulerCache,
	tempUnschedCache TempUnschedCache,
) *KiroHealthCheckService {
	return &KiroHealthCheckService{
		accountRepo:      accountRepo,
		accountProbe:     probe,
		recovery:         recovery,
		schedulerCache:   schedulerCache,
		tempUnschedCache: tempUnschedCache,
		interval:         30 * time.Second,
		stopCh:           make(chan struct{}),
	}
}

// SetInterval 调整健康检查间隔（注意：别设得太快，probe 会打到 Kiro 上游）。
func (s *KiroHealthCheckService) SetInterval(d time.Duration) {
	if d < 10*time.Second {
		d = 10 * time.Second
	}
	s.interval = d
}

// Start 启动后台循环；启动时立即跑一次。
func (s *KiroHealthCheckService) Start() {
	if s == nil || s.accountRepo == nil || s.accountProbe == nil {
		slog.Info("kiro_health_check.disabled_missing_deps")
		return
	}
	s.wg.Add(1)
	go s.loop()
	slog.Info("kiro_health_check.started", "interval", s.interval.String())
}

// Stop 停止循环（可安全多次调用）。
func (s *KiroHealthCheckService) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
	s.wg.Wait()
	slog.Info("kiro_health_check.stopped")
}

func (s *KiroHealthCheckService) loop() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	// 启动后马上跑一次，不等第一个 tick。
	s.runCycle()

	for {
		select {
		case <-ticker.C:
			s.runCycle()
		case <-s.stopCh:
			return
		}
	}
}

func (s *KiroHealthCheckService) runCycle() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	accounts, err := s.accountRepo.ListByPlatform(ctx, PlatformKiro)
	if err != nil {
		slog.Warn("kiro_health_check.list_failed", "error", err)
		return
	}

	var probed, revived, probeFailed int
	for i := range accounts {
		account := &accounts[i]
		if account.Type != AccountTypeOAuth {
			continue
		}
		// 只处理 active 状态的账号；error 状态的保持现状（但由于硬写规则
		// Kiro 账号从来不会被写成 error，除非用户手动操作）。
		if account.Status != StatusActive {
			continue
		}

		probed++
		if s.tryReviveOne(ctx, account) {
			revived++
		} else {
			probeFailed++
		}
	}

	if revived > 0 || probeFailed > 0 {
		slog.Info("kiro_health_check.cycle_completed",
			"total", len(accounts),
			"probed", probed,
			"revived", revived,
			"probe_failed", probeFailed,
		)
	} else {
		slog.Debug("kiro_health_check.cycle_completed",
			"total", len(accounts),
			"probed", probed,
		)
	}
}

// tryReviveOne probe 一个账号并在成功时清状态。返回 true 表示 probe 成功。
func (s *KiroHealthCheckService) tryReviveOne(ctx context.Context, account *Account) bool {
	probeCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := s.accountProbe.ProbeAccount(probeCtx, account); err != nil {
		slog.Debug("kiro_health_check.probe_failed",
			"account_id", account.ID,
			"error", err,
		)
		return false
	}

	// Probe 成功 → 按"批量重置状态"的清理路径走一次。
	if s.recovery != nil {
		result, err := s.recovery.RecoverAccountState(ctx, account.ID, AccountRecoveryOptions{})
		if err != nil {
			slog.Warn("kiro_health_check.recover_state_failed",
				"account_id", account.ID,
				"error", err,
			)
		} else if result != nil && (result.ClearedError || result.ClearedRateLimit) {
			slog.Info("kiro_health_check.recovered_state",
				"account_id", account.ID,
				"cleared_error", result.ClearedError,
				"cleared_rate_limit", result.ClearedRateLimit,
			)
		}
	}

	// 同步 Redis 的 temp-unsched 缓存（兜底，Recovery 已清 DB）。
	if s.tempUnschedCache != nil {
		if err := s.tempUnschedCache.DeleteTempUnsched(ctx, account.ID); err != nil {
			slog.Debug("kiro_health_check.temp_unsched_cache_clear_failed",
				"account_id", account.ID,
				"error", err,
			)
		}
	}

	// 同步调度器缓存（拿 DB 最新快照，避免用陈旧 account 覆盖）。
	if s.schedulerCache != nil {
		if latest, err := s.accountRepo.GetByID(ctx, account.ID); err == nil && latest != nil {
			if err := s.schedulerCache.SetAccount(ctx, latest); err != nil {
				slog.Debug("kiro_health_check.scheduler_cache_sync_failed",
					"account_id", account.ID,
					"error", err,
				)
			}
		}
	}

	return true
}
