package kiro

// Prompt cache simulator for Kiro / CodeWhisperer.
//
// Why this exists
// ---------------
// The Kiro CodeWhisperer upstream only honours its own `cachePoint` markers
// (see cache_points.go) and *ignores* Anthropic-style `cache_control.ttl`
// values like `"5m"` / `"1h"` that Claude Code, Cline and Cursor send.
// Worse, several Kiro models do not surface `tokenUsage.cacheReadInputTokens`
// at all, so even when Kiro happens to serve a cache hit the client never
// sees its `cache_control` breakpoints "light up".
//
// PromptCacheTracker simulates Anthropic's prompt-caching accounting in the
// proxy itself:
//
//   - Build a stable fingerprint per cacheable segment (system / tools / a
//     prefix of the messages array).
//   - Remember which fingerprints we have seen for each account, plus when
//     they expire (5m or 1h sliding window).
//   - At response time, if the upstream did *not* report cache reads, we
//     overlay simulated `cache_read_input_tokens` /
//     `cache_creation_input_tokens` on the usage object so clients finally
//     observe the cache-hit signal they expect.
//
// Behaviour rules
// ---------------
//  1. Upstream truth wins. If Kiro returns non-zero cacheReadInputTokens or
//     cacheWriteInputTokens we DO NOT simulate — we trust the upstream value
//     so totals never get double-counted.
//  2. The simulator only writes when the upstream stayed silent.
//  3. Threshold: a segment is cacheable only when its estimated token count
//     reaches the model's minimum (1024 for most models, 4096 for Opus).
//  4. Maximum savings ratio is 85%, mirroring Anthropic's published cap.
//  5. Per-account 200-entry cap, 60s background GC, sliding TTL refresh on
//     hit so multi-turn conversations stay warm.
//  6. Fingerprint is only committed for *successful* completions; partial
//     or aborted streams must not poison the table.
//  7. Concurrency safe via sync.Mutex on a per-account bucket.
//
// This file is self-contained: callers integrate by creating a Profile from
// a payload (BuildProfile), polling Lookup before forwarding, and calling
// Commit on successful completion. The accounting overlay is computed via
// Profile.Apply on the recorded usage.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	// Token thresholds gate which segments are eligible. Anthropic charges
	// the cache write fee only when the segment passes a minimum length;
	// matching that gate prevents the simulator from inflating tiny prompts.
	cacheMinTokensDefault = 1024
	cacheMinTokensOpus    = 4096

	// Maximum simulated savings ratio. Anthropic publishes ~90% as the
	// theoretical ceiling for prompt caching; we cap at 85% to leave a
	// safety margin and stay conservative.
	maxSavingsRatio = 0.85

	// TTLs follow the two ephemeral windows Anthropic exposes via
	// cache_control.ttl. Kiro upstream ignores both, but we honour them in
	// the simulator so 5m/1h breakpoints behave like Anthropic.
	ephemeralTTLShort = 5 * time.Minute
	ephemeralTTLLong  = 1 * time.Hour

	// Per-account cap on remembered fingerprints. Each entry is small, but
	// in long-running deployments without the cap we would slowly leak
	// memory.
	accountFingerprintCap = 200

	// Background GC period — bounded so expired entries are released even
	// when an account stops hitting the gateway.
	cacheGCInterval = 60 * time.Second

	// charsPerToken converts segment byte length into a rough token estimate
	// when the upstream has not reported real input tokens. 3 chars ≈ 1
	// token matches our gateway-wide estimateKiroInputTokens heuristic.
	charsPerToken = 3
)

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

// PromptCacheTracker is the singleton store. Use Default for the
// process-wide instance; tests construct their own with NewTracker so they
// can drive a synthetic clock.
type PromptCacheTracker struct {
	now func() time.Time

	mu       sync.Mutex
	accounts map[int64]*accountCache

	gcOnce sync.Once
	gcStop chan struct{}
}

type accountCache struct {
	mu      sync.Mutex
	entries map[string]time.Time // fingerprint -> expiresAt (sliding)
	order   []string             // FIFO insertion order for cap eviction
}

// Profile represents the cacheable segments derived from a single request.
// It is created once per request via BuildProfile, consulted before
// forwarding (Lookup), and finalised once the response completes (Commit).
type Profile struct {
	tracker   *PromptCacheTracker
	accountID int64
	model     string

	// Segments are ordered from most stable to least stable: system → tools
	// → message prefixes. Order matters because we mark only the longest
	// matched prefix as "read" to avoid double-counting.
	segments []segment

	// totalTokens is the sum of every segment's token estimate; used to
	// compute the simulated read amount and write fallback.
	totalTokens int

	// minTokens is the per-segment token threshold for this model.
	minTokens int

	// hits captures which segments were already present in the account's
	// cache table at Lookup time. Persisted only in memory for the request
	// lifetime; never written back if the request ultimately fails.
	hits []bool
}

// Adjustment is the overlay returned by Profile.Apply. Callers merge it
// into the encoder's usage state when the upstream did not report cache
// fields itself.
type Adjustment struct {
	// InputTokens is the billable input the client should observe (raw
	// minus simulated cache read). Zero means "do not adjust".
	InputTokens int64
	// CacheReadInputTokens is the simulated read count that was previously
	// "cached" from a matching prefix.
	CacheReadInputTokens int64
	// CacheCreationInputTokens is the simulated write count for newly-seen
	// segments that crossed the threshold this turn.
	CacheCreationInputTokens int64
}

// ---------------------------------------------------------------------------
// Singleton + constructor
// ---------------------------------------------------------------------------

var defaultTracker = func() *PromptCacheTracker {
	t := NewTracker(time.Now)
	t.startGC()
	return t
}()

// Default returns the process-wide tracker singleton.
func Default() *PromptCacheTracker { return defaultTracker }

// NewTracker constructs an isolated tracker. Pass a custom clock for tests.
// The returned tracker has no GC goroutine; call startGC if needed.
func NewTracker(clock func() time.Time) *PromptCacheTracker {
	if clock == nil {
		clock = time.Now
	}
	return &PromptCacheTracker{
		now:      clock,
		accounts: make(map[int64]*accountCache),
		gcStop:   make(chan struct{}),
	}
}

// startGC kicks off the periodic eviction goroutine. Idempotent.
func (t *PromptCacheTracker) startGC() {
	t.gcOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(cacheGCInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					t.gc()
				case <-t.gcStop:
					return
				}
			}
		}()
	})
}

// Stop terminates the GC goroutine. Tests can call this to avoid leaks.
// Calling Stop on the default tracker is allowed but uncommon.
func (t *PromptCacheTracker) Stop() { close(t.gcStop) }

// ---------------------------------------------------------------------------
// BuildProfile — request-side analysis
// ---------------------------------------------------------------------------

// BuildProfile derives the cacheable segments from an Anthropic-shaped
// request. It returns nil when the request carries nothing worth caching,
// in which case callers can short-circuit the whole simulator path.
//
// `accountID` scopes the fingerprints so different Kiro accounts never
// share simulated hits — both for billing isolation and because Kiro's
// real prefix cache is account-scoped server-side anyway.
func (t *PromptCacheTracker) BuildProfile(accountID int64, req *AnthropicRequest) *Profile {
	if t == nil || req == nil {
		return nil
	}

	model := req.Model
	minTokens := cacheMinTokensDefault
	if isOpusFamily(model) {
		minTokens = cacheMinTokensOpus
	}

	var segs []segment

	// (1) System prompt — long-lived, mapped to 1h TTL.
	if sysSeg, ok := newSegment("system", req.System, ephemeralTTLLong); ok {
		segs = append(segs, sysSeg)
	}

	// (2) Tools array — also long-lived; agent toolchains rarely change
	// mid-conversation.
	if len(req.Tools) > 0 {
		if toolsSeg, ok := newSegment("tools", req.Tools, ephemeralTTLLong); ok {
			segs = append(segs, toolsSeg)
		}
	}

	// (3) Messages prefix — short-lived (5m). For multi-turn conversations
	// we record the *full* current history as one fingerprint; subsequent
	// turns whose history starts with the same prefix will re-hit the
	// table because the same canonical bytes recur as the head of their
	// own larger prefix. A single fingerprint per request keeps the
	// account table small; matching by exact equality (not subset) is the
	// trade-off and matches kiro2api's design.
	if len(req.Messages) > 1 {
		// Cache at most every prefix of length n-1 down to 1 — but only
		// the deepest one is "interesting" for hit-rate computation. We
		// store one fingerprint covering messages[:n-1] so the next turn
		// (which has messages[:n-1] as its own messages[:n-2]) can hit it.
		prefix := req.Messages[:len(req.Messages)-1]
		if msgSeg, ok := newSegment("messages", prefix, ephemeralTTLShort); ok {
			segs = append(segs, msgSeg)
		}
	}

	if len(segs) == 0 {
		return nil
	}

	// Filter segments below the model's minimum-tokens threshold.
	filtered := segs[:0]
	totalTokens := 0
	for _, s := range segs {
		if s.tokens < minTokens {
			continue
		}
		filtered = append(filtered, s)
		totalTokens += s.tokens
	}
	if len(filtered) == 0 {
		return nil
	}

	return &Profile{
		tracker:     t,
		accountID:   accountID,
		model:       model,
		segments:    filtered,
		totalTokens: totalTokens,
		minTokens:   minTokens,
		hits:        make([]bool, len(filtered)),
	}
}

// Lookup checks every segment fingerprint against the account table. It
// refreshes the expiry of each hit (sliding window). This is read-only
// from the caller's perspective: results only become visible to other
// requests after Commit succeeds.
func (p *Profile) Lookup() {
	if p == nil || p.tracker == nil {
		return
	}
	bucket := p.tracker.bucket(p.accountID)
	now := p.tracker.now()

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	for i, s := range p.segments {
		exp, ok := bucket.entries[s.fingerprint]
		if !ok || !exp.After(now) {
			continue
		}
		p.hits[i] = true
		// Sliding window: refresh expiry to "now + ttl" so heavily-used
		// segments do not silently lapse mid-conversation.
		bucket.entries[s.fingerprint] = now.Add(s.ttl)
	}
}

// Commit records every segment fingerprint that did not already hit. Call
// this only after the upstream completes successfully — otherwise an
// aborted stream would advertise stale fingerprints to the next request.
func (p *Profile) Commit() {
	if p == nil || p.tracker == nil {
		return
	}
	bucket := p.tracker.bucket(p.accountID)
	now := p.tracker.now()

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	for i, s := range p.segments {
		if p.hits[i] {
			continue // already refreshed in Lookup
		}
		// Insert; honour cap with FIFO eviction.
		if _, exists := bucket.entries[s.fingerprint]; !exists {
			if len(bucket.order) >= accountFingerprintCap {
				oldest := bucket.order[0]
				bucket.order = bucket.order[1:]
				delete(bucket.entries, oldest)
			}
			bucket.order = append(bucket.order, s.fingerprint)
		}
		bucket.entries[s.fingerprint] = now.Add(s.ttl)
	}
}

// Apply combines the simulator's view with whatever the upstream reported
// and returns an overlay the caller can merge into its usage tally.
//
// `rawInput` is the upstream-reported input token count (or our local
// estimate when the upstream stayed silent). `upstreamRead` /
// `upstreamWrite` are the cache fields Kiro returned — when either is
// non-zero we defer entirely to the upstream and return a zero overlay
// (no double-counting).
func (p *Profile) Apply(rawInput, upstreamRead, upstreamWrite int64) Adjustment {
	if p == nil {
		return Adjustment{}
	}
	if upstreamRead > 0 || upstreamWrite > 0 {
		return Adjustment{}
	}
	if rawInput <= 0 {
		return Adjustment{}
	}

	var hitTokens, missTokens int
	for i, s := range p.segments {
		if p.hits[i] {
			hitTokens += s.tokens
		} else {
			missTokens += s.tokens
		}
	}
	if hitTokens == 0 && missTokens == 0 {
		return Adjustment{}
	}

	// Cap simulated read at 85% of the raw input to mirror Anthropic's
	// published savings ceiling. Without this, a stale conversation with a
	// large cached prefix could effectively bill 0 input tokens to the
	// client which doesn't match real Anthropic behaviour.
	maxRead := int64(float64(rawInput) * maxSavingsRatio)
	read := int64(hitTokens)
	if read > maxRead {
		read = maxRead
	}
	if read < 0 {
		read = 0
	}

	// Simulated write equals fresh segments that crossed the threshold.
	// This is what Anthropic charges as `cache_creation_input_tokens` when
	// the prefix is being warmed up.
	write := int64(missTokens)
	if write > rawInput {
		write = rawInput
	}
	if write < 0 {
		write = 0
	}

	billable := rawInput - read
	if billable < 0 {
		billable = 0
	}

	return Adjustment{
		InputTokens:              billable,
		CacheReadInputTokens:     read,
		CacheCreationInputTokens: write,
	}
}

// HasSegments reports whether the profile carries any cacheable content.
// Useful as a fast skip on requests that aren't worth simulating.
func (p *Profile) HasSegments() bool { return p != nil && len(p.segments) > 0 }

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// segment is one cacheable unit: a stable fingerprint + estimated tokens +
// per-Anthropic ttl.
type segment struct {
	name        string
	fingerprint string
	tokens      int
	ttl         time.Duration
}

// newSegment canonicalises `value`, computes its SHA-256 fingerprint and
// estimates token count. Returns ok=false when the segment is empty.
func newSegment(name string, value any, ttl time.Duration) (segment, bool) {
	bytes, err := canonicalJSON(value)
	if err != nil || len(bytes) == 0 {
		return segment{}, false
	}
	h := sha256.New()
	h.Write([]byte(name))
	h.Write([]byte{0})
	h.Write(bytes)
	fp := hex.EncodeToString(h.Sum(nil))
	tokens := len(bytes) / charsPerToken
	if tokens <= 0 {
		return segment{}, false
	}
	return segment{
		name:        name,
		fingerprint: fp,
		tokens:      tokens,
		ttl:         ttl,
	}, true
}

// canonicalJSON serialises `v` with map keys sorted at every level so the
// fingerprint is stable across Go map iteration order changes.
func canonicalJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var any any
	if err := json.Unmarshal(raw, &any); err != nil {
		// Non-JSON convertible value; treat as opaque bytes so we still
		// fingerprint deterministically.
		return raw, nil
	}
	return marshalCanonical(any)
}

func marshalCanonical(v any) ([]byte, error) {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var sb strings.Builder
		sb.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				sb.WriteByte(',')
			}
			kb, _ := json.Marshal(k)
			sb.Write(kb)
			sb.WriteByte(':')
			vb, err := marshalCanonical(x[k])
			if err != nil {
				return nil, err
			}
			sb.Write(vb)
		}
		sb.WriteByte('}')
		return []byte(sb.String()), nil
	case []any:
		var sb strings.Builder
		sb.WriteByte('[')
		for i, item := range x {
			if i > 0 {
				sb.WriteByte(',')
			}
			ib, err := marshalCanonical(item)
			if err != nil {
				return nil, err
			}
			sb.Write(ib)
		}
		sb.WriteByte(']')
		return []byte(sb.String()), nil
	default:
		return json.Marshal(x)
	}
}

// bucket returns the per-account cache table, creating it on demand. Holds
// the tracker mutex briefly; per-account work happens on the bucket mutex
// so unrelated accounts never serialise.
func (t *PromptCacheTracker) bucket(accountID int64) *accountCache {
	t.mu.Lock()
	defer t.mu.Unlock()
	if existing, ok := t.accounts[accountID]; ok {
		return existing
	}
	b := &accountCache{
		entries: make(map[string]time.Time),
	}
	t.accounts[accountID] = b
	return b
}

// gc walks every account bucket and drops expired fingerprints.
func (t *PromptCacheTracker) gc() {
	t.mu.Lock()
	buckets := make([]*accountCache, 0, len(t.accounts))
	for _, b := range t.accounts {
		buckets = append(buckets, b)
	}
	t.mu.Unlock()

	now := t.now()
	for _, b := range buckets {
		b.mu.Lock()
		// Iterate insertion order so we keep `order` consistent with
		// `entries`. Walk forward, drop expired, rewrite slice.
		kept := b.order[:0]
		for _, fp := range b.order {
			exp, ok := b.entries[fp]
			if !ok {
				continue
			}
			if !exp.After(now) {
				delete(b.entries, fp)
				continue
			}
			kept = append(kept, fp)
		}
		b.order = kept
		b.mu.Unlock()
	}
}

// isOpusFamily picks the larger 4096-token threshold for Opus-class models
// (which have larger context windows and stricter caching minimums).
func isOpusFamily(model string) bool {
	m := strings.ToLower(model)
	return strings.Contains(m, "opus")
}
