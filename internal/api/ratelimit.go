package api

import (
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	defaultRatePerSecond = 20.0
	defaultBurst         = 40.0
)

// tokenBucket is a mutex-guarded token bucket: tokens refill continuously at
// rate per second, up to burst, and each allow() call consumes one.
type tokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	lastRefill time.Time
	rate       float64
	burst      float64
}

func newTokenBucket(rate, burst float64) *tokenBucket {
	return &tokenBucket{tokens: burst, lastRefill: time.Now(), rate: rate, burst: burst}
}

func (b *tokenBucket) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(b.lastRefill).Seconds()
	b.lastRefill = now
	b.tokens = math.Min(b.burst, b.tokens+elapsed*b.rate)
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// rateLimiter hands out one tokenBucket per identity, created lazily on
// first use.
type rateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
	rate    float64
	burst   float64
}

func newRateLimiter(rate, burst float64) *rateLimiter {
	return &rateLimiter{buckets: make(map[string]*tokenBucket), rate: rate, burst: burst}
}

func (rl *rateLimiter) allow(identity string) bool {
	rl.mu.Lock()
	b, ok := rl.buckets[identity]
	if !ok {
		b = newTokenBucket(rl.rate, rl.burst)
		rl.buckets[identity] = b
	}
	rl.mu.Unlock()
	return b.allow()
}

// allowRate checks identity's budget and, if exhausted, writes a 429 with a
// Retry-After hint and returns false. Callers (requireAdmin,
// requireTenantAccess) call this only after successfully authenticating the
// request, so identity is a proven credential, never attacker-controlled.
func (a *API) allowRate(w http.ResponseWriter, identity string) bool {
	if a.limiter.allow(identity) {
		return true
	}
	retryAfter := int(math.Ceil(1 / a.limiter.rate))
	w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
	return false
}
