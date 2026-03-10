package tenant

import (
	"sync"
	"time"
)

// RateLimiter implements a token bucket rate limiter per tenant.
type RateLimiter struct {
	mu       sync.Mutex
	rate     float64   // tokens per second
	tokens   float64   // current available tokens
	capacity float64   // maximum burst capacity
	lastTime time.Time // last refill time
}

// NewRateLimiter creates a new token bucket rate limiter.
// A rate of 0 means unlimited (no rate limiting).
func NewRateLimiter(rate float64) *RateLimiter {
	return &RateLimiter{
		rate:     rate,
		tokens:   rate, // start with a full bucket
		capacity: rate, // burst capacity equals rate
		lastTime: time.Now(),
	}
}

// Allow checks if a request is allowed under the rate limit.
// Returns nil if allowed, ErrRateLimitExceeded otherwise.
func (rl *RateLimiter) Allow() error {
	if rl.rate <= 0 {
		return nil // unlimited
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(rl.lastTime).Seconds()
	rl.lastTime = now

	// Refill tokens.
	rl.tokens += elapsed * rl.rate
	if rl.tokens > rl.capacity {
		rl.tokens = rl.capacity
	}

	if rl.tokens < 1.0 {
		return ErrRateLimitExceeded
	}

	rl.tokens--
	return nil
}

// AllowN checks if n requests are allowed under the rate limit.
// Returns nil if allowed, ErrRateLimitExceeded otherwise.
func (rl *RateLimiter) AllowN(n int) error {
	if rl.rate <= 0 {
		return nil
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(rl.lastTime).Seconds()
	rl.lastTime = now

	rl.tokens += elapsed * rl.rate
	if rl.tokens > rl.capacity {
		rl.tokens = rl.capacity
	}

	needed := float64(n)
	if rl.tokens < needed {
		return ErrRateLimitExceeded
	}

	rl.tokens -= needed
	return nil
}
