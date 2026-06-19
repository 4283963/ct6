package retry

import (
	"math/rand/v2"
	"sync"
	"time"
)

// Backoff 失败重试退避算法配置。
// 采用“指数退避 + 比例抖动（Equal Jitter 变体）”：
//
//	raw    = base * multiplier^attempt
//	raw    = min(raw, max)
//	jitter = raw * jitterRatio
//	next   = clamp(raw - jitter + U(0, 2*jitter), 0, +inf)
//
// 抖动可避免多个失败任务在同一时刻集中重试（thundering herd）。
type Backoff struct {
	base        time.Duration
	max         time.Duration
	multiplier  float64
	jitterRatio float64

	mu  sync.Mutex
	rng *rand.Rand
}

// NewBackoff 构造退避器。
func NewBackoff(base, max time.Duration, multiplier, jitterRatio float64) *Backoff {
	if multiplier <= 1 {
		multiplier = 2
	}
	if jitterRatio < 0 {
		jitterRatio = 0
	}
	if jitterRatio > 1 {
		jitterRatio = 1
	}
	return &Backoff{
		base:        base,
		max:         max,
		multiplier:  multiplier,
		jitterRatio: jitterRatio,
		rng:         rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano())^0x9E3779B97F4A7C15)),
	}
}

// Next 返回第 attempt 次失败后的下一次重试等待时长（attempt 从 0 起）。
func (b *Backoff) Next(attempt int) time.Duration {
	raw := float64(b.base)
	for i := 0; i < attempt && raw > 0; i++ {
		raw *= b.multiplier
		if raw > float64(b.max) {
			raw = float64(b.max)
			break
		}
	}
	if raw > float64(b.max) {
		raw = float64(b.max)
	}
	if raw < 0 {
		raw = 0
	}

	jitter := raw * b.jitterRatio
	b.mu.Lock()
	delta := b.rng.Float64() * 2 * jitter
	b.mu.Unlock()

	next := raw - jitter + delta
	if next < 0 {
		next = 0
	}
	return time.Duration(next)
}

// ShouldRetry 判断在当前 attempt 后是否仍可重试。
// attempt 为已尝试次数（含本次失败），maxRetries 为最大重试次数。
func ShouldRetry(attempt, maxRetries int) bool {
	return attempt < maxRetries
}

// MaxAttempts 返回总尝试次数（含首次），用于与 attempt 比较。
func MaxAttempts(maxRetries int) int {
	return maxRetries + 1
}
