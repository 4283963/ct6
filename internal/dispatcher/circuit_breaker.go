package dispatcher

import (
	"net/url"
	"sync"
	"time"
)

// circuitBreaker 基于 host 的简易滑窗熔断器。
// 不追求高精度（任务调度对时间精度不敏感），以互斥保护的计数桶实现。
//
// 核心语义：
//   - Open（熔断）：近 window 内连续失败数 >= threshold → 所有请求快速失败
//   - Closed（闭合）：正常放行
//   - 熔断会在 breakerCooldown 后自动回到 Closed，允许少量探测请求重新打开
type circuitBreaker struct {
	mu        sync.Mutex
	hosts     map[string]*hostState
	threshold int
	window    time.Duration
	cooldown  time.Duration
}

type hostState struct {
	failures []time.Time
	openedAt time.Time
	isOpen   bool
}

func newCircuitBreaker(threshold int, window, cooldown time.Duration) *circuitBreaker {
	return &circuitBreaker{
		hosts:     make(map[string]*hostState),
		threshold: threshold,
		window:    window,
		cooldown:  cooldown,
	}
}

// Allow 判断该 host 当前是否允许请求。
// 返回 false 表示熔断器打开，应快速失败。
func (b *circuitBreaker) Allow(rawURL string) bool {
	host := extractHost(rawURL)
	if host == "" {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.hosts[host]
	if s == nil {
		return true
	}
	// 自动半开：冷却结束后允许一个探测请求
	if s.isOpen && time.Since(s.openedAt) > b.cooldown {
		s.isOpen = false
		s.failures = s.failures[:0]
		return true
	}
	return !s.isOpen
}

// Record 记录一次成功/失败。
// 失败时：滑窗内失败数 >= threshold → 熔断
// 成功时：清空失败计数
func (b *circuitBreaker) Record(rawURL string, success bool) {
	host := extractHost(rawURL)
	if host == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.hosts[host]
	if s == nil {
		s = &hostState{}
		b.hosts[host] = s
	}
	now := time.Now()
	// 淘汰窗口外的旧记录
	cutoff := now.Add(-b.window)
	i := 0
	for ; i < len(s.failures) && s.failures[i].Before(cutoff); i++ {
	}
	s.failures = append(s.failures[:0], s.failures[i:]...)

	if success {
		// 成功即清零：代表下游恢复
		s.failures = s.failures[:0]
		s.isOpen = false
		return
	}
	s.failures = append(s.failures, now)
	if len(s.failures) >= b.threshold {
		s.isOpen = true
		s.openedAt = now
	}
}

// IsOpen 仅用于观测/测试。
func (b *circuitBreaker) IsOpen(rawURL string) bool {
	host := extractHost(rawURL)
	if host == "" {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.hosts[host]
	if s == nil {
		return false
	}
	return s.isOpen && time.Since(s.openedAt) <= b.cooldown
}

func extractHost(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Host
}
