package dispatcher

import (
	"testing"
	"time"
)

func TestCircuitBreaker_Basic(t *testing.T) {
	b := newCircuitBreaker(3, 10*time.Second, time.Hour)
	u := "http://host1.example.com/x"

	for i := 0; i < 3; i++ {
		if !b.Allow(u) {
			t.Fatalf("should allow before threshold, iter %d", i)
		}
		b.Record(u, false)
	}
	// 第 4 次 Allow 应当被拒绝
	if b.Allow(u) {
		t.Fatal("should reject after 3 consecutive failures")
	}
	if !b.IsOpen(u) {
		t.Fatal("breaker should be open")
	}

	// 其他 host 不受影响
	if !b.Allow("http://other.example.com/") {
		t.Fatal("other host should not be affected")
	}
}

func TestCircuitBreaker_SuccessClearsState(t *testing.T) {
	b := newCircuitBreaker(3, 10*time.Second, time.Hour)
	u := "http://h.example.com"
	b.Record(u, false)
	b.Record(u, false)
	b.Record(u, true) // 一次成功清零
	b.Record(u, false)
	b.Record(u, false)
	if !b.Allow(u) {
		t.Fatal("should still allow: success resets counter")
	}
}

func TestCircuitBreaker_HalfOpenAfterCooldown(t *testing.T) {
	b := newCircuitBreaker(2, 10*time.Second, 5*time.Millisecond)
	u := "http://h.example.com"
	b.Record(u, false)
	b.Record(u, false)
	if b.Allow(u) {
		t.Fatal("should be open immediately")
	}
	time.Sleep(20 * time.Millisecond)
	// 冷却结束，进入 half-open，允许一个探测请求
	if !b.Allow(u) {
		t.Fatal("should half-open after cooldown")
	}
}

func TestExtractHost(t *testing.T) {
	if got := extractHost("https://a.b:8080/path"); got != "a.b:8080" {
		t.Fatalf("unexpected host %q", got)
	}
	if got := extractHost("not-a-url"); got != "" {
		t.Fatalf("bad url should yield empty host, got %q", got)
	}
}
