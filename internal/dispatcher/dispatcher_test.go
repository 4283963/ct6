package dispatcher

import (
	"testing"

	"ct6/internal/model"
	"ct6/pkg/httpclient"
)

// 关键不变量：投递给消费端的 X-Idempotency-Key 必须是稳定的 task_key，
// 这样“重试”与“崩溃重投”对消费端都呈现为同一把幂等键，从而杜绝重复消费。
func TestBuildHeaders_StableIdempotencyKey(t *testing.T) {
	d := &Dispatcher{}
	task := model.Task{TaskKey: "order-123", Headers: `{"X-Tenant":"acme"}`}
	h := d.buildHeaders(task, "order-123#1", 1)

	if got := h[httpclient.HeaderIdempotencyKey]; got != "order-123" {
		t.Fatalf("idempotency key must be stable task_key, got %q", got)
	}
	if got := h[httpclient.HeaderDeliveryToken]; got != "order-123#1" {
		t.Fatalf("delivery token mismatch: %q", got)
	}
	if got := h[httpclient.HeaderTaskKey]; got != "order-123" {
		t.Fatalf("task key header mismatch: %q", got)
	}
	if got := h[httpclient.HeaderAttempt]; got != "1" {
		t.Fatalf("attempt header mismatch: %q", got)
	}
	if got := h["X-Tenant"]; got != "acme" {
		t.Fatalf("user header should be preserved, got %q", got)
	}
}

// 安全不变量：用户自定义 headers 不得覆盖系统注入的幂等性 headers。
func TestBuildHeaders_SystemHeadersNotOverridable(t *testing.T) {
	d := &Dispatcher{}
	task := model.Task{TaskKey: "legit", Headers: `{"X-Idempotency-Key":"evil","X-Task-Key":"evil","X-Attempt":"999"}`}
	h := d.buildHeaders(task, "legit#3", 3)

	if got := h[httpclient.HeaderIdempotencyKey]; got != "legit" {
		t.Fatalf("user must not override system idempotency key, got %q", got)
	}
	if got := h[httpclient.HeaderAttempt]; got != "3" {
		t.Fatalf("user must not override system attempt header, got %q", got)
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("abc", 10); got != "abc" {
		t.Fatalf("unexpected truncation: %q", got)
	}
	if got := truncate("abcdefghij", 3); got != "abc...(truncated)" {
		t.Fatalf("unexpected truncation: %q", got)
	}
}
