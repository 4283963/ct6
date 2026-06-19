package retry

import (
	"testing"
	"time"
)

func TestBackoff_RespectsMax(t *testing.T) {
	b := NewBackoff(time.Second, 5*time.Second, 2.0, 0)
	for attempt := 0; attempt < 20; attempt++ {
		d := b.Next(attempt)
		if d > 5*time.Second {
			t.Fatalf("attempt %d backoff %s exceeds max 5s", attempt, d)
		}
		if d <= 0 {
			t.Fatalf("attempt %d backoff must be > 0, got %s", attempt, d)
		}
	}
}

func TestBackoff_GrowsWithAttempt(t *testing.T) {
	b := NewBackoff(time.Second, 5*time.Minute, 2.0, 0)
	prev := b.Next(0)
	// With zero jitter, backoff should strictly increase with attempt.
	for attempt := 1; attempt < 8; attempt++ {
		cur := b.Next(attempt)
		if cur <= prev {
			t.Fatalf("expected strictly increasing backoff, attempt %d cur %s prev %s", attempt, cur, prev)
		}
		prev = cur
	}
}

func TestBackoff_JitterWithinBounds(t *testing.T) {
	b := NewBackoff(time.Second, 10*time.Second, 2.0, 0.3)
	raw := float64(time.Second)
	for i := 0; i < 4; i++ {
		raw *= 2
	}
	upperBound := time.Duration(raw * 1.3) // raw + jitter (ratio 0.3)
	for attempt := 0; attempt < 5; attempt++ {
		d := b.Next(attempt)
		if d < 0 {
			t.Fatalf("backoff must be non-negative, got %s", d)
		}
		if d > upperBound+time.Second {
			t.Fatalf("backoff %s exceeds expected jittered upper bound %s", d, upperBound)
		}
	}
}

func TestShouldRetry(t *testing.T) {
	if !ShouldRetry(0, 5) {
		t.Fatal("attempt 0 with max 5 should retry")
	}
	if !ShouldRetry(4, 5) {
		t.Fatal("attempt 4 with max 5 should retry")
	}
	if ShouldRetry(5, 5) {
		t.Fatal("attempt 5 with max 5 should NOT retry")
	}
	if MaxAttempts(5) != 6 {
		t.Fatal("MaxAttempts(5) should be 6")
	}
}
