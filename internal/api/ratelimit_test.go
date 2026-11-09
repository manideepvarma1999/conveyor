package api

import (
	"testing"
	"time"
)

func TestLimiterRefillsOverTime(t *testing.T) {
	l := newLimiter(2, 3) // 2/s, burst 3
	now := time.Unix(0, 0)
	for i := range 3 {
		if !l.allow("k", now) {
			t.Fatalf("request %d within burst was rejected", i)
		}
	}
	if l.allow("k", now) {
		t.Fatal("request beyond burst was allowed")
	}
	if !l.allow("other", now) {
		t.Fatal("buckets must be per key")
	}
	if !l.allow("k", now.Add(500*time.Millisecond)) {
		t.Fatal("one token should refill after 0.5s at 2/s")
	}
	if l.allow("k", now.Add(500*time.Millisecond)) {
		t.Fatal("only one token should have refilled")
	}
}
