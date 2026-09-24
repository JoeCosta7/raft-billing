package api

import (
	"testing"
	"time"
)

func TestTokenBucket_AllowsUpToBurstThenRejects(t *testing.T) {
	b := newTokenBucket(1, 3)
	for i := range 3 {
		if !b.allow() {
			t.Fatalf("call %d: got false, want true (within burst)", i)
		}
	}
	if b.allow() {
		t.Fatal("call 4: got true, want false (burst exhausted)")
	}
}

func TestTokenBucket_RefillsOverTime(t *testing.T) {
	// A fast rate keeps this test from needing a real multi-second sleep.
	b := newTokenBucket(1000, 1)
	if !b.allow() {
		t.Fatal("first call: got false, want true")
	}
	if b.allow() {
		t.Fatal("immediate second call: got true, want false (burst=1, no time elapsed)")
	}
	time.Sleep(5 * time.Millisecond)
	if !b.allow() {
		t.Fatal("after refill wait: got false, want true")
	}
}

func TestRateLimiter_IdentitiesAreIndependent(t *testing.T) {
	rl := newRateLimiter(1, 1)
	if !rl.allow("tenant-a") {
		t.Fatal("tenant-a first call: got false, want true")
	}
	if rl.allow("tenant-a") {
		t.Fatal("tenant-a second call: got true, want false (budget exhausted)")
	}
	if !rl.allow("tenant-b") {
		t.Fatal("tenant-b first call: got false, want true (independent budget from tenant-a)")
	}
}
