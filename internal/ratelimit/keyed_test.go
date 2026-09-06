package ratelimit

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestKeyedIsolatesKeysAndSweeps(t *testing.T) {
	now := time.Unix(0, 0)
	k := NewKeyed(1, 1, 10)
	k.swept = now
	if !k.Allow("a", now) || k.Allow("a", now) || !k.Allow("b", now) {
		t.Fatal("keys must not share tokens")
	}
	if !k.Allow("a", now.Add(time.Second)) {
		t.Fatal("one token per second")
	}
	k.Allow("c", now.Add(IdleTtl+2*time.Second))
	if _, ok := k.entries["a"]; ok || len(k.entries) != 1 {
		t.Fatalf("idle keys not swept: %d", len(k.entries))
	}
	if !NewKeyed(0, 0, 10).Allow("x", now) {
		t.Fatal("zero rate must allow")
	}
}

func TestKeyedOverflowKeepsSweepSchedule(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 0)
	k := NewKeyed(1, 1, 2)
	k.swept = now
	for _, key := range []string{"cold", "hot"} {
		if !k.Allow(key, now) {
			t.Fatal("initial independent bucket denied")
		}
	}
	beforeSweep := now.Add(IdleTtl - time.Second)
	if !k.Allow("hot", beforeSweep) {
		t.Fatal("tracked bucket did not refill")
	}
	if !k.Allow("overflow-a", beforeSweep) || k.Allow("overflow-b", beforeSweep) {
		t.Fatal("full map must share one overflow bucket")
	}
	if !k.swept.Equal(now) {
		t.Error("overflow scanned early and postponed the scheduled sweep")
	}
	if len(k.entries) != 2 {
		t.Fatal("overflow changed the tracked-key count")
	}
	afterSweep := now.Add(IdleTtl + time.Second)
	if !k.Allow("hot", afterSweep) {
		t.Fatal("tracked bucket must remain independent of overflow")
	}
	if _, ok := k.entries["cold"]; ok || len(k.entries) != 1 {
		t.Errorf("overflow postponed idle cleanup: tracked=%d", len(k.entries))
	}
	if !k.Allow("fresh", afterSweep) {
		t.Fatal("sweep did not free a slot for a new independent bucket")
	}
	if _, ok := k.entries["fresh"]; !ok || len(k.entries) != 2 {
		t.Fatal("fresh key was not tracked after cleanup")
	}
}

func TestKeyedConcurrentOverflow(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 0)
	k := NewKeyed(1, 1, 2)
	k.swept = now
	var accepted atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range 32 {
		wg.Go(func() {
			<-start
			key := strconv.Itoa(i)
			for range 8 {
				if k.Allow(key, now) {
					accepted.Add(1)
				}
			}
		})
	}
	close(start)
	wg.Wait()
	if accepted.Load() != 3 || len(k.entries) != 2 {
		t.Fatalf(
			"want two independent tokens and one overflow token: accepted=%d tracked=%d",
			accepted.Load(),
			len(k.entries),
		)
	}
}

func BenchmarkKeyedOverflow(b *testing.B) {
	for _, capacity := range []int{100, 10000, 100000} {
		b.Run(strconv.Itoa(capacity), func(b *testing.B) {
			now := time.Unix(0, 0)
			k := NewKeyed(1, 1, capacity)
			k.swept = now
			for i := range capacity {
				if !k.Allow(strconv.Itoa(i), now) {
					b.Fatal("could not fill tracked keys")
				}
			}
			if len(k.entries) != capacity {
				b.Fatalf("benchmark requires a full map: %d", len(k.entries))
			}
			keys := []string{"overflow-a", "overflow-b", "overflow-c", "overflow-d"}
			var i int
			b.ReportAllocs()
			for b.Loop() {
				k.Allow(keys[i%len(keys)], now)
				i++
			}
			if len(k.entries) != capacity {
				b.Fatal("overflow changed tracked-key count")
			}
		})
	}
}
