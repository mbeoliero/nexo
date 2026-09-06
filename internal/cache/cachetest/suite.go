// Shared suite; each implementation's _test.go runs it.
package cachetest

import (
	"context"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/mbeoliero/nexo/internal/cache"
)

func Run(t *testing.T, c cache.Cache) {
	ctx := t.Context()
	p := cache.KeyPrefix + "test:" + uuid.NewV7().String() + ":"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := c.Del(ctx, p+"missing", p+"a", p+"nx", p+"short", p+"cnt", p+"conditional"); err != nil {
			t.Errorf("cleanup cache keys: %v", err)
		}
	})

	if _, found, err := c.Get(ctx, p+"missing"); err != nil || found {
		t.Fatalf("Get missing: found=%v err=%v", found, err)
	}
	if err := c.Set(ctx, p+"a", "1", 0); err != nil {
		t.Fatal(err)
	}
	if v, found, _ := c.Get(ctx, p+"a"); !found || v != "1" {
		t.Fatalf("Get a: %q %v", v, found)
	}

	if ok, _ := c.SetNX(ctx, p+"a", "2", 0); ok {
		t.Fatal("SetNX on existing key must fail")
	}
	if ok, _ := c.SetNX(ctx, p+"nx", "x", time.Hour); !ok {
		t.Fatal("SetNX on new key must succeed")
	}

	if err := c.Set(ctx, p+"short", "v", 30*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, p+"conditional", "v", 30*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	if _, found, err := c.Get(ctx, p+"short"); err != nil || found {
		t.Fatalf("expired key: found=%v err=%v", found, err)
	}
	if err := c.DelIfValue(ctx, p+"conditional", "v"); err != nil {
		t.Fatalf("DelIfValue expired: %v", err)
	}
	if v, found, err := c.Get(ctx, p+"conditional"); err != nil || found {
		t.Fatalf("expired key changed: value=%q found=%v err=%v", v, found, err)
	}
	if ok, _ := c.SetNX(ctx, p+"short", "again", 0); !ok {
		t.Fatal("SetNX must treat expired key as absent")
	}

	m, err := c.MGet(ctx, []string{p + "a", p + "nx", p + "missing"})
	if err != nil || len(m) != 2 || m[p+"a"] != "1" || m[p+"nx"] != "x" {
		t.Fatalf("MGet: %v %v", m, err)
	}

	t.Run("DelIfValue", func(t *testing.T) {
		ctx := t.Context()
		key := p + "conditional"
		for _, tc := range []struct {
			name  string
			value string
			ttl   time.Duration
		}{
			{name: "no-expiry", value: "old"},
			{name: "live-expiry", value: "old", ttl: time.Hour},
			{name: "empty-value"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if err := c.Set(t.Context(), key, tc.value, tc.ttl); err != nil {
					t.Fatal(err)
				}
				if err := c.DelIfValue(t.Context(), key, tc.value); err != nil {
					t.Fatal(err)
				}
				if v, found, err := c.Get(t.Context(), key); err != nil || found {
					t.Fatalf("matching value remains: value=%q found=%v err=%v", v, found, err)
				}
			})
		}
		if err := c.DelIfValue(ctx, p+"missing", "old"); err != nil {
			t.Fatalf("DelIfValue missing: %v", err)
		}
		if _, found, err := c.Get(ctx, p+"missing"); err != nil || found {
			t.Fatalf("missing key changed: found=%v err=%v", found, err)
		}
		if err := c.Set(ctx, key, "old", 0); err != nil {
			t.Fatal(err)
		}
		if err := c.Set(ctx, key, "new", 0); err != nil {
			t.Fatal(err)
		}
		if err := c.DelIfValue(ctx, key, "old"); err != nil {
			t.Fatal(err)
		}
		if v, found, err := c.Get(ctx, key); err != nil || !found || v != "new" {
			t.Fatalf("replacement changed: value=%q found=%v err=%v", v, found, err)
		}
		if v, found, err := c.Get(ctx, p+"nx"); err != nil || !found || v != "x" {
			t.Fatalf("other key changed: value=%q found=%v err=%v", v, found, err)
		}
	})

	t.Run("DelIfValueConcurrentSet", func(t *testing.T) {
		ctx := t.Context()
		key := p + "conditional"
		for round := range 100 {
			if err := c.Set(ctx, key, "old", 0); err != nil {
				t.Fatal(err)
			}
			start := make(chan struct{})
			var wg sync.WaitGroup
			var setErr, delErr error
			wg.Go(func() {
				<-start
				setErr = c.Set(ctx, key, "new", 0)
			})
			wg.Go(func() {
				<-start
				delErr = c.DelIfValue(ctx, key, "old")
			})
			close(start)
			wg.Wait()
			if setErr != nil || delErr != nil {
				t.Fatalf("round %d: Set=%v DelIfValue=%v", round, setErr, delErr)
			}
			if v, found, err := c.Get(ctx, key); err != nil || !found || v != "new" {
				t.Fatalf("round %d: value=%q found=%v err=%v, want new", round, v, found, err)
			}
		}
	})

	if n, err := c.IncrBy(ctx, p+"cnt", 5, time.Hour); err != nil || n != 5 {
		t.Fatalf("IncrBy new: value=%d err=%v", n, err)
	}
	if n, err := c.IncrBy(ctx, p+"cnt", -2, time.Hour); err != nil || n != 3 {
		t.Fatalf("IncrBy existing: value=%d err=%v", n, err)
	}

	t.Run("IncrByExistingZeroTtl", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			ttl      time.Duration
			incrTtl  time.Duration
			wantLive bool
		}{
			{name: "persistent", incrTtl: 100 * time.Millisecond, wantLive: true},
			{name: "expiring", ttl: 200 * time.Millisecond, incrTtl: time.Hour},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx := t.Context()
				key := p + "cnt"
				if err := c.Set(ctx, key, "0", tc.ttl); err != nil {
					t.Fatal(err)
				}
				if n, err := c.IncrBy(ctx, key, 5, tc.incrTtl); err != nil || n != 5 {
					t.Fatalf("IncrBy zero: value=%d err=%v", n, err)
				}
				time.Sleep(400 * time.Millisecond)
				v, found, err := c.Get(ctx, key)
				if err != nil || found != tc.wantLive {
					t.Fatalf("original TTL changed: found=%v want=%v err=%v", found, tc.wantLive, err)
				}
				if found && v != "5" {
					t.Fatalf("counter value=%q, want 5", v)
				}
			})
		}
	})

	t.Run("IncrByInt64Precision", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			initial string
			delta   int64
			want    int64
		}{
			{name: "new-above-2-to-53", delta: 1<<53 + 1, want: 1<<53 + 1},
			{name: "existing-above-2-to-53", initial: "9007199254740992", delta: 1, want: 1<<53 + 1},
			{name: "max-int64", delta: math.MaxInt64, want: math.MaxInt64},
			{name: "min-int64", delta: math.MinInt64, want: math.MinInt64},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx := t.Context()
				key := p + "cnt"
				if err := c.Del(ctx, key); err != nil {
					t.Fatal(err)
				}
				if tc.initial != "" {
					if err := c.Set(ctx, key, tc.initial, 0); err != nil {
						t.Fatal(err)
					}
				}
				if n, err := c.IncrBy(ctx, key, tc.delta, 0); err != nil || n != tc.want {
					t.Fatalf("IncrBy: value=%d want=%d err=%v", n, tc.want, err)
				}
				want := strconv.FormatInt(tc.want, 10)
				if v, found, err := c.Get(ctx, key); err != nil || !found || v != want {
					t.Fatalf("stored counter: value=%q want=%q found=%v err=%v", v, want, found, err)
				}
			})
		}
	})

	t.Run("IncrByOverflowPreservesValue", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			value int64
			delta int64
		}{
			{name: "positive", value: math.MaxInt64, delta: 1},
			{name: "negative", value: math.MinInt64, delta: -1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx := t.Context()
				key := p + "cnt"
				want := strconv.FormatInt(tc.value, 10)
				if err := c.Set(ctx, key, want, 0); err != nil {
					t.Fatal(err)
				}
				if n, err := c.IncrBy(ctx, key, tc.delta, time.Hour); err == nil {
					t.Fatalf("IncrBy overflow: value=%d, want error", n)
				}
				if v, found, err := c.Get(ctx, key); err != nil || !found || v != want {
					t.Fatalf("overflow changed counter: value=%q want=%q found=%v err=%v", v, want, found, err)
				}
			})
		}
	})

	if ok, _ := c.Expire(ctx, p+"missing", time.Hour); ok {
		t.Fatal("Expire missing must be false")
	}
	if ok, _ := c.Expire(ctx, p+"a", 30*time.Millisecond); !ok {
		t.Fatal("Expire existing must be true")
	}
	time.Sleep(60 * time.Millisecond)
	if _, found, err := c.Get(ctx, p+"a"); err != nil || found {
		t.Fatalf("Expire: found=%v err=%v", found, err)
	}

	if err := c.Del(ctx, p+"nx", p+"cnt", p+"missing"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := c.Get(ctx, p+"nx"); err != nil || found {
		t.Fatalf("Del: found=%v err=%v", found, err)
	}
}
