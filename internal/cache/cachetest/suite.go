// Shared suite; each implementation's _test.go runs it.
package cachetest

import (
	"context"
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
		for _, key := range []string{p + "a", p + "nx", p + "short", p + "conditional"} {
			value, found, err := c.Get(ctx, key)
			if err != nil {
				t.Errorf("read owned cache key for cleanup: %v", err)
				continue
			}
			if found {
				if err := c.DelIfValue(ctx, key, value); err != nil {
					t.Errorf("cleanup owned cache key: %v", err)
				}
			}
		}
	})

	if _, found, err := c.Get(ctx, p+"missing"); err != nil || found {
		t.Fatalf("Get missing: found=%v err=%v", found, err)
	}
	if err := c.Set(ctx, p+"a", "1", 0); err != nil {
		t.Fatal(err)
	}
	if v, found, err := c.Get(ctx, p+"a"); err != nil || !found || v != "1" {
		t.Fatalf("Get a: value=%q found=%v err=%v", v, found, err)
	}

	if ok, err := c.SetNX(ctx, p+"a", "2", 0); err != nil || ok {
		t.Fatalf("SetNX existing: ok=%v err=%v", ok, err)
	}
	if ok, err := c.SetNX(ctx, p+"nx", "x", time.Hour); err != nil || !ok {
		t.Fatalf("SetNX new: ok=%v err=%v", ok, err)
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
	if ok, err := c.SetNX(ctx, p+"short", "again", 0); err != nil || !ok {
		t.Fatalf("SetNX expired: ok=%v err=%v", ok, err)
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
}
