package local

import (
	"context"
	"fmt"
	"maps"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/mbeoliero/nexo/internal/cache"
)

type entry struct {
	val string
	exp time.Time
}

func (e entry) alive(now time.Time) bool { return e.exp.IsZero() || e.exp.After(now) }

type Cache struct {
	mu   sync.Mutex
	m    map[string]entry
	stop func()
}

var _ cache.Cache = (*Cache)(nil)

func New() *Cache {
	c := &Cache{m: map[string]entry{}}
	ctx, cancel := context.WithCancel(context.Background())
	c.stop = cancel
	go c.sweep(ctx, time.Minute)
	return c
}

func (c *Cache) sweep(ctx context.Context, every time.Duration) {
	// Hoisted: inside the loop this allocated a fresh ticker per wake-up and restarted the
	// interval each time instead of firing on a fixed schedule. Go 1.23+ collects it without Stop.
	tick := time.Tick(every)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			c.mu.Lock()
			now := time.Now()
			maps.DeleteFunc(c.m, func(_ string, e entry) bool { return !e.alive(now) })
			c.mu.Unlock()
		}
	}
}

func (c *Cache) Close() error {
	c.stop()
	return nil
}

func expiry(now time.Time, ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return now.Add(ttl)
}

func (c *Cache) get(key string) (entry, bool) {
	e, ok := c.m[key]
	if !ok {
		return entry{}, false
	}
	if !e.alive(time.Now()) {
		delete(c.m, key)
		return entry{}, false
	}
	return e, true
}

func (c *Cache) Get(_ context.Context, key string) (string, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.get(key)
	return e.val, ok, nil
}

func (c *Cache) MGet(_ context.Context, keys []string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		if e, ok := c.get(k); ok {
			out[k] = e.val
		}
	}
	return out, nil
}

func (c *Cache) Set(_ context.Context, key, val string, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[key] = entry{val: val, exp: expiry(time.Now(), ttl)}
	return nil
}

func (c *Cache) SetNX(_ context.Context, key, val string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.get(key); ok {
		return false, nil
	}
	c.m[key] = entry{val: val, exp: expiry(time.Now(), ttl)}
	return true, nil
}

func (c *Cache) Del(_ context.Context, keys ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		delete(c.m, k)
	}
	return nil
}

func (c *Cache) DelIfValue(_ context.Context, key, expected string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.get(key); ok && e.val == expected {
		delete(c.m, key)
	}
	return nil
}

func (c *Cache) IncrBy(_ context.Context, key string, delta int64, ttl time.Duration) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var cur int64
	e, ok := c.get(key)
	if ok {
		n, err := strconv.ParseInt(e.val, 10, 64)
		if err != nil {
			return 0, err
		}
		cur = n
	}
	if delta > 0 && cur > math.MaxInt64-delta {
		return 0, fmt.Errorf("cache IncrBy overflow: %w", strconv.ErrRange)
	}
	if delta < 0 && cur < math.MinInt64-delta {
		return 0, fmt.Errorf("cache IncrBy overflow: %w", strconv.ErrRange)
	}
	cur += delta
	exp := e.exp
	if !ok {
		exp = expiry(time.Now(), ttl)
	}
	c.m[key] = entry{val: strconv.FormatInt(cur, 10), exp: exp}
	return cur, nil
}

func (c *Cache) Expire(_ context.Context, key string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.get(key)
	if !ok {
		return false, nil
	}
	e.exp = expiry(time.Now(), ttl)
	c.m[key] = e
	return true, nil
}
