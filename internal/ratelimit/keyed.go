// Package ratelimit is one golang.org/x/time/rate limiter per key: per client IP for the
// bcrypt-heavy auth routes, per sender for message sends. Node-local; the LB (deploy/nginx.conf)
// is the production layer.
package ratelimit

import (
	"maps"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// IdleTtl is how long a key survives without a call before the next sweep drops it.
const IdleTtl = 5 * time.Minute

// Keyed sweeps idle keys and caps how many it tracks, so a wide key set costs the caller
// throughput rather than this node's memory.
type Keyed struct {
	mu       sync.Mutex
	rate     rate.Limit
	burst    int
	max      int
	entries  map[string]*entry
	overflow *rate.Limiter
	swept    time.Time
}

type entry struct {
	lim  *rate.Limiter
	seen time.Time
}

// NewKeyed allows perSec per key with the given burst. Past max distinct keys new ones share a
// single bucket instead of growing the map without bound. perSec <= 0 allows everything.
func NewKeyed(perSec float64, burst, max int) *Keyed {
	limit := rate.Limit(perSec)
	return &Keyed{rate: limit, burst: burst, max: max, entries: map[string]*entry{},
		overflow: rate.NewLimiter(limit, burst), swept: time.Now()}
}

func (k *Keyed) Allow(key string, now time.Time) bool {
	if k.rate <= 0 { // immutable after NewKeyed, so no lock
		return true
	}
	k.mu.Lock()
	if now.Sub(k.swept) > IdleTtl {
		k.sweep(now)
	}
	l := k.limiterFor(key, now)
	k.mu.Unlock()
	return l.AllowN(now, 1)
}

// limiterFor returns key's own limiter, or the shared overflow one when the map is full.
// Callers hold k.mu.
func (k *Keyed) limiterFor(key string, now time.Time) *rate.Limiter {
	if e, ok := k.entries[key]; ok {
		e.seen = now
		return e.lim
	}
	if len(k.entries) >= k.max {
		return k.overflow
	}
	e := &entry{lim: rate.NewLimiter(k.rate, k.burst), seen: now}
	k.entries[key] = e
	return e.lim
}

// sweep drops entries idle for longer than IdleTtl. Callers hold k.mu.
func (k *Keyed) sweep(now time.Time) {
	maps.DeleteFunc(k.entries, func(_ string, e *entry) bool { return now.Sub(e.seen) > IdleTtl })
	k.swept = now
}
