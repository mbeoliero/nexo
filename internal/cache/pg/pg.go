package pg

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mbeoliero/kit/log"

	"github.com/mbeoliero/nexo/internal/cache"
	"github.com/mbeoliero/nexo/internal/cache/pg/gen"
)

const (
	maxConns     = 4
	cleanupBatch = 1000
)

// Expiry is evaluated with the database clock (now()) so nodes need not agree on time.
type Cache struct {
	pool *pgxpool.Pool
	q    *gen.Queries
	stop context.CancelFunc
}

var _ cache.Cache = (*Cache)(nil)

func New(ctx context.Context, dsn string, cleanerInterval time.Duration) (*Cache, error) {
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("cache/pg: %w", err)
	}
	pc.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("cache/pg: %w", err)
	}
	// pgxpool connects lazily; a bad DSN must fail at startup, not on the first login.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("cache/pg: ping: %w", err)
	}
	c := &Cache{pool: pool, q: gen.New(pool)}
	cctx, cancel := context.WithCancel(context.Background())
	c.stop = cancel
	if cleanerInterval > 0 {
		go c.cleaner(cctx, cleanerInterval)
	}
	return c, nil
}

func (c *Cache) Close() error {
	c.stop()
	c.pool.Close()
	return nil
}

// Jittered so multiple nodes do not sweep in lockstep; SKIP LOCKED makes overlap harmless anyway.
func (c *Cache) cleaner(ctx context.Context, every time.Duration) {
	// rand.Int64N panics on n <= 0, so a sub-4ns interval would take the process down from a
	// goroutine no recover() covers. Validate rejects those, and this keeps the invariant local.
	jitter := int64(every / 4)
	for {
		delay := every
		if jitter > 0 {
			delay += time.Duration(rand.Int64N(jitter))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
			if n, err := c.q.Cleanup(ctx, cleanupBatch); err != nil {
				log.CtxWarn(ctx, "cache/pg cleanup: %v", err)
			} else if n > 0 {
				log.CtxDebug(ctx, "cache/pg cleanup: deleted %d", n)
			}
		}
	}
}

// ttlSeconds is what SQL turns into now() + interval; <= 0 keeps the row without expiry.
func ttlSeconds(ttl time.Duration) float64 { return max(ttl, 0).Seconds() }

func (c *Cache) Get(ctx context.Context, key string) (string, bool, error) {
	v, err := c.q.Get(ctx, key)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return v, err == nil, err
}

func (c *Cache) MGet(ctx context.Context, keys []string) (map[string]string, error) {
	rows, err := c.q.MGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(rows))
	for _, r := range rows {
		out[r.Key] = r.Value
	}
	return out, nil
}

func (c *Cache) Set(ctx context.Context, key, val string, ttl time.Duration) error {
	return c.q.Set(ctx, gen.SetParams{Key: key, Value: val, TtlSeconds: ttlSeconds(ttl)})
}

func (c *Cache) SetNX(ctx context.Context, key, val string, ttl time.Duration) (bool, error) {
	_, err := c.q.SetNX(ctx, gen.SetNXParams{Key: key, Value: val, TtlSeconds: ttlSeconds(ttl)})
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (c *Cache) Del(ctx context.Context, keys ...string) error {
	return c.q.Del(ctx, keys)
}

func (c *Cache) DelIfValue(ctx context.Context, key, expected string) error {
	return c.q.DelIfValue(ctx, gen.DelIfValueParams{Key: key, Value: expected})
}

func (c *Cache) IncrBy(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error) {
	return c.q.IncrBy(ctx, gen.IncrByParams{Key: key, Delta: delta, TtlSeconds: ttlSeconds(ttl)})
}

func (c *Cache) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	_, err := c.q.Expire(ctx, gen.ExpireParams{Key: key, TtlSeconds: ttlSeconds(ttl)})
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}
