package redis

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mbeoliero/nexo/internal/cache"
)

type Cache struct {
	cli *redis.Client
}

var _ cache.Cache = (*Cache)(nil)

func New(ctx context.Context, addr, password string, db int) (*Cache, error) {
	cli := redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: db, ContextTimeoutEnabled: true})
	if err := cli.Ping(ctx).Err(); err != nil {
		cli.Close()
		return nil, err
	}
	return &Cache{cli: cli}, nil
}

func (c *Cache) Close() error { return c.cli.Close() }

func (c *Cache) Get(ctx context.Context, key string) (string, bool, error) {
	v, err := c.cli.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	return v, err == nil, err
}

func (c *Cache) MGet(ctx context.Context, keys []string) (map[string]string, error) {
	if len(keys) == 0 {
		return map[string]string{}, nil
	}
	vals, err := c.cli.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(keys))
	for i, v := range vals {
		if s, ok := v.(string); ok {
			out[keys[i]] = s
		}
	}
	return out, nil
}

func (c *Cache) Set(ctx context.Context, key, val string, ttl time.Duration) error {
	return c.cli.Set(ctx, key, val, max(ttl, 0)).Err()
}

func (c *Cache) SetNX(ctx context.Context, key, val string, ttl time.Duration) (bool, error) {
	return c.cli.SetNX(ctx, key, val, max(ttl, 0)).Result()
}

func (c *Cache) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return c.cli.Del(ctx, keys...).Err()
}

var delIfValue = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0`)

func (c *Cache) DelIfValue(ctx context.Context, key, expected string) error {
	return delIfValue.Run(ctx, c.cli, []string{key}, expected).Err()
}

// TTL applies only when the key is created, matching the pg implementation.
var incrBy = redis.NewScript(`
local exists = redis.call('EXISTS', KEYS[1])
redis.call('INCRBY', KEYS[1], ARGV[1])
if exists == 0 and tonumber(ARGV[2]) > 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return redis.call('GET', KEYS[1])`)

func (c *Cache) IncrBy(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error) {
	return incrBy.Run(ctx, c.cli, []string{key}, delta, max(ttl, 0).Milliseconds()).Int64()
}

func (c *Cache) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return c.cli.Persist(ctx, key).Result()
	}
	return c.cli.PExpire(ctx, key, ttl).Result()
}
