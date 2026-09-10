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

func (c *Cache) Set(ctx context.Context, key, val string, ttl time.Duration) error {
	return c.cli.Set(ctx, key, val, max(ttl, 0)).Err()
}

func (c *Cache) SetNX(ctx context.Context, key, val string, ttl time.Duration) (bool, error) {
	return c.cli.SetNX(ctx, key, val, max(ttl, 0)).Result()
}

var delIfValue = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0`)

func (c *Cache) DelIfValue(ctx context.Context, key, expected string) error {
	return delIfValue.Run(ctx, c.cli, []string{key}, expected).Err()
}
