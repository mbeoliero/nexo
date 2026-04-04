package cluster

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/mbeoliero/nexo/pkg/constant"
)

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()

	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr: server.Addr(),
	})

	originalPrefix := constant.GetRedisKeyPrefix()
	constant.InitRedisKeyPrefix("nexo:test:")

	t.Cleanup(func() {
		constant.InitRedisKeyPrefix(originalPrefix)
		_ = client.Close()
		server.Close()
	})

	return server, client
}

func waitForCondition(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not satisfied before timeout")
}
