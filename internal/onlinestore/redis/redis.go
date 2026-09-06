package redis

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mbeoliero/nexo/internal/onlinestore"
	"github.com/mbeoliero/nexo/internal/store"
)

// Store keeps one ZSET per user: member "platform:node:conn_id", score = expiry unix seconds.
// Reads filter by score and never write; Renew trims what expired.
type Store struct {
	cli *redis.Client
	ttl time.Duration
	now func() time.Time
}

func New(ctx context.Context, addr, password string, db int, ttl time.Duration) (*Store, error) {
	cli := redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: db, ContextTimeoutEnabled: true})
	if err := cli.Ping(ctx).Err(); err != nil {
		cli.Close()
		return nil, fmt.Errorf("onlinestore/redis: %w", err)
	}
	return &Store{cli: cli, ttl: ttl, now: store.NowMs}, nil
}

func (s *Store) Close() error { return s.cli.Close() }

func key(userId string) string { return "nexo:online:" + userId }

func member(nodeId string, c onlinestore.ConnRef) string {
	return strconv.Itoa(c.PlatformId) + ":" + nodeId + ":" + c.ConnId
}

func (s *Store) Add(ctx context.Context, nodeId string, c onlinestore.ConnRef) error {
	score := float64(s.now().Add(s.ttl).Unix())
	pipe := s.cli.TxPipeline()
	pipe.ZAdd(ctx, key(c.UserId), redis.Z{Score: score, Member: member(nodeId, c)})
	pipe.Expire(ctx, key(c.UserId), 2*s.ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Store) Remove(ctx context.Context, nodeId string, c onlinestore.ConnRef) error {
	return s.cli.ZRem(ctx, key(c.UserId), member(nodeId, c)).Err()
}

func (s *Store) Renew(ctx context.Context, nodeId string, conns []onlinestore.ConnRef) error {
	if len(conns) == 0 {
		return nil
	}
	now := s.now()
	score := float64(now.Add(s.ttl).Unix())
	cutoff := "(" + strconv.FormatInt(now.Unix(), 10)
	pipe := s.cli.Pipeline()
	for _, c := range conns {
		// Gateway serializes snapshot+Renew with Add/Remove; lost live registrations can be restored.
		pipe.ZAdd(ctx, key(c.UserId), redis.Z{Score: score, Member: member(nodeId, c)})
		// Drop what a crashed node left behind. These keys are exactly the ones that stay alive
		// long enough to accumulate, since Expire below keeps refreshing them.
		pipe.ZRemRangeByScore(ctx, key(c.UserId), "-inf", cutoff)
		pipe.Expire(ctx, key(c.UserId), 2*s.ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Store) Online(ctx context.Context, userIds []string) (map[string][]int, error) {
	if len(userIds) == 0 {
		return map[string][]int{}, nil
	}
	now := strconv.FormatInt(s.now().Unix(), 10)
	pipe := s.cli.Pipeline()
	cmds := make([]*redis.StringSliceCmd, len(userIds))
	for i, id := range userIds {
		// Read-only: the score filter already hides expired members, and a write here would make
		// presence lookups fail against a read-only replica. Renew does the trimming.
		cmds[i] = pipe.ZRangeByScore(ctx, key(id), &redis.ZRangeBy{Min: now, Max: "+inf"})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	out := map[string][]int{}
	for i, id := range userIds {
		for _, m := range cmds[i].Val() {
			p, _, _ := strings.Cut(m, ":")
			if platform, err := strconv.Atoi(p); err == nil && !slices.Contains(out[id], platform) {
				out[id] = append(out[id], platform)
			}
		}
	}
	return out, nil
}

// PurgeNode is a no-op: entries carry a score and expire within ttl on their own,
// and a restarted node reuses fresh conn ids.
func (s *Store) PurgeNode(context.Context, string) error { return nil }
