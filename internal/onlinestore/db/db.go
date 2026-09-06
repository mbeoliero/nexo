package db

import (
	"context"
	"slices"
	"time"

	"github.com/mbeoliero/nexo/internal/onlinestore"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/samber/lo"
)

// Store keeps connections in the online_conns table; rows older than ttl are ignored,
// so a dead node's entries expire on their own.
type Store struct {
	st  store.OnlineConnStore
	ttl time.Duration
	now func() time.Time
}

func New(st store.OnlineConnStore, ttl time.Duration) *Store {
	return &Store{st: st, ttl: ttl, now: store.NowMs}
}

func (s *Store) Add(ctx context.Context, nodeId string, c onlinestore.ConnRef) error {
	return s.st.UpsertOnlineConn(ctx, &store.OnlineConn{ConnId: c.ConnId, UserId: c.UserId, PlatformId: int32(c.PlatformId), NodeId: nodeId, HeartbeatAt: s.now()})
}

// conn_id is unique across nodes, so this driver does not need nodeId.
func (s *Store) Remove(ctx context.Context, _ string, c onlinestore.ConnRef) error {
	return s.st.DeleteOnlineConn(ctx, c.ConnId)
}

// renewChunk bounds the id list per statement: gormstore expands `conn_id IN ?` to one placeholder
// per id, and MySQL refuses a statement with more than 65535 of them.
const renewChunk = 500

// Renew stamps one heartbeat time across every chunk, so a slow batch cannot make the last
// connections look fresher than the first.
func (s *Store) Renew(ctx context.Context, nodeId string, conns []onlinestore.ConnRef) error {
	ids := lo.Map(conns, func(c onlinestore.ConnRef, _ int) string { return c.ConnId })
	now := s.now()
	for chunk := range slices.Chunk(ids, renewChunk) {
		if err := s.st.RenewOnlineConns(ctx, nodeId, chunk, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Online(ctx context.Context, userIds []string) (map[string][]int, error) {
	rows, err := s.st.ListOnlineConns(ctx, userIds, s.now().Add(-s.ttl))
	if err != nil {
		return nil, err
	}
	out := map[string][]int{}
	for _, r := range rows {
		if !slices.Contains(out[r.UserId], int(r.PlatformId)) {
			out[r.UserId] = append(out[r.UserId], int(r.PlatformId))
		}
	}
	return out, nil
}

func (s *Store) PurgeNode(ctx context.Context, nodeId string) error {
	return s.st.DeleteOnlineConnsByNode(ctx, nodeId)
}
