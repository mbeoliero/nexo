package pgstore

import (
	"context"
	"time"

	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/pgstore/gen"
)

func (s *Store) UpsertOnlineConn(ctx context.Context, c *store.OnlineConn) error {
	return wrap(s.q.UpsertOnlineConn(ctx, gen.UpsertOnlineConnParams{ConnId: c.ConnId, UserId: c.UserId, PlatformId: c.PlatformId, NodeId: c.NodeId, HeartbeatAt: c.HeartbeatAt}))
}

func (s *Store) DeleteOnlineConn(ctx context.Context, connId string) error {
	return wrap(s.q.DeleteOnlineConn(ctx, connId))
}

func (s *Store) RenewOnlineConns(ctx context.Context, nodeId string, connIds []string, now time.Time) error {
	if len(connIds) == 0 {
		return nil
	}
	return wrap(s.q.RenewOnlineConns(ctx, gen.RenewOnlineConnsParams{NodeId: nodeId, ConnIds: connIds, HeartbeatAt: now}))
}

func (s *Store) ListOnlineConns(ctx context.Context, userIds []string, since time.Time) ([]store.OnlineConn, error) {
	if len(userIds) == 0 {
		return nil, nil
	}
	rows, err := s.q.ListOnlineConns(ctx, gen.ListOnlineConnsParams{UserIds: userIds, Since: since})
	if err != nil {
		return nil, wrap(err)
	}
	return lo.Map(rows, func(r gen.OnlineConn, _ int) store.OnlineConn { return store.OnlineConn(r) }), nil
}

func (s *Store) DeleteOnlineConnsByNode(ctx context.Context, nodeId string) error {
	return wrap(s.q.DeleteOnlineConnsByNode(ctx, nodeId))
}
