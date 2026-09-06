package gormstore

import (
	"context"
	"time"

	"github.com/samber/lo"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/gormstore/model"
)

func (s *Store) UpsertOnlineConn(ctx context.Context, c *store.OnlineConn) error {
	row := model.OnlineConn(*c)
	return wrap(gorm.G[model.OnlineConn](s.db, clause.OnConflict{Columns: []clause.Column{{Name: "conn_id"}}, DoUpdates: clause.AssignmentColumns([]string{"user_id", "platform_id", "node_id", "heartbeat_at"})}).Create(ctx, &row))
}

func (s *Store) DeleteOnlineConn(ctx context.Context, connId string) error {
	_, err := gorm.G[model.OnlineConn](s.db).Where("conn_id = ?", connId).Delete(ctx)
	return wrap(err)
}

func (s *Store) RenewOnlineConns(ctx context.Context, nodeId string, connIds []string, now time.Time) error {
	if len(connIds) == 0 {
		return nil
	}
	_, err := gorm.G[model.OnlineConn](s.db).Where("node_id = ? AND conn_id IN ?", nodeId, connIds).Update(ctx, "heartbeat_at", now)
	return wrap(err)
}

func (s *Store) ListOnlineConns(ctx context.Context, userIds []string, since time.Time) ([]store.OnlineConn, error) {
	if len(userIds) == 0 {
		return nil, nil
	}
	rows, err := gorm.G[model.OnlineConn](s.db).Where("user_id IN ? AND heartbeat_at > ?", userIds, since).Find(ctx)
	if err != nil {
		return nil, wrap(err)
	}
	return lo.Map(rows, func(r model.OnlineConn, _ int) store.OnlineConn { return store.OnlineConn(r) }), nil
}

func (s *Store) DeleteOnlineConnsByNode(ctx context.Context, nodeId string) error {
	_, err := gorm.G[model.OnlineConn](s.db).Where("node_id = ?", nodeId).Delete(ctx)
	return wrap(err)
}
