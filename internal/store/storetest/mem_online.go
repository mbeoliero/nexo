package storetest

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/mbeoliero/nexo/internal/store"
)

func (m *Mem) UpsertOnlineConn(_ context.Context, c *store.OnlineConn) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.online[c.ConnId] = *c
	return nil
}

func (m *Mem) DeleteOnlineConn(_ context.Context, connId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.online, connId)
	return nil
}

func (m *Mem) RenewOnlineConns(_ context.Context, nodeId string, connIds []string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, c := range m.online {
		if c.NodeId == nodeId && slices.Contains(connIds, c.ConnId) {
			c.HeartbeatAt = now
			m.online[k] = c
		}
	}
	return nil
}

func (m *Mem) ListOnlineConns(_ context.Context, userIds []string, since time.Time) ([]store.OnlineConn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.OnlineConn
	for _, c := range m.online {
		if slices.Contains(userIds, c.UserId) && c.HeartbeatAt.After(since) {
			out = append(out, c)
		}
	}
	return out, nil
}

func (m *Mem) DeleteOnlineConnsByNode(_ context.Context, nodeId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	maps.DeleteFunc(m.online, func(_ string, c store.OnlineConn) bool { return c.NodeId == nodeId })
	return nil
}
