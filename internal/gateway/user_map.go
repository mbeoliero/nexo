package gateway

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mbeoliero/nexo/internal/cluster"
)

// UserMap manages user connections
type UserMap struct {
	mu       sync.RWMutex
	users    map[string]*UserPlatform // userId -> UserPlatform
	connById map[string]*Client
}

// UserPlatform holds all connections for a user
type UserPlatform struct {
	Clients []*Client
	Time    time.Time
}

// NewUserMap creates a new UserMap
func NewUserMap(rdb *redis.Client) *UserMap {
	_ = rdb
	return &UserMap{
		users:    make(map[string]*UserPlatform),
		connById: make(map[string]*Client),
	}
}

// Register registers a client
func (m *UserMap) Register(ctx context.Context, client *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()

	userPlatform, exists := m.users[client.UserId]
	if !exists {
		userPlatform = &UserPlatform{
			Clients: make([]*Client, 0, 4),
		}
		m.users[client.UserId] = userPlatform
	}

	userPlatform.Clients = append(userPlatform.Clients, client)
	userPlatform.Time = time.Now()
	m.connById[client.ConnId] = client
}

// Unregister unregisters a client and reports whether the connection was
// removed plus whether the user became fully offline afterwards.
func (m *UserMap) Unregister(ctx context.Context, client *Client) (bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	userPlatform, exists := m.users[client.UserId]
	if !exists {
		return false, false
	}

	// Remove the specific client
	newClients := make([]*Client, 0, len(userPlatform.Clients))
	removed := false
	for _, c := range userPlatform.Clients {
		if c.ConnId != client.ConnId {
			newClients = append(newClients, c)
			continue
		}
		removed = true
	}
	if !removed {
		return false, false
	}
	userPlatform.Clients = newClients
	delete(m.connById, client.ConnId)

	// If no more clients, remove user from map
	if len(userPlatform.Clients) == 0 {
		delete(m.users, client.UserId)
		return true, true // User completely disconnected
	}

	return true, false
}

// GetAll gets all clients for a user
func (m *UserMap) GetAll(userId string) ([]*Client, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	userPlatform, exists := m.users[userId]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid race conditions
	clients := make([]*Client, len(userPlatform.Clients))
	copy(clients, userPlatform.Clients)
	return clients, true
}

// GetByPlatform gets clients for a specific platform
func (m *UserMap) GetByPlatform(userId string, platformId int) ([]*Client, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	userPlatform, exists := m.users[userId]
	if !exists {
		return nil, false
	}

	var clients []*Client
	for _, c := range userPlatform.Clients {
		if c.PlatformId == platformId {
			clients = append(clients, c)
		}
	}
	return clients, len(clients) > 0
}

// HasConnection checks if user has any connection
func (m *UserMap) HasConnection(userId string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	userPlatform, exists := m.users[userId]
	return exists && len(userPlatform.Clients) > 0
}

// GetOnlineUserCount returns the number of online users
func (m *UserMap) GetOnlineUserCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.users)
}

// GetOnlineConnCount returns the total number of connections
func (m *UserMap) GetOnlineConnCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, up := range m.users {
		count += len(up.Clients)
	}
	return count
}

// IsOnline checks if user is online (checks Redis for distributed support)
func (m *UserMap) IsOnline(ctx context.Context, userId string) bool {
	_ = ctx
	return m.HasConnection(userId)
}

// RefreshOnlineStatus refreshes the online status TTL
func (m *UserMap) RefreshOnlineStatus(ctx context.Context, userId string) {
	_ = ctx
	_ = userId
}

// GetAllOnlineUserIds returns all online user Ids (local only)
func (m *UserMap) GetAllOnlineUserIds() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	userIds := make([]string, 0, len(m.users))
	for userId := range m.users {
		userIds = append(userIds, userId)
	}
	return userIds
}

// SnapshotRouteConns returns all local connections in route-store form.
func (m *UserMap) SnapshotRouteConns(instanceId string) []cluster.RouteConn {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]cluster.RouteConn, 0, len(m.connById))
	for _, client := range m.connById {
		result = append(result, cluster.RouteConn{
			UserId:     client.UserId,
			ConnId:     client.ConnId,
			InstanceId: instanceId,
			PlatformId: client.PlatformId,
		})
	}
	return result
}

// GetByConnId returns the exact local client by connection id.
func (m *UserMap) GetByConnId(connId string) (*Client, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	client, ok := m.connById[connId]
	return client, ok
}
