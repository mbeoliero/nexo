package gateway

import (
	"maps"
	"slices"
	"sync"

	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/config"
)

// UserMap is the node-local connection registry. Register and Unregister share one
// lock so a late Register can never resurrect an already closed client.
type UserMap struct {
	mu     sync.RWMutex
	limits config.LimitsConfig
	// byUser holds adopted clients and answers routing; byUserN counts reserved slots, which
	// includes connections still inside the WS upgrade. Only byUserN may drive the per-user limit:
	// concurrent handshakes all reserve before any of them adopts.
	byUser  map[string][]*Client
	byUserN map[string]int
	byToken map[string]int
	byIp    map[string]int
	total   int
	closing bool
}

// Slot is the capacity one connection takes; Reserve holds it across the WS upgrade so an
// over-limit client is refused with 429 before the protocol switch (design §9).
type Slot struct{ UserId, TokenId, Ip string }

func NewUserMap(limits config.LimitsConfig) *UserMap {
	return &UserMap{limits: limits, byUser: map[string][]*Client{}, byUserN: map[string]int{}, byToken: map[string]int{}, byIp: map[string]int{}}
}

func over(n, limit int) bool { return limit > 0 && n >= limit }

// Reserve takes a slot or reports why it cannot; Adopt or Release must follow.
func (m *UserMap) Reserve(s Slot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reserve(s)
}

func (m *UserMap) reserve(s Slot) error {
	switch {
	case m.closing:
		return errcode.ErrNodeDraining
	case over(m.total, m.limits.WsConnsTotal):
		return errcode.ErrConnOverLimit.WithMessage("node connection limit reached")
	case over(m.byUserN[s.UserId], m.limits.WsConnsPerUser):
		return errcode.ErrConnOverLimit.WithMessage("too many connections for this user")
	case s.TokenId != "" && over(m.byToken[s.TokenId], m.limits.WsConnsPerToken):
		return errcode.ErrConnOverLimit.WithMessage("too many connections for this token")
	case s.Ip != "" && over(m.byIp[s.Ip], m.limits.WsConnsPerIp):
		return errcode.ErrConnOverLimit.WithMessage("too many connections from this address")
	}
	m.byUserN[s.UserId]++
	incr(m.byToken, s.TokenId) // external_jwt has no token id: no key rather than a shared "" bucket
	incr(m.byIp, s.Ip)
	m.total++
	return nil
}

func (m *UserMap) release(s Slot) {
	decr(m.byUserN, s.UserId)
	decr(m.byToken, s.TokenId)
	decr(m.byIp, s.Ip)
	m.total--
}

// Release gives a reserved slot back when the upgrade never produced a client.
func (m *UserMap) Release(s Slot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.release(s)
}

// Register reserves and adopts in one step (no upgrade in between).
func (m *UserMap) Register(c *Client) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.reserve(c.slot()); err != nil {
		return err
	}
	return m.adopt(c)
}

// Adopt binds a client to the slot Reserve took for it; on failure the slot is released.
func (m *UserMap) Adopt(c *Client) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.adopt(c)
}

func (m *UserMap) adopt(c *Client) error {
	var err error
	select {
	case <-c.closed:
		err = errcode.ErrConnClosed
	default:
		if m.closing {
			err = errcode.ErrNodeDraining
		}
	}
	if err != nil {
		m.release(c.slot())
		return err
	}
	m.byUser[c.UserId] = append(m.byUser[c.UserId], c)
	return nil
}

// Close refuses further registrations and returns the clients to drain; one lock
// covers both so a connection cannot slip in after the snapshot.
func (m *UserMap) Close() []*Client {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closing = true
	return slices.Concat(slices.Collect(maps.Values(m.byUser))...)
}

func (m *UserMap) Unregister(c *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	conns := m.byUser[c.UserId]
	i := slices.Index(conns, c)
	if i < 0 {
		return
	}
	conns = slices.Delete(conns, i, i+1)
	if len(conns) == 0 {
		delete(m.byUser, c.UserId)
	} else {
		m.byUser[c.UserId] = conns
	}
	m.release(c.slot())
}

func incr(m map[string]int, k string) {
	if k != "" {
		m[k]++
	}
}

func decr(m map[string]int, k string) {
	if k == "" {
		return
	}
	if m[k] <= 1 {
		delete(m, k)
		return
	}
	m[k]--
}

// Get returns a copy; callers may hold it across sends.
func (m *UserMap) Get(userId string) []*Client {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Clone(m.byUser[userId])
}

func (m *UserMap) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.total
}

// Online keeps the ids that have at least one local connection, in input order.
func (m *UserMap) Online(userIds []string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return lo.Filter(userIds, func(id string, _ int) bool { return len(m.byUser[id]) > 0 })
}

func (m *UserMap) All() []*Client {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Concat(slices.Collect(maps.Values(m.byUser))...)
}
