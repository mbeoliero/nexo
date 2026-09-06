package storetest

import (
	"context"
	"sync"
	"time"

	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/internal/store"
)

// Mem is an in-memory Store for service tests. WithTx serializes transactions with one
// mutex, which stands in for the conversation row lock; there is no rollback.
type Mem struct {
	txMu      sync.Mutex
	mu        sync.Mutex
	users     map[string]store.User
	groups    map[string]store.Group
	members   map[memKey]store.GroupMember
	convs     map[string]store.Conversation
	userConvs map[memKey]store.UserConversation
	messages  []store.Message
	online    map[string]store.OnlineConn
}

var _ store.Store = (*Mem)(nil)

func NewMem() *Mem {
	return &Mem{users: map[string]store.User{}, groups: map[string]store.Group{}, members: map[memKey]store.GroupMember{},
		convs: map[string]store.Conversation{}, userConvs: map[memKey]store.UserConversation{}, online: map[string]store.OnlineConn{}}
}

// memTx is the transaction-scoped view a callback gets: every operation forwards to Mem, but
// it owns nothing, so it refuses to nest and its Close is a no-op (the real backends do the same).
type memTx struct{ *Mem }

var _ store.Store = memTx{}

func (memTx) WithTx(context.Context, func(store.Store) error) error { return store.ErrNestedTx }
func (memTx) Close()                                                {}

func (m *Mem) WithTx(_ context.Context, fn func(store.Store) error) error {
	m.txMu.Lock()
	defer m.txMu.Unlock()
	return fn(memTx{m})
}
func (m *Mem) Ping(context.Context) error { return nil }
func (m *Mem) Close()                     {}

func (m *Mem) GetUser(_ context.Context, id string) (*store.User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.users[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &u, nil
}

func (m *Mem) GetUsers(_ context.Context, ids []string) ([]store.User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.User
	for _, id := range ids {
		if u, ok := m.users[id]; ok {
			out = append(out, u)
		}
	}
	return out, nil
}

func (m *Mem) GetUserByUsername(_ context.Context, username string) (*store.User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, u := range m.users {
		if u.Username == username {
			return &u, nil
		}
	}
	return nil, store.ErrNotFound
}

func (m *Mem) CreateUser(_ context.Context, u *store.User) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.users[u.Id]; ok {
		return store.ErrDuplicate
	}
	m.users[u.Id] = *u
	return nil
}

func (m *Mem) UpdateUserProfile(_ context.Context, id string, nickname, avatar, extra *string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.users[id]
	if !ok {
		return store.ErrNotFound
	}
	cur.Nickname, cur.Avatar, cur.Extra = lo.FromPtrOr(nickname, cur.Nickname), lo.FromPtrOr(avatar, cur.Avatar), lo.FromPtrOr(extra, cur.Extra)
	cur.UpdatedAt = now
	m.users[id] = cur
	return nil
}

func (m *Mem) UpsertUser(_ context.Context, u *store.User) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.users[u.Id]
	if !ok {
		cur = store.User{Id: u.Id, CreatedAt: u.UpdatedAt}
	}
	cur.Nickname, cur.Avatar, cur.Extra, cur.UpdatedAt = u.Nickname, u.Avatar, u.Extra, u.UpdatedAt
	m.users[u.Id] = cur
	return nil
}
