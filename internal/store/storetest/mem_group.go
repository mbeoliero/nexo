package storetest

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/internal/store"
)

type memKey struct{ a, b string }

func (m *Mem) CreateGroup(_ context.Context, g *store.Group, members []store.GroupMember) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.groups[g.Id]; ok {
		return store.ErrDuplicate
	}
	m.groups[g.Id] = *g
	for _, gm := range members {
		m.members[memKey{gm.GroupId, gm.UserId}] = gm
	}
	return nil
}

func (m *Mem) GetGroup(_ context.Context, id string) (*store.Group, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &g, nil
}

func (m *Mem) AddGroupMember(_ context.Context, gm *store.GroupMember) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := memKey{gm.GroupId, gm.UserId}
	if _, ok := m.members[k]; ok {
		return store.ErrDuplicate
	}
	m.members[k] = *gm
	return nil
}

func (m *Mem) RemoveGroupMember(_ context.Context, groupId, userId string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := memKey{groupId, userId}
	_, ok := m.members[k]
	delete(m.members, k)
	return ok, nil
}

func (m *Mem) GetGroupMember(_ context.Context, groupId, userId string) (*store.GroupMember, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	gm, ok := m.members[memKey{groupId, userId}]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &gm, nil
}

func (m *Mem) sortedMembers(pred func(store.GroupMember) bool) []store.GroupMember {
	var out []store.GroupMember
	for _, gm := range m.members {
		if pred(gm) {
			out = append(out, gm)
		}
	}
	slices.SortFunc(out, func(a, b store.GroupMember) int {
		if order := a.JoinedAt.Compare(b.JoinedAt); order != 0 {
			return order
		}
		return strings.Compare(a.UserId, b.UserId)
	})
	return out
}

func (m *Mem) ListGroupMembers(_ context.Context, groupId string) ([]store.GroupMember, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sortedMembers(func(gm store.GroupMember) bool { return gm.GroupId == groupId }), nil
}

func (m *Mem) CountGroupMembers(_ context.Context, groupId string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var count int64
	for _, gm := range m.members {
		if gm.GroupId == groupId {
			count++
		}
	}
	return count, nil
}

func (m *Mem) ListUserGroupIds(_ context.Context, userId string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ids []string
	for _, gm := range m.sortedMembers(func(gm store.GroupMember) bool { return gm.UserId == userId }) {
		ids = append(ids, gm.GroupId)
	}
	return ids, nil
}

func (m *Mem) LockConversation(_ context.Context, id string, typ int32, groupId string, now time.Time) (*store.Conversation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.convs[id]
	if !ok {
		c = store.Conversation{ConversationId: id, Type: typ, GroupId: groupId, CreatedAt: now, UpdatedAt: now}
		m.convs[id] = c
	}
	return &c, nil
}

func (m *Mem) GetUserConversation(_ context.Context, ownerId, conversationId string) (*store.UserConversation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	uc, ok := m.userConvs[memKey{ownerId, conversationId}]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &uc, nil
}

func (m *Mem) GetUserConversationRow(_ context.Context, ownerId, conversationId string) (*store.UserConversationRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	uc, ok := m.userConvs[memKey{ownerId, conversationId}]
	if !ok {
		return nil, store.ErrNotFound
	}
	c, ok := m.convs[conversationId]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &store.UserConversationRow{UserConversation: uc, ConvMaxSeq: c.MaxSeq}, nil
}

func (m *Mem) UpsertUserConversation(_ context.Context, uc *store.UserConversation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := memKey{uc.OwnerId, uc.ConversationId}
	cur, ok := m.userConvs[k]
	if !ok {
		cur = *uc
		cur.CreatedAt = uc.UpdatedAt
	}
	cur.MinSeq, cur.MaxSeq, cur.ReadSeq, cur.UpdatedAt = uc.MinSeq, uc.MaxSeq, uc.ReadSeq, uc.UpdatedAt
	m.userConvs[k] = cur
	return nil
}

func (m *Mem) DeleteUserConversation(_ context.Context, ownerId, conversationId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.userConvs, memKey{ownerId, conversationId})
	return nil
}

func (m *Mem) VisibleOwners(_ context.Context, conversationId string, ownerIds []string, seq int64) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, id := range ownerIds {
		if uc, ok := m.userConvs[memKey{id, conversationId}]; ok && uc.MinSeq <= seq && (uc.MaxSeq == 0 || seq <= uc.MaxSeq) {
			out = append(out, id)
		}
	}
	return out, nil
}

func (m *Mem) MutedOwners(_ context.Context, conversationId string, ownerIds []string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, id := range ownerIds {
		if uc, ok := m.userConvs[memKey{id, conversationId}]; ok && uc.RecvMsgOpt != 0 {
			out = append(out, id)
		}
	}
	return out, nil
}

func (m *Mem) SetUserConversationMaxSeq(_ context.Context, ownerId, conversationId string, maxSeq int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := memKey{ownerId, conversationId}
	uc, ok := m.userConvs[k]
	if !ok {
		return nil
	}
	uc.MaxSeq = maxSeq
	m.userConvs[k] = uc
	return nil
}

// Test hooks to shape state the service cannot reach yet (seq allocation lands in phase 4).
func (m *Mem) SetConversation(c store.Conversation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.convs[c.ConversationId] = c
}

func (m *Mem) SetGroupMember(gm store.GroupMember) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.members[memKey{gm.GroupId, gm.UserId}] = gm
}

func (m *Mem) SetGroup(g store.Group) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.groups[g.Id] = g
}

func (m *Mem) GetConversation(_ context.Context, id string) (*store.Conversation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.convs[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &c, nil
}

func (m *Mem) SetUserConversationOpt(_ context.Context, ownerId, conversationId string, recvMsgOpt *int32, isPinned *bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := memKey{ownerId, conversationId}
	uc, ok := m.userConvs[k]
	if !ok {
		return store.ErrNotFound
	}
	uc.RecvMsgOpt, uc.IsPinned = lo.FromPtrOr(recvMsgOpt, uc.RecvMsgOpt), lo.FromPtrOr(isPinned, uc.IsPinned)
	m.userConvs[k] = uc
	return nil
}

func (m *Mem) CreateUserConversations(_ context.Context, ucs []store.UserConversation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := make(map[memKey]struct{}, len(ucs))
	for _, uc := range ucs {
		key := memKey{a: uc.OwnerId, b: uc.ConversationId}
		if _, ok := m.userConvs[key]; ok {
			return store.ErrDuplicate
		}
		if _, ok := seen[key]; ok {
			return store.ErrDuplicate
		}
		seen[key] = struct{}{}
	}
	for _, uc := range ucs {
		uc.CreatedAt = uc.UpdatedAt
		m.userConvs[memKey{a: uc.OwnerId, b: uc.ConversationId}] = uc
	}
	return nil
}
