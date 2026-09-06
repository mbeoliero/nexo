package storetest

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/internal/store"
)

func (m *Mem) GetMessageByClientId(_ context.Context, conversationId, senderId, clientMsgId string) (*store.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, msg := range m.messages {
		if msg.ConversationId == conversationId && msg.SenderId == senderId && msg.ClientMsgId == clientMsgId {
			return &msg, nil
		}
	}
	return nil, store.ErrNotFound
}

func (m *Mem) InsertMessage(_ context.Context, msg *store.Message) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, cur := range m.messages {
		if cur.ServerMsgId == msg.ServerMsgId {
			return false, nil
		}
		duplicateSeq := cur.Seq == msg.Seq
		duplicateClient := cur.SenderId == msg.SenderId && cur.ClientMsgId == msg.ClientMsgId
		if cur.ConversationId == msg.ConversationId && (duplicateSeq || duplicateClient) {
			return false, nil
		}
	}
	m.messages = append(m.messages, *msg)
	return true, nil
}

func (m *Mem) SetConversationMaxSeq(_ context.Context, conversationId string, maxSeq int64, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.convs[conversationId]
	c.MaxSeq, c.UpdatedAt = maxSeq, now
	m.convs[conversationId] = c
	return nil
}

func (m *Mem) ListMessages(_ context.Context, conversationId string, beginSeq, endSeq int64, limit int) ([]store.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.Message
	for _, msg := range m.messages {
		if msg.ConversationId == conversationId && msg.Seq >= beginSeq && msg.Seq <= endSeq {
			out = append(out, msg)
		}
	}
	slices.SortFunc(out, func(a, b store.Message) int { return int(a.Seq - b.Seq) })
	return out[:min(len(out), limit)], nil
}

func (m *Mem) GetMessages(_ context.Context, keys []store.MessageKey) ([]store.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.Message
	for _, msg := range m.messages {
		if slices.Contains(keys, store.MessageKey{ConversationId: msg.ConversationId, Seq: msg.Seq}) {
			out = append(out, msg)
		}
	}
	return out, nil
}

func (m *Mem) TouchUserConversation(_ context.Context, uc *store.UserConversation, readSeq int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := memKey{uc.OwnerId, uc.ConversationId}
	cur, ok := m.userConvs[k]
	if !ok {
		cur = *uc
		cur.MinSeq, cur.MaxSeq, cur.CreatedAt = 1, 0, uc.UpdatedAt
	}
	cur.UpdatedAt = lo.Ternary(cur.UpdatedAt.After(uc.UpdatedAt), cur.UpdatedAt, uc.UpdatedAt)
	cur.ReadSeq = max(cur.ReadSeq, readSeq)
	m.userConvs[k] = cur
	return nil
}

func (m *Mem) TouchConversationMembers(_ context.Context, conversationId string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, uc := range m.userConvs {
		if uc.ConversationId == conversationId && uc.MaxSeq == 0 && now.After(uc.UpdatedAt) {
			uc.UpdatedAt = now
			m.userConvs[k] = uc
		}
	}
	return nil
}

func (m *Mem) AdvanceReadSeq(_ context.Context, ownerId, conversationId string, readSeq int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := memKey{ownerId, conversationId}
	if uc, ok := m.userConvs[k]; ok {
		uc.ReadSeq = max(uc.ReadSeq, readSeq)
		m.userConvs[k] = uc
	}
	return nil
}

func (m *Mem) ListUserConversations(_ context.Context, ownerId string, cursor store.ListCursor, limit int) ([]store.UserConversationRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.UserConversationRow
	for _, uc := range m.userConvs {
		before := uc.UpdatedAt.Before(cursor.UpdatedAt) || uc.UpdatedAt.Equal(cursor.UpdatedAt) && uc.ConversationId < cursor.ConversationId
		if uc.OwnerId == ownerId && before {
			out = append(out, store.UserConversationRow{UserConversation: uc, ConvMaxSeq: m.convs[uc.ConversationId].MaxSeq})
		}
	}
	slices.SortFunc(out, func(a, b store.UserConversationRow) int {
		if order := b.UpdatedAt.Compare(a.UpdatedAt); order != 0 {
			return order
		}
		return strings.Compare(b.ConversationId, a.ConversationId)
	})
	return out[:min(len(out), limit)], nil
}
