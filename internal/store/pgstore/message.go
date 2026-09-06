package pgstore

import (
	"context"
	"time"

	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/pgstore/gen"
)

func (s *Store) GetMessageByClientId(ctx context.Context, conversationId, senderId, clientMsgId string) (*store.Message, error) {
	m, err := s.q.GetMessageByClientId(ctx, gen.GetMessageByClientIdParams{ConversationId: conversationId, SenderId: senderId, ClientMsgId: clientMsgId})
	if err != nil {
		return nil, wrap(err)
	}
	return new(store.Message(m)), nil
}

func (s *Store) InsertMessage(ctx context.Context, m *store.Message) (bool, error) {
	n, err := s.q.InsertMessage(ctx, gen.InsertMessageParams(*m))
	return n > 0, wrap(err)
}

func (s *Store) SetConversationMaxSeq(ctx context.Context, conversationId string, maxSeq int64, now time.Time) error {
	return wrap(s.q.SetConversationMaxSeq(ctx, gen.SetConversationMaxSeqParams{ConversationId: conversationId, MaxSeq: maxSeq, UpdatedAt: now}))
}

func (s *Store) ListMessages(ctx context.Context, conversationId string, beginSeq, endSeq int64, limit int) ([]store.Message, error) {
	rows, err := s.q.ListMessages(ctx, gen.ListMessagesParams{ConversationId: conversationId, BeginSeq: beginSeq, EndSeq: endSeq, RowLimit: int32(limit)})
	if err != nil {
		return nil, wrap(err)
	}
	return lo.Map(rows, func(m gen.Message, _ int) store.Message { return store.Message(m) }), nil
}

func (s *Store) GetMessages(ctx context.Context, keys []store.MessageKey) ([]store.Message, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	rows, err := s.q.GetMessages(ctx, gen.GetMessagesParams{
		ConversationIds: lo.Map(keys, func(k store.MessageKey, _ int) string { return k.ConversationId }),
		Seqs:            lo.Map(keys, func(k store.MessageKey, _ int) int64 { return k.Seq }),
	})
	if err != nil {
		return nil, wrap(err)
	}
	return lo.Map(rows, func(m gen.Message, _ int) store.Message { return store.Message(m) }), nil
}

func (s *Store) TouchUserConversation(ctx context.Context, uc *store.UserConversation, readSeq int64) error {
	return wrap(s.q.TouchUserConversation(ctx, gen.TouchUserConversationParams{
		OwnerId: uc.OwnerId, ConversationId: uc.ConversationId, Type: uc.Type, PeerUserId: uc.PeerUserId, GroupId: uc.GroupId,
		ReadSeq: readSeq, UpdatedAt: uc.UpdatedAt,
	}))
}

func (s *Store) TouchConversationMembers(ctx context.Context, conversationId string, now time.Time) error {
	return wrap(s.q.TouchConversationMembers(ctx, gen.TouchConversationMembersParams{ConversationId: conversationId, UpdatedAt: now}))
}

func (s *Store) AdvanceReadSeq(ctx context.Context, ownerId, conversationId string, readSeq int64) error {
	return wrap(s.q.AdvanceReadSeq(ctx, gen.AdvanceReadSeqParams{OwnerId: ownerId, ConversationId: conversationId, ReadSeq: readSeq}))
}

func (s *Store) ListUserConversations(ctx context.Context, ownerId string, cursor store.ListCursor, limit int) ([]store.UserConversationRow, error) {
	rows, err := s.q.ListUserConversations(ctx, gen.ListUserConversationsParams{
		OwnerId: ownerId, CursorUpdatedAt: cursor.UpdatedAt, CursorConversationId: cursor.ConversationId, RowLimit: int32(limit),
	})
	if err != nil {
		return nil, wrap(err)
	}
	return lo.Map(rows, func(r gen.ListUserConversationsRow, _ int) store.UserConversationRow {
		return store.UserConversationRow{UserConversation: store.UserConversation(r.UserConversation), ConvMaxSeq: r.ConvMaxSeq}
	}), nil
}
