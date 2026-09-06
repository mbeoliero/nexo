package gormstore

import (
	"context"
	"slices"
	"time"

	"github.com/samber/lo"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/gormstore/model"
)

func (s *Store) GetMessageByClientId(ctx context.Context, conversationId, senderId, clientMsgId string) (*store.Message, error) {
	m, err := gorm.G[model.Message](s.db).Where("conversation_id = ? AND sender_id = ? AND client_msg_id = ?", conversationId, senderId, clientMsgId).First(ctx)
	if err != nil {
		return nil, wrap(err)
	}
	return new(store.Message(m)), nil
}

func (s *Store) InsertMessage(ctx context.Context, m *store.Message) (bool, error) {
	row := model.Message(*m)
	res := gorm.WithResult()
	err := gorm.G[model.Message](s.db, res, clause.OnConflict{DoNothing: true}).Create(ctx, &row)
	return res.RowsAffected > 0, wrap(err)
}

func (s *Store) SetConversationMaxSeq(ctx context.Context, conversationId string, maxSeq int64, now time.Time) error {
	_, err := gorm.G[model.Conversation](s.db).Where("conversation_id = ?", conversationId).Set(
		clause.Assignment{Column: clause.Column{Name: "max_seq"}, Value: maxSeq},
		clause.Assignment{Column: clause.Column{Name: "updated_at"}, Value: now},
	).Update(ctx)
	return wrap(err)
}

func (s *Store) ListMessages(ctx context.Context, conversationId string, beginSeq, endSeq int64, limit int) ([]store.Message, error) {
	rows, err := gorm.G[model.Message](s.db).Where("conversation_id = ? AND seq >= ? AND seq <= ?", conversationId, beginSeq, endSeq).Order("seq").Limit(limit).Find(ctx)
	if err != nil {
		return nil, wrap(err)
	}
	return lo.Map(rows, func(m model.Message, _ int) store.Message { return store.Message(m) }), nil
}

// getMessagesChunk bounds one row-constructor IN list. Every key costs two placeholders and
// limits.conversation_page_max is operator-configurable with no ceiling, so a large page would
// otherwise walk into MySQL's 65535-placeholder limit (onlinestore/db chunks for the same reason).
const getMessagesChunk = 500

// Row-constructor IN works on both MySQL 8 and PostgreSQL, and GORM expands a [][]any argument
// into one, so the tuple list stays bound parameters instead of SQL text built here.
func (s *Store) GetMessages(ctx context.Context, keys []store.MessageKey) ([]store.Message, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	out := make([]store.Message, 0, len(keys))
	for chunk := range slices.Chunk(keys, getMessagesChunk) {
		tuples := lo.Map(chunk, func(k store.MessageKey, _ int) []any { return []any{k.ConversationId, k.Seq} })
		rows, err := gorm.G[model.Message](s.db).Where("(conversation_id, seq) IN ?", tuples).Find(ctx)
		if err != nil {
			return nil, wrap(err)
		}
		out = append(out, lo.Map(rows, func(m model.Message, _ int) store.Message { return store.Message(m) })...)
	}
	return out, nil
}

func (s *Store) TouchUserConversation(ctx context.Context, uc *store.UserConversation, readSeq int64) error {
	row := model.UserConversation{
		OwnerId: uc.OwnerId, ConversationId: uc.ConversationId, Type: uc.Type, PeerUserId: uc.PeerUserId, GroupId: uc.GroupId,
		MinSeq: 1, ReadSeq: readSeq, UpdatedAt: uc.UpdatedAt, CreatedAt: uc.UpdatedAt,
	}
	onConflict := clause.OnConflict{
		Columns: []clause.Column{{Name: "owner_id"}, {Name: "conversation_id"}},
		DoUpdates: clause.Set{
			{Column: clause.Column{Name: "updated_at"}, Value: gorm.Expr("GREATEST(user_conversations.updated_at, ?)", uc.UpdatedAt)},
			{Column: clause.Column{Name: "read_seq"}, Value: gorm.Expr("GREATEST(user_conversations.read_seq, ?)", readSeq)},
		},
	}
	return wrap(gorm.G[model.UserConversation](s.db, onConflict).Create(ctx, &row))
}

func (s *Store) TouchConversationMembers(ctx context.Context, conversationId string, now time.Time) error {
	_, err := gorm.G[model.UserConversation](s.db).Where("conversation_id = ? AND max_seq = 0", conversationId).Update(ctx, "updated_at", gorm.Expr("GREATEST(updated_at, ?)", now))
	return wrap(err)
}

func (s *Store) AdvanceReadSeq(ctx context.Context, ownerId, conversationId string, readSeq int64) error {
	_, err := gorm.G[model.UserConversation](s.db).Where("owner_id = ? AND conversation_id = ?", ownerId, conversationId).Update(ctx, "read_seq", gorm.Expr("GREATEST(read_seq, ?)", readSeq))
	return wrap(err)
}

type ucRow struct {
	model.UserConversation
	ConvMaxSeq int64
}

func (s *Store) ListUserConversations(ctx context.Context, ownerId string, cursor store.ListCursor, limit int) ([]store.UserConversationRow, error) {
	var rows []ucRow
	err := s.db.WithContext(ctx).Table("user_conversations AS uc").
		Select("uc.*, c.max_seq AS conv_max_seq").
		Joins("JOIN conversations c ON c.conversation_id = uc.conversation_id").
		Where("uc.owner_id = ? AND (uc.updated_at, uc.conversation_id) < (?, ?)", ownerId, cursor.UpdatedAt, cursor.ConversationId).
		Order("uc.updated_at DESC, uc.conversation_id DESC").Limit(limit).Scan(&rows).Error
	if err != nil {
		return nil, wrap(err)
	}
	return lo.Map(rows, func(r ucRow, _ int) store.UserConversationRow {
		return store.UserConversationRow{UserConversation: store.UserConversation(r.UserConversation), ConvMaxSeq: r.ConvMaxSeq}
	}), nil
}
