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

const batchSize = 100

func (s *Store) CreateGroup(ctx context.Context, g *store.Group, members []store.GroupMember) error {
	if err := gorm.G[model.Group](s.db).Create(ctx, new(model.Group(*g))); err != nil {
		return wrap(err)
	}
	if len(members) == 0 {
		return nil
	}
	rows := lo.Map(members, func(m store.GroupMember, _ int) model.GroupMember { return model.GroupMember(m) })
	return wrap(gorm.G[model.GroupMember](s.db).CreateInBatches(ctx, &rows, batchSize))
}

func (s *Store) GetGroup(ctx context.Context, id string) (*store.Group, error) {
	g, err := gorm.G[model.Group](s.db).Where("id = ?", id).First(ctx)
	if err != nil {
		return nil, wrap(err)
	}
	return new(store.Group(g)), nil
}

func (s *Store) AddGroupMember(ctx context.Context, m *store.GroupMember) error {
	return wrap(gorm.G[model.GroupMember](s.db).Create(ctx, new(model.GroupMember(*m))))
}

func (s *Store) RemoveGroupMember(ctx context.Context, groupId, userId string) (bool, error) {
	n, err := gorm.G[model.GroupMember](s.db).Where("group_id = ? AND user_id = ?", groupId, userId).Delete(ctx)
	return n > 0, wrap(err)
}

func (s *Store) GetGroupMember(ctx context.Context, groupId, userId string) (*store.GroupMember, error) {
	m, err := gorm.G[model.GroupMember](s.db).Where("group_id = ? AND user_id = ?", groupId, userId).First(ctx)
	if err != nil {
		return nil, wrap(err)
	}
	return new(store.GroupMember(m)), nil
}

func (s *Store) ListGroupMembers(ctx context.Context, groupId string) ([]store.GroupMember, error) {
	rows, err := gorm.G[model.GroupMember](s.db).Where("group_id = ?", groupId).Order("joined_at, user_id").Find(ctx)
	if err != nil {
		return nil, wrap(err)
	}
	return lo.Map(rows, func(m model.GroupMember, _ int) store.GroupMember { return store.GroupMember(m) }), nil
}

func (s *Store) CountGroupMembers(ctx context.Context, groupId string) (int64, error) {
	n, err := gorm.G[model.GroupMember](s.db).Where("group_id = ?", groupId).Count(ctx, "*")
	return n, wrap(err)
}

func (s *Store) ListUserGroupIds(ctx context.Context, userId string) ([]string, error) {
	rows, err := gorm.G[model.GroupMember](s.db).Where("user_id = ?", userId).Order("joined_at").Find(ctx)
	if err != nil {
		return nil, wrap(err)
	}
	return lo.Map(rows, func(m model.GroupMember, _ int) string { return m.GroupId }), nil
}

func (s *Store) LockConversation(ctx context.Context, id string, typ int32, groupId string, now time.Time) (*store.Conversation, error) {
	row := model.Conversation{ConversationId: id, Type: typ, GroupId: groupId, CreatedAt: now, UpdatedAt: now}
	if err := gorm.G[model.Conversation](s.db, clause.OnConflict{DoNothing: true}).Create(ctx, &row); err != nil {
		return nil, wrap(err)
	}
	c, err := gorm.G[model.Conversation](s.db, clause.Locking{Strength: "UPDATE"}).Where("conversation_id = ?", id).First(ctx)
	if err != nil {
		return nil, wrap(err)
	}
	return new(store.Conversation(c)), nil
}

func (s *Store) GetUserConversation(ctx context.Context, ownerId, conversationId string) (*store.UserConversation, error) {
	uc, err := gorm.G[model.UserConversation](s.db).Where("owner_id = ? AND conversation_id = ?", ownerId, conversationId).First(ctx)
	if err != nil {
		return nil, wrap(err)
	}
	return new(store.UserConversation(uc)), nil
}

func (s *Store) GetUserConversationRow(ctx context.Context, ownerId, conversationId string) (*store.UserConversationRow, error) {
	from := clause.From{
		Tables: []clause.Table{{Name: "user_conversations", Alias: "uc"}},
		Joins: []clause.Join{{Type: clause.InnerJoin, Table: clause.Table{Name: "conversations", Alias: "c"},
			ON: clause.Where{Exprs: []clause.Expression{clause.Eq{
				Column: clause.Column{Table: "c", Name: "conversation_id"},
				Value:  clause.Column{Table: "uc", Name: "conversation_id"},
			}}},
		}},
	}
	r, err := gorm.G[ucRow](s.db, from).Select("uc.*, c.max_seq AS conv_max_seq").
		Where("uc.owner_id = ? AND uc.conversation_id = ?", ownerId, conversationId).Take(ctx)
	if err != nil {
		return nil, wrap(err)
	}
	return &store.UserConversationRow{UserConversation: store.UserConversation(r.UserConversation), ConvMaxSeq: r.ConvMaxSeq}, nil
}

func (s *Store) UpsertUserConversation(ctx context.Context, uc *store.UserConversation) error {
	row := model.UserConversation(*uc)
	row.CreatedAt = uc.UpdatedAt
	onConflict := clause.OnConflict{
		Columns:   []clause.Column{{Name: "owner_id"}, {Name: "conversation_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"min_seq", "max_seq", "read_seq", "updated_at"}),
	}
	return wrap(gorm.G[model.UserConversation](s.db, onConflict).Create(ctx, &row))
}

func (s *Store) SetUserConversationMaxSeq(ctx context.Context, ownerId, conversationId string, maxSeq int64) error {
	_, err := gorm.G[model.UserConversation](s.db).Where("owner_id = ? AND conversation_id = ?", ownerId, conversationId).Update(ctx, "max_seq", maxSeq)
	return wrap(err)
}

func (s *Store) DeleteUserConversation(ctx context.Context, ownerId, conversationId string) error {
	_, err := gorm.G[model.UserConversation](s.db).Where("owner_id = ? AND conversation_id = ?", ownerId, conversationId).Delete(ctx)
	return wrap(err)
}

func (s *Store) VisibleOwners(ctx context.Context, conversationId string, ownerIds []string, seq int64) ([]string, error) {
	if len(ownerIds) == 0 {
		return nil, nil
	}
	rows, err := gorm.G[model.UserConversation](s.db).
		Where("conversation_id = ? AND owner_id IN ? AND min_seq <= ? AND (max_seq = 0 OR max_seq >= ?)", conversationId, ownerIds, seq, seq).
		Find(ctx)
	if err != nil {
		return nil, wrap(err)
	}
	return lo.Map(rows, func(uc model.UserConversation, _ int) string { return uc.OwnerId }), nil
}

func (s *Store) MutedOwners(ctx context.Context, conversationId string, ownerIds []string) ([]string, error) {
	if len(ownerIds) == 0 {
		return nil, nil
	}
	rows, err := gorm.G[model.UserConversation](s.db).Where("conversation_id = ? AND owner_id IN ? AND recv_msg_opt <> 0", conversationId, ownerIds).Find(ctx)
	if err != nil {
		return nil, wrap(err)
	}
	return lo.Map(rows, func(uc model.UserConversation, _ int) string { return uc.OwnerId }), nil
}

func (s *Store) GetConversation(ctx context.Context, id string) (*store.Conversation, error) {
	c, err := gorm.G[model.Conversation](s.db).Where("conversation_id = ?", id).First(ctx)
	if err != nil {
		return nil, wrap(err)
	}
	return new(store.Conversation(c)), nil
}

func (s *Store) SetUserConversationOpt(ctx context.Context, ownerId, conversationId string, recvMsgOpt *int32, isPinned *bool) error {
	var sets []clause.Assigner
	if recvMsgOpt != nil {
		sets = append(sets, clause.Assignment{Column: clause.Column{Name: "recv_msg_opt"}, Value: *recvMsgOpt})
	}
	if isPinned != nil {
		sets = append(sets, clause.Assignment{Column: clause.Column{Name: "is_pinned"}, Value: *isPinned})
	}
	if len(sets) == 0 {
		_, err := s.GetUserConversation(ctx, ownerId, conversationId)
		return err
	}
	n, err := gorm.G[model.UserConversation](s.db).Where("owner_id = ? AND conversation_id = ?", ownerId, conversationId).Set(sets...).Update(ctx)
	if err == nil && n == 0 {
		// MySQL reports 0 affected rows when the values were already set; only a missing row is NotFound.
		_, err = s.GetUserConversation(ctx, ownerId, conversationId)
	}
	return wrap(err)
}

func (s *Store) CreateUserConversations(ctx context.Context, ucs []store.UserConversation) error {
	if len(ucs) == 0 {
		return nil
	}
	rows := lo.Map(ucs, func(uc store.UserConversation, _ int) model.UserConversation {
		row := model.UserConversation(uc)
		row.CreatedAt = uc.UpdatedAt
		return row
	})
	return wrap(gorm.G[model.UserConversation](s.db).CreateInBatches(ctx, &rows, 200))
}
