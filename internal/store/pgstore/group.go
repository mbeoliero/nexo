package pgstore

import (
	"context"
	"time"

	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/pgstore/gen"
)

func (s *Store) CreateGroup(ctx context.Context, g *store.Group, members []store.GroupMember) error {
	if err := s.q.CreateGroup(ctx, gen.CreateGroupParams(*g)); err != nil {
		return wrap(err)
	}
	rows := lo.Map(members, func(m store.GroupMember, _ int) gen.AddGroupMembersParams { return gen.AddGroupMembersParams(m) })
	_, err := s.q.AddGroupMembers(ctx, rows)
	return wrap(err)
}

func (s *Store) GetGroup(ctx context.Context, id string) (*store.Group, error) {
	g, err := s.q.GetGroup(ctx, id)
	if err != nil {
		return nil, wrap(err)
	}
	return new(store.Group(g)), nil
}

func (s *Store) AddGroupMember(ctx context.Context, m *store.GroupMember) error {
	return wrap(s.q.AddGroupMember(ctx, gen.AddGroupMemberParams(*m)))
}

func (s *Store) RemoveGroupMember(ctx context.Context, groupId, userId string) (bool, error) {
	n, err := s.q.RemoveGroupMember(ctx, gen.RemoveGroupMemberParams{GroupId: groupId, UserId: userId})
	return n > 0, wrap(err)
}

func (s *Store) GetGroupMember(ctx context.Context, groupId, userId string) (*store.GroupMember, error) {
	m, err := s.q.GetGroupMember(ctx, gen.GetGroupMemberParams{GroupId: groupId, UserId: userId})
	if err != nil {
		return nil, wrap(err)
	}
	return new(store.GroupMember(m)), nil
}

func (s *Store) ListGroupMembers(ctx context.Context, groupId string) ([]store.GroupMember, error) {
	rows, err := s.q.ListGroupMembers(ctx, groupId)
	if err != nil {
		return nil, wrap(err)
	}
	return lo.Map(rows, func(m gen.GroupMember, _ int) store.GroupMember { return store.GroupMember(m) }), nil
}

func (s *Store) CountGroupMembers(ctx context.Context, groupId string) (int64, error) {
	n, err := s.q.CountGroupMembers(ctx, groupId)
	return n, wrap(err)
}

func (s *Store) ListUserGroupIds(ctx context.Context, userId string) ([]string, error) {
	ids, err := s.q.ListUserGroupIds(ctx, userId)
	return ids, wrap(err)
}

func (s *Store) LockConversation(ctx context.Context, id string, typ int32, groupId string, now time.Time) (*store.Conversation, error) {
	if err := s.q.InsertConversationIfMissing(ctx, gen.InsertConversationIfMissingParams{ConversationId: id, Type: typ, GroupId: groupId, CreatedAt: now}); err != nil {
		return nil, wrap(err)
	}
	c, err := s.q.LockConversation(ctx, id)
	if err != nil {
		return nil, wrap(err)
	}
	return new(store.Conversation(c)), nil
}

func (s *Store) GetUserConversation(ctx context.Context, ownerId, conversationId string) (*store.UserConversation, error) {
	uc, err := s.q.GetUserConversation(ctx, gen.GetUserConversationParams{OwnerId: ownerId, ConversationId: conversationId})
	if err != nil {
		return nil, wrap(err)
	}
	return new(store.UserConversation(uc)), nil
}

func (s *Store) GetUserConversationRow(ctx context.Context, ownerId, conversationId string) (*store.UserConversationRow, error) {
	r, err := s.q.GetUserConversationRow(ctx, gen.GetUserConversationRowParams{OwnerId: ownerId, ConversationId: conversationId})
	if err != nil {
		return nil, wrap(err)
	}
	return &store.UserConversationRow{UserConversation: store.UserConversation(r.UserConversation), ConvMaxSeq: r.ConvMaxSeq}, nil
}

func (s *Store) UpsertUserConversation(ctx context.Context, uc *store.UserConversation) error {
	return wrap(s.q.UpsertUserConversation(ctx, gen.UpsertUserConversationParams{
		OwnerId: uc.OwnerId, ConversationId: uc.ConversationId, Type: uc.Type, PeerUserId: uc.PeerUserId, GroupId: uc.GroupId,
		MinSeq: uc.MinSeq, MaxSeq: uc.MaxSeq, ReadSeq: uc.ReadSeq, RecvMsgOpt: uc.RecvMsgOpt, IsPinned: uc.IsPinned, Extra: uc.Extra, UpdatedAt: uc.UpdatedAt,
	}))
}

func (s *Store) SetUserConversationMaxSeq(ctx context.Context, ownerId, conversationId string, maxSeq int64) error {
	return wrap(s.q.SetUserConversationMaxSeq(ctx, gen.SetUserConversationMaxSeqParams{OwnerId: ownerId, ConversationId: conversationId, MaxSeq: maxSeq}))
}

func (s *Store) DeleteUserConversation(ctx context.Context, ownerId, conversationId string) error {
	return wrap(s.q.DeleteUserConversation(ctx, gen.DeleteUserConversationParams{OwnerId: ownerId, ConversationId: conversationId}))
}

func (s *Store) VisibleOwners(ctx context.Context, conversationId string, ownerIds []string, seq int64) ([]string, error) {
	if len(ownerIds) == 0 {
		return nil, nil
	}
	ids, err := s.q.VisibleOwners(ctx, gen.VisibleOwnersParams{ConversationId: conversationId, OwnerIds: ownerIds, Seq: seq})
	return ids, wrap(err)
}

func (s *Store) MutedOwners(ctx context.Context, conversationId string, ownerIds []string) ([]string, error) {
	if len(ownerIds) == 0 {
		return nil, nil
	}
	ids, err := s.q.MutedOwners(ctx, gen.MutedOwnersParams{ConversationId: conversationId, OwnerIds: ownerIds})
	return ids, wrap(err)
}

func (s *Store) GetConversation(ctx context.Context, id string) (*store.Conversation, error) {
	c, err := s.q.GetConversation(ctx, id)
	if err != nil {
		return nil, wrap(err)
	}
	return new(store.Conversation(c)), nil
}

func (s *Store) SetUserConversationOpt(ctx context.Context, ownerId, conversationId string, recvMsgOpt *int32, isPinned *bool) error {
	n, err := s.q.SetUserConversationOpt(ctx, gen.SetUserConversationOptParams{OwnerId: ownerId, ConversationId: conversationId, RecvMsgOpt: recvMsgOpt, IsPinned: isPinned})
	if err == nil && n == 0 {
		return store.ErrNotFound
	}
	return wrap(err)
}

func (s *Store) CreateUserConversations(ctx context.Context, ucs []store.UserConversation) error {
	if len(ucs) == 0 {
		return nil
	}
	rows := lo.Map(ucs, func(uc store.UserConversation, _ int) gen.CreateUserConversationsParams {
		return gen.CreateUserConversationsParams{
			OwnerId: uc.OwnerId, ConversationId: uc.ConversationId, Type: uc.Type, PeerUserId: uc.PeerUserId, GroupId: uc.GroupId,
			MinSeq: uc.MinSeq, MaxSeq: uc.MaxSeq, ReadSeq: uc.ReadSeq, RecvMsgOpt: uc.RecvMsgOpt, IsPinned: uc.IsPinned, Extra: uc.Extra,
			UpdatedAt: uc.UpdatedAt, CreatedAt: uc.UpdatedAt,
		}
	})
	_, err := s.q.CreateUserConversations(ctx, rows)
	return wrap(err)
}
