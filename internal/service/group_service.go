package service

import (
	"context"
	"errors"

	"github.com/mbeoliero/kit/log"
	"gorm.io/gorm"

	"github.com/mbeoliero/nexo/internal/entity"
	"github.com/mbeoliero/nexo/internal/repository"
	"github.com/mbeoliero/nexo/pkg/constant"
	"github.com/mbeoliero/nexo/pkg/errcode"
	"github.com/mbeoliero/nexo/pkg/idgen"
)

// GroupService handles group-related business logic
type GroupService struct {
	groupRepo *repository.GroupRepo
	seqRepo   *repository.SeqRepo
	repos     *repository.Repositories
}

// NewGroupService creates a new GroupService
func NewGroupService(repos *repository.Repositories) *GroupService {
	return &GroupService{
		groupRepo: repos.Group,
		seqRepo:   repos.Seq,
		repos:     repos,
	}
}

// CreateGroupRequest represents group creation request
type CreateGroupRequest struct {
	Name         string   `json:"name"`
	Introduction string   `json:"introduction,omitempty"`
	Avatar       string   `json:"avatar,omitempty"`
	MemberIds    []string `json:"member_ids,omitempty"` // Initial members to invite
}

// CreateGroup creates a new group
func (s *GroupService) CreateGroup(ctx context.Context, creatorId string, req *CreateGroupRequest) (*entity.Group, error) {
	groupId, err := idgen.NextID()
	if err != nil {
		log.CtxError(ctx, "generate group id failed: %v", err)
		return nil, errcode.ErrInternalServer
	}
	now := entity.NowUnixMilli()

	group := &entity.Group{
		Id:            groupId,
		Name:          req.Name,
		Introduction:  req.Introduction,
		Avatar:        req.Avatar,
		Status:        constant.GroupStatusNormal,
		CreatorUserId: creatorId,
	}

	err = s.repos.Transaction(ctx, func(tx *gorm.DB) error {
		// Create group
		group.CreatedAt = now
		group.UpdatedAt = now
		if err := tx.Create(group).Error; err != nil {
			return err
		}

		// Create seq_conversations record
		conversationId := entity.GenGroupConversationId(groupId)
		if err := s.seqRepo.EnsureSeqConversationExists(ctx, tx, conversationId); err != nil {
			return err
		}

		// Add creator as owner
		creator := &entity.GroupMember{
			GroupId:   groupId,
			UserId:    creatorId,
			RoleLevel: constant.RoleLevelOwner,
			Status:    constant.GroupMemberStatusNormal,
			JoinedAt:  now,
			JoinSeq:   1, // Creator sees all messages from seq 1
		}
		if err := s.groupRepo.AddMember(ctx, tx, creator); err != nil {
			return err
		}

		// Set creator's min_seq
		if err := s.seqRepo.SetSeqUserMinSeq(ctx, tx, creatorId, conversationId, 1); err != nil {
			return err
		}

		// Add initial members if any
		for _, memberId := range req.MemberIds {
			if memberId == creatorId {
				continue
			}
			member := &entity.GroupMember{
				GroupId:       groupId,
				UserId:        memberId,
				RoleLevel:     constant.RoleLevelMember,
				Status:        constant.GroupMemberStatusNormal,
				JoinedAt:      now,
				JoinSeq:       1, // Initial members see all messages
				InviterUserId: creatorId,
			}
			if err := s.groupRepo.AddMember(ctx, tx, member); err != nil {
				return err
			}
			if err := s.seqRepo.SetSeqUserMinSeq(ctx, tx, memberId, conversationId, 1); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		log.CtxError(ctx, "create group failed: %v", err)
		return nil, errcode.ErrInternalServer
	}

	log.CtxInfo(ctx, "group created: group_id=%s, creator_id=%s", groupId, creatorId)
	return group, nil
}

// JoinGroup joins a user to a group
// New members cannot see historical messages (join_seq = max_seq + 1)
func (s *GroupService) JoinGroup(ctx context.Context, groupId, userId, inviterId string) error {
	conversationId := entity.GenGroupConversationId(groupId)

	err := s.repos.Transaction(ctx, func(tx *gorm.DB) error {
		// Check if group exists and is normal
		group, err := s.groupRepo.GetByIdWithTx(ctx, tx, groupId)
		if err != nil {
			return classifyGroupLookupError(err)
		}
		if !group.IsNormal() {
			return errcode.ErrGroupDismissed
		}

		// Check if already a member
		existingMember, err := s.groupRepo.GetMemberWithTx(ctx, tx, groupId, userId)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if existingMember != nil && existingMember.IsNormal() {
			return errcode.ErrAlreadyGroupMember
		}

		// Lock seq_conversations row and get max_seq
		maxSeq, err := s.seqRepo.GetMaxSeqWithLock(ctx, tx, conversationId)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		// join_seq = max_seq + 1, so new member only sees messages from now on
		joinSeq := maxSeq + 1
		now := entity.NowUnixMilli()

		member := &entity.GroupMember{
			GroupId:       groupId,
			UserId:        userId,
			RoleLevel:     constant.RoleLevelMember,
			Status:        constant.GroupMemberStatusNormal,
			JoinedAt:      now,
			JoinSeq:       joinSeq,
			InviterUserId: inviterId,
		}

		// Add member (handles rejoin scenario with ON DUPLICATE KEY UPDATE)
		if err := s.groupRepo.AddMember(ctx, tx, member); err != nil {
			return err
		}

		// Set user's min_seq for this conversation
		if err := s.seqRepo.SetSeqUserMinSeq(ctx, tx, userId, conversationId, joinSeq); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if e, ok := err.(*errcode.Error); ok {
			return e
		}
		log.CtxError(ctx, "join group failed: group_id=%s, user_id=%s, error=%v", groupId, userId, err)
		return errcode.ErrInternalServer
	}

	log.CtxInfo(ctx, "user joined group: group_id=%s, user_id=%s", groupId, userId)
	return nil
}

// QuitGroup removes a user from a group
// After quitting, user cannot see new messages (max_seq is set)
func (s *GroupService) QuitGroup(ctx context.Context, groupId, userId string) error {
	conversationId := entity.GenGroupConversationId(groupId)

	err := s.repos.Transaction(ctx, func(tx *gorm.DB) error {
		// Check if user is a member
		member, err := s.groupRepo.GetMemberWithTx(ctx, tx, groupId, userId)
		if err != nil {
			return classifyGroupMemberLookupError(err)
		}
		if !member.IsNormal() {
			return errcode.ErrNotGroupMember
		}

		// Owner cannot quit (must transfer ownership or dismiss)
		if member.IsOwner() {
			return errcode.ErrCannotKickOwner
		}

		// Get current max_seq
		maxSeq, err := s.seqRepo.GetMaxSeqWithLock(ctx, tx, conversationId)
		if err != nil && err != gorm.ErrRecordNotFound {
			return err
		}

		// Update member status to left
		if err := s.groupRepo.UpdateMemberStatus(ctx, tx, groupId, userId, constant.GroupMemberStatusLeft); err != nil {
			return err
		}

		// Set user's max_seq so they cannot see future messages
		if err := s.seqRepo.SetSeqUserMaxSeq(ctx, tx, userId, conversationId, maxSeq); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if e, ok := err.(*errcode.Error); ok {
			return e
		}
		log.CtxError(ctx, "quit group failed: group_id=%s, user_id=%s, error=%v", groupId, userId, err)
		return errcode.ErrInternalServer
	}

	log.CtxInfo(ctx, "user quit group: group_id=%s, user_id=%s", groupId, userId)
	return nil
}

// GetGroupInfo gets group info
func (s *GroupService) GetGroupInfo(ctx context.Context, groupId string) (*entity.GroupInfo, error) {
	group, err := s.groupRepo.GetById(ctx, groupId)
	if err != nil {
		return nil, classifyGroupLookupError(err)
	}

	memberCount, err := s.groupRepo.GetMemberCount(ctx, groupId)
	if err != nil {
		log.CtxError(ctx, "get member count failed: group_id=%s, error=%v", groupId, err)
		memberCount = 0
	}

	return &entity.GroupInfo{
		Id:            group.Id,
		Name:          group.Name,
		Introduction:  group.Introduction,
		Avatar:        group.Avatar,
		Status:        group.Status,
		CreatorUserId: group.CreatorUserId,
		MemberCount:   memberCount,
		CreatedAt:     group.CreatedAt,
	}, nil
}

// GetGroupMembers gets group members
func (s *GroupService) GetGroupMembers(ctx context.Context, groupId string) ([]*entity.GroupMember, error) {
	members, err := s.groupRepo.GetActiveMembers(ctx, groupId)
	if err != nil {
		log.CtxError(ctx, "get group members failed: group_id=%s, error=%v", groupId, err)
		return nil, errcode.ErrInternalServer
	}
	return members, nil
}

// GetActiveMemberUserIds gets active member user Ids
func (s *GroupService) GetActiveMemberUserIds(ctx context.Context, groupId string) ([]string, error) {
	return s.groupRepo.GetActiveMemberUserIds(ctx, groupId)
}

func classifyGroupLookupError(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return errcode.ErrGroupNotFound
	}
	return errcode.ErrInternalServer
}

func classifyGroupMemberLookupError(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return errcode.ErrNotGroupMember
	}
	return errcode.ErrInternalServer
}

// IsActiveMember checks if user is an active member
func (s *GroupService) IsActiveMember(ctx context.Context, groupId, userId string) (bool, error) {
	return s.groupRepo.IsActiveMember(ctx, groupId, userId)
}
