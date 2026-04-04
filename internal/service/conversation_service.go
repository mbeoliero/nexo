package service

import (
	"context"

	"github.com/mbeoliero/kit/log"

	"github.com/mbeoliero/nexo/internal/entity"
	"github.com/mbeoliero/nexo/internal/repository"
	"github.com/mbeoliero/nexo/pkg/constant"
	"github.com/mbeoliero/nexo/pkg/errcode"
)

// ConversationService handles conversation-related business logic
type ConversationService struct {
	convRepo  *repository.ConversationRepo
	groupRepo *repository.GroupRepo
	seqRepo   *repository.SeqRepo
	repos     *repository.Repositories
}

// NewConversationService creates a new ConversationService
func NewConversationService(repos *repository.Repositories) *ConversationService {
	return &ConversationService{
		convRepo:  repos.Conversation,
		groupRepo: repos.Group,
		seqRepo:   repos.Seq,
		repos:     repos,
	}
}

// GetUserConversations gets all conversations for a user
func (s *ConversationService) GetUserConversations(ctx context.Context, userId string) ([]*entity.ConversationInfo, error) {
	convWithSeqs, err := s.convRepo.GetUserConversationsWithSeq(ctx, userId)
	if err != nil {
		log.CtxError(ctx, "get user conversations failed: user_id=%s, error=%v", userId, err)
		return nil, errcode.ErrInternalServer
	}

	result := make([]*entity.ConversationInfo, 0, len(convWithSeqs))
	for _, conv := range convWithSeqs {
		info := &entity.ConversationInfo{
			ConversationId:   conv.ConversationId,
			ConversationType: conv.ConversationType,
			PeerUserId:       conv.PeerUserId,
			GroupId:          conv.GroupId,
			RecvMsgOpt:       conv.RecvMsgOpt,
			IsPinned:         conv.IsPinned,
			UnreadCount:      conv.UnreadCount,
			MaxSeq:           conv.MaxSeq,
			ReadSeq:          conv.ReadSeq,
			UpdatedAt:        conv.UpdatedAt,
		}
		result = append(result, info)
	}

	return result, nil
}

// GetConversation gets a specific conversation for a user
func (s *ConversationService) GetConversation(ctx context.Context, userId, conversationId string) (*entity.ConversationInfo, error) {
	conv, err := s.convRepo.GetByOwnerAndConvId(ctx, userId, conversationId)
	if err != nil {
		log.CtxError(ctx, "get conversation failed: user_id=%s, conversation_id=%s, error=%v", userId, conversationId, err)
		return nil, errcode.ErrInternalServer
	}
	if conv == nil {
		return nil, errcode.ErrConvNotFound
	}

	// Get seq info
	seqConv, seqConvErr := s.seqRepo.GetConversationSeqInfo(ctx, conversationId)
	seqUser, seqUserErr := s.seqRepo.GetSeqUser(ctx, userId, conversationId)
	maxSeq, readSeq, err := resolveConversationSeqState(seqConv, seqConvErr, seqUser, seqUserErr)
	if err != nil {
		log.CtxError(ctx, "get conversation seq state failed: user_id=%s conversation_id=%s err=%v, seqConvErr=%v, seqUserErr=%v", userId, conversationId, err, seqConvErr, seqUserErr)
		return nil, err
	}

	unreadCount := maxSeq - readSeq
	if unreadCount < 0 {
		unreadCount = 0
	}

	return &entity.ConversationInfo{
		ConversationId:   conv.ConversationId,
		ConversationType: conv.ConversationType,
		PeerUserId:       conv.PeerUserId,
		GroupId:          conv.GroupId,
		RecvMsgOpt:       conv.RecvMsgOpt,
		IsPinned:         conv.IsPinned,
		UnreadCount:      unreadCount,
		MaxSeq:           maxSeq,
		ReadSeq:          readSeq,
		UpdatedAt:        conv.UpdatedAt,
	}, nil
}

func resolveConversationSeqState(seqConv *entity.SeqConversation, seqConvErr error, seqUser *entity.SeqUser, seqUserErr error) (maxSeq, readSeq int64, err error) {
	if seqConvErr != nil || seqUserErr != nil {
		return 0, 0, errcode.ErrInternalServer
	}
	if seqConv != nil {
		maxSeq = seqConv.MaxSeq
	}
	if seqUser != nil {
		readSeq = seqUser.ReadSeq
	}
	return maxSeq, readSeq, nil
}

// UpdateConversationRequest represents update conversation request
type UpdateConversationRequest struct {
	RecvMsgOpt *int32 `json:"recv_msg_opt,omitempty"`
	IsPinned   *bool  `json:"is_pinned,omitempty"`
}

// UpdateConversation updates conversation settings
func (s *ConversationService) UpdateConversation(ctx context.Context, userId, conversationId string, req *UpdateConversationRequest) error {
	updates := make(map[string]interface{})
	if req.RecvMsgOpt != nil {
		updates["recv_msg_opt"] = *req.RecvMsgOpt
	}
	if req.IsPinned != nil {
		updates["is_pinned"] = *req.IsPinned
	}

	if len(updates) == 0 {
		return nil
	}

	if err := s.convRepo.Update(ctx, userId, conversationId, updates); err != nil {
		log.CtxError(ctx, "update conversation failed: %v", err)
		return errcode.ErrInternalServer
	}

	return nil
}

// MarkRead marks a conversation as read up to a seq
func (s *ConversationService) MarkRead(ctx context.Context, userId, conversationId string, readSeq int64) error {
	if err := s.ensureConversationAccess(ctx, userId, conversationId); err != nil {
		return err
	}
	if err := s.seqRepo.UpdateReadSeq(ctx, userId, conversationId, readSeq); err != nil {
		log.CtxError(ctx, "update read seq failed: %v", err)
		return errcode.ErrInternalServer
	}
	return nil
}

// GetMaxReadSeq gets the max seq and read seq for a conversation
func (s *ConversationService) GetMaxReadSeq(ctx context.Context, userId, conversationId string) (maxSeq, readSeq int64, err error) {
	if err := s.ensureConversationAccess(ctx, userId, conversationId); err != nil {
		return 0, 0, err
	}
	seqConv, err := s.seqRepo.GetConversationSeqInfo(ctx, conversationId)
	if err != nil {
		log.CtxError(ctx, "get conversation seq failed: user_id=%s conversation_id=%s err=%v", userId, conversationId, err)
		return 0, 0, errcode.ErrInternalServer
	}

	seqUser, err := s.seqRepo.GetSeqUser(ctx, userId, conversationId)
	if err != nil {
		log.CtxError(ctx, "get user seq failed: user_id=%s conversation_id=%s err=%v", userId, conversationId, err)
		return 0, 0, errcode.ErrInternalServer
	}
	if seqUser != nil {
		readSeq = seqUser.ReadSeq
	}

	return seqConv.MaxSeq, readSeq, nil
}

func (s *ConversationService) ensureConversationAccess(ctx context.Context, userId, conversationId string) error {
	hasAccess, err := s.checkConversationAccess(ctx, userId, conversationId)
	if err != nil {
		log.CtxError(ctx, "check conversation access failed: user_id=%s conversation_id=%s err=%v", userId, conversationId, err)
		return errcode.ErrInternalServer
	}
	if !hasAccess {
		return errcode.ErrNoPermission
	}
	return nil
}

func (s *ConversationService) checkConversationAccess(ctx context.Context, userId, conversationId string) (bool, error) {
	if len(conversationId) < 3 {
		return false, nil
	}

	switch conversationId[:3] {
	case constant.SingleConversationPrefix:
		if len(conversationId) <= 3 {
			return false, nil
		}
		return containsUserId(conversationId[3:], userId), nil
	case constant.GroupConversationPrefix:
		if s.groupRepo == nil {
			return false, errcode.ErrInternalServer
		}
		member, err := s.groupRepo.GetMember(ctx, conversationId[3:], userId)
		found, err := resolveGroupMembershipLookup(err)
		if err != nil {
			return false, err
		}
		if !found {
			return false, nil
		}
		return member.IsNormal(), nil
	default:
		return false, nil
	}
}
