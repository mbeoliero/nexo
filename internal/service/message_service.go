package service

import (
	"context"
	"errors"
	"strings"

	"github.com/mbeoliero/kit/log"
	"gorm.io/gorm"

	"github.com/mbeoliero/nexo/internal/entity"
	"github.com/mbeoliero/nexo/internal/repository"
	"github.com/mbeoliero/nexo/pkg/constant"
	"github.com/mbeoliero/nexo/pkg/errcode"
)

// MessagePusher interface for pushing messages
type MessagePusher interface {
	AsyncPushToUsers(msg *entity.Message, userIds []string, excludeConnId string)
}

// MessageService handles message-related business logic
type MessageService struct {
	msgRepo   *repository.MessageRepo
	seqRepo   *repository.SeqRepo
	convRepo  *repository.ConversationRepo
	groupRepo *repository.GroupRepo
	userRepo  *repository.UserRepo
	repos     *repository.Repositories
	pusher    MessagePusher
}

// NewMessageService creates a new MessageService
func NewMessageService(repos *repository.Repositories) *MessageService {
	return &MessageService{
		msgRepo:   repos.Message,
		seqRepo:   repos.Seq,
		convRepo:  repos.Conversation,
		groupRepo: repos.Group,
		userRepo:  repos.User,
		repos:     repos,
	}
}

// SetPusher sets the message pusher
func (s *MessageService) SetPusher(pusher MessagePusher) {
	s.pusher = pusher
}

// SendMessageRequest represents send message request
type SendMessageRequest struct {
	ClientMsgId string                `json:"client_msg_id"`
	RecvId      string                `json:"recv_id,omitempty"`  // For single chat
	GroupId     string                `json:"group_id,omitempty"` // For group chat
	SessionType int32                 `json:"session_type"`
	MsgType     int32                 `json:"msg_type"`
	Content     entity.MessageContent `json:"content"`
}

// SendSingleMessage sends a single chat message
func (s *MessageService) SendSingleMessage(ctx context.Context, senderId string, req *SendMessageRequest) (*entity.Message, error) {
	// Validate request
	if req.RecvId == "" {
		return nil, errcode.ErrInvalidParam
	}
	if req.ClientMsgId == "" {
		return nil, errcode.ErrInvalidParam
	}

	// Check for idempotency
	existingMsg, err := s.msgRepo.GetByClientMsgId(ctx, senderId, req.ClientMsgId)
	if err != nil {
		log.CtxError(ctx, "check idempotency failed: %v", err)
		return nil, errcode.ErrInternalServer
	}
	if existingMsg != nil {
		// Return existing message (idempotent response)
		log.CtxDebug(ctx, "duplicate message: client_msg_id=%s", req.ClientMsgId)
		return existingMsg, nil
	}

	conversationId := entity.GenSingleConversationId(senderId, req.RecvId)
	now := entity.NowUnixMilli()

	var msg *entity.Message

	err = s.repos.Transaction(ctx, func(tx *gorm.DB) error {
		// Allocate seq
		seq, err := s.seqRepo.AllocSeq(ctx, conversationId)
		if err != nil {
			return errcode.ErrSeqAllocFailed.Wrap(err)
		}

		// Create message
		msg = &entity.Message{
			ConversationId: conversationId,
			Seq:            seq,
			ClientMsgId:    req.ClientMsgId,
			SenderId:       senderId,
			RecvId:         req.RecvId,
			SessionType:    constant.SessionTypeSingle,
			MsgType:        req.MsgType,
			SendAt:         now,
		}
		msg.SetContent(req.Content)

		if err = s.msgRepo.Create(ctx, tx, msg); err != nil {
			return err
		}

		// Sync seq to MySQL
		if err = s.seqRepo.SyncSeqToMySQLWithTx(ctx, tx, conversationId, seq); err != nil {
			return err
		}

		// Ensure conversations exist for both parties with correct peer_user_id
		if err = s.convRepo.EnsureSingleChatConversations(ctx, tx, conversationId, senderId, req.RecvId); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		var e *errcode.Error
		if errors.As(err, &e) {
			return nil, e
		}
		log.CtxError(ctx, "send single message failed: %v", err)
		return nil, errcode.ErrSendFailed
	}

	// Update sender's read_seq (sender has read their own message)
	_ = s.seqRepo.UpdateReadSeq(ctx, senderId, conversationId, msg.Seq)

	// Async push to receiver (and sender's other connections)
	if s.pusher != nil {
		s.pusher.AsyncPushToUsers(msg, []string{senderId, req.RecvId}, "")
	}

	log.CtxInfo(ctx, "single message sent: sender_id=%s, recv_id=%s, seq=%d", senderId, req.RecvId, msg.Seq)
	return msg, nil
}

// SendGroupMessage sends a group chat message
func (s *MessageService) SendGroupMessage(ctx context.Context, senderId string, req *SendMessageRequest) (*entity.Message, error) {
	// Validate request
	if req.GroupId == "" {
		return nil, errcode.ErrInvalidParam
	}
	if req.ClientMsgId == "" {
		return nil, errcode.ErrInvalidParam
	}

	// Check permission: sender must be active group member
	member, err := s.groupRepo.GetMember(ctx, req.GroupId, senderId)
	if err != nil {
		return nil, errcode.ErrNotGroupMember
	}
	if !member.IsNormal() {
		return nil, errcode.ErrMemberNotActive
	}

	// Check group status
	group, err := s.groupRepo.GetById(ctx, req.GroupId)
	if err != nil {
		return nil, errcode.ErrGroupNotFound
	}
	if !group.IsNormal() {
		return nil, errcode.ErrGroupDismissed
	}

	// Check for idempotency
	existingMsg, err := s.msgRepo.GetByClientMsgId(ctx, senderId, req.ClientMsgId)
	if err != nil {
		log.CtxError(ctx, "check idempotency failed: %v", err)
		return nil, errcode.ErrInternalServer
	}
	if existingMsg != nil {
		// Return existing message (idempotent response)
		log.CtxDebug(ctx, "duplicate message: client_msg_id=%s", req.ClientMsgId)
		return existingMsg, nil
	}

	conversationId := entity.GenGroupConversationId(req.GroupId)
	now := entity.NowUnixMilli()

	var msg *entity.Message

	err = s.repos.Transaction(ctx, func(tx *gorm.DB) error {
		// Allocate seq
		seq, err := s.seqRepo.AllocSeq(ctx, conversationId)
		if err != nil {
			return errcode.ErrSeqAllocFailed.Wrap(err)
		}

		// Create message
		msg = &entity.Message{
			ConversationId: conversationId,
			Seq:            seq,
			ClientMsgId:    req.ClientMsgId,
			SenderId:       senderId,
			GroupId:        req.GroupId,
			SessionType:    constant.SessionTypeGroup,
			MsgType:        req.MsgType,
			SendAt:         now,
		}
		msg.SetContent(req.Content)

		if err := s.msgRepo.Create(ctx, tx, msg); err != nil {
			return err
		}

		// Sync seq to MySQL
		if err := s.seqRepo.SyncSeqToMySQLWithTx(ctx, tx, conversationId, seq); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if e, ok := err.(*errcode.Error); ok {
			return nil, e
		}
		log.CtxError(ctx, "send group message failed: %v", err)
		return nil, errcode.ErrSendFailed
	}

	// Update sender's read_seq
	_ = s.seqRepo.UpdateReadSeq(ctx, senderId, conversationId, msg.Seq)

	// Async push to all active group members
	if s.pusher != nil {
		memberIds, err := s.groupRepo.GetActiveMemberUserIds(ctx, req.GroupId)
		if err == nil && len(memberIds) > 0 {
			s.pusher.AsyncPushToUsers(msg, memberIds, "")
		}
	}

	log.CtxInfo(ctx, "group message sent: sender_id=%s, group_id=%s, seq=%d", senderId, req.GroupId, msg.Seq)
	return msg, nil
}

// SendMessage sends a message (auto-detect single/group)
func (s *MessageService) SendMessage(ctx context.Context, senderId string, req *SendMessageRequest) (*entity.Message, error) {
	if req.SessionType == constant.SessionTypeSingle || req.RecvId != "" {
		return s.SendSingleMessage(ctx, senderId, req)
	}
	if req.SessionType == constant.SessionTypeGroup || req.GroupId != "" {
		return s.SendGroupMessage(ctx, senderId, req)
	}
	return nil, errcode.ErrInvalidParam
}

// PullMessagesRequest represents pull messages request
type PullMessagesRequest struct {
	ConversationId string `json:"conversation_id"`
	BeginSeq       int64  `json:"begin_seq"`
	EndSeq         int64  `json:"end_seq"`
	Limit          int    `json:"limit"`
}

// PullMessages pulls messages for a user
func (s *MessageService) PullMessages(ctx context.Context, userId string, req *PullMessagesRequest) ([]*entity.Message, int64, error) {
	// Authorization check: verify user has access to this conversation
	hasAccess, err := s.checkConversationAccess(ctx, userId, req.ConversationId)
	if err != nil {
		log.CtxError(ctx, "check conversation access failed: %v", err)
		return nil, 0, errcode.ErrInternalServer
	}
	if !hasAccess {
		return nil, 0, errcode.ErrNoPermission
	}

	// Get conversation max seq
	convSeq, err := s.seqRepo.GetConversationSeqInfo(ctx, req.ConversationId)
	if err != nil {
		log.CtxError(ctx, "get conversation seq failed: %v", err)
		return nil, 0, errcode.ErrInternalServer
	}

	// Get user's visible range for this conversation
	seqUser, seqUserErr := s.seqRepo.GetSeqUser(ctx, userId, req.ConversationId)
	beginSeq, endSeq, err := resolvePullRange(req.BeginSeq, req.EndSeq, convSeq.MaxSeq, seqUser, seqUserErr)
	if err != nil {
		log.CtxError(ctx, "get user visible seq range failed: user_id=%s conversation_id=%s err=%v", userId, req.ConversationId, err)
		return nil, 0, err
	}

	// Validate range
	if beginSeq > endSeq {
		return []*entity.Message{}, convSeq.MaxSeq, nil
	}

	// Pull messages
	limit := req.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}

	messages, err := s.msgRepo.PullMessages(ctx, req.ConversationId, beginSeq, endSeq, limit)
	if err != nil {
		log.CtxError(ctx, "pull messages failed: %v", err)
		return nil, 0, errcode.ErrPullFailed
	}

	return messages, convSeq.MaxSeq, nil
}

func resolvePullRange(beginSeq, endSeq, convMaxSeq int64, seqUser *entity.SeqUser, seqUserErr error) (int64, int64, error) {
	if seqUserErr != nil {
		return 0, 0, errcode.ErrInternalServer
	}
	if endSeq == 0 {
		endSeq = convMaxSeq
	}
	if seqUser != nil {
		beginSeq, endSeq = seqUser.ClampSeqRange(beginSeq, endSeq, convMaxSeq)
	}
	return beginSeq, endSeq, nil
}

// checkConversationAccess verifies if a user has access to a conversation
func (s *MessageService) checkConversationAccess(ctx context.Context, userId, conversationId string) (bool, error) {
	// Parse conversation Id to determine type
	if len(conversationId) < 3 {
		return false, nil
	}

	prefix := conversationId[:3]
	switch prefix {
	case constant.SingleConversationPrefix:
		// Single chat: si_{userA}_{userB}
		// User must be one of the participants
		return s.checkSingleChatAccess(userId, conversationId), nil
	case constant.GroupConversationPrefix:
		// Group chat: sg_{groupId}
		// User must be an active member of the group
		groupId := conversationId[3:]
		return s.checkGroupChatAccess(ctx, userId, groupId)
	default:
		return false, nil
	}
}

// checkSingleChatAccess checks if user is a participant in single chat
func (s *MessageService) checkSingleChatAccess(userId, conversationId string) bool {
	// conversationId format: si_{userA}:{userB} where userA < userB lexicographically
	// Uses ":" as separator between userIds to support userIds containing "_"
	if len(conversationId) <= 3 {
		return false
	}
	participants := conversationId[3:] // Remove "si_" prefix
	// User must be one of the participants
	return containsUserId(participants, userId)
}

// checkGroupChatAccess checks if user is an active member of the group
func (s *MessageService) checkGroupChatAccess(ctx context.Context, userId, groupId string) (bool, error) {
	member, err := s.groupRepo.GetMember(ctx, groupId, userId)
	found, err := resolveGroupMembershipLookup(err)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	// User must be an active member (not quit/kicked)
	return member.IsNormal(), nil
}

func resolveGroupMembershipLookup(err error) (bool, error) {
	if err == nil {
		return true, nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	return false, err
}

// containsUserId checks if userId is part of the conversation participants string
// participants format: {userA}:{userB} (using ":" as separator)
func containsUserId(participants, userId string) bool {
	// Find the ":" separator
	idx := strings.Index(participants, ":")
	if idx == -1 {
		return false
	}
	userA := participants[:idx]
	userB := participants[idx+1:]
	return userA == userId || userB == userId
}

// GetMaxSeq gets the max seq for a conversation (with authorization check)
func (s *MessageService) GetMaxSeq(ctx context.Context, userId, conversationId string) (int64, error) {
	// Authorization check: verify user has access to this conversation
	hasAccess, err := s.checkConversationAccess(ctx, userId, conversationId)
	if err != nil {
		log.CtxError(ctx, "check conversation access failed: %v", err)
		return 0, errcode.ErrInternalServer
	}
	if !hasAccess {
		return 0, errcode.ErrNoPermission
	}

	return s.seqRepo.GetMaxSeq(ctx, conversationId)
}

// UpdateReadSeq updates user's read seq for a conversation (with authorization check)
func (s *MessageService) UpdateReadSeq(ctx context.Context, userId, conversationId string, readSeq int64) error {
	// Authorization check: verify user has access to this conversation
	hasAccess, err := s.checkConversationAccess(ctx, userId, conversationId)
	if err != nil {
		log.CtxError(ctx, "check conversation access failed: %v", err)
		return errcode.ErrInternalServer
	}
	if !hasAccess {
		return errcode.ErrNoPermission
	}

	return s.seqRepo.UpdateReadSeq(ctx, userId, conversationId, readSeq)
}
