package handler

import (
	"context"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/mbeoliero/kit/log"

	"github.com/mbeoliero/nexo/internal/middleware"
	"github.com/mbeoliero/nexo/internal/service"
	"github.com/mbeoliero/nexo/pkg/errcode"
	"github.com/mbeoliero/nexo/pkg/response"
)

// MessageHandler handles message-related requests
type MessageHandler struct {
	msgService *service.MessageService
	sendGate   SendGate
}

type SendGate interface {
	AcquireSendLease() (func(), error)
}

// NewMessageHandler creates a new MessageHandler
func NewMessageHandler(msgService *service.MessageService, sendGate SendGate) *MessageHandler {
	return &MessageHandler{
		msgService: msgService,
		sendGate:   sendGate,
	}
}

// SendMessage handles send message request (HTTP fallback)
func (h *MessageHandler) SendMessage(ctx context.Context, c *app.RequestContext) {
	userId := middleware.GetUserId(c)
	if userId == "" {
		log.CtxWarn(ctx, "http send message unauthorized: path=%s", c.Path())
		response.ErrorWithCode(ctx, c, errcode.ErrUnauthorized)
		return
	}

	var req service.SendMessageRequest
	if err := c.BindAndValidate(&req); err != nil {
		log.CtxWarn(ctx, "http send message invalid request: user_id=%s path=%s err=%v", userId, c.Path(), err)
		response.ErrorWithCode(ctx, c, errcode.ErrInvalidParam)
		return
	}
	log.CtxDebug(ctx, "http send message request accepted: user_id=%s path=%s session_type=%d recv_id=%s group_id=%s client_msg_id=%s", userId, c.Path(), req.SessionType, req.RecvId, req.GroupId, req.ClientMsgId)

	if h.sendGate != nil {
		release, err := h.sendGate.AcquireSendLease()
		if err != nil {
			log.CtxWarn(ctx, "http send message gate rejected: user_id=%s path=%s err=%v", userId, c.Path(), err)
			response.Error(ctx, c, err)
			return
		}
		defer release()
	}

	msg, err := h.msgService.SendMessage(ctx, userId, &req)
	if err != nil {
		log.CtxWarn(ctx, "http send message service failed: user_id=%s path=%s client_msg_id=%s err=%v", userId, c.Path(), req.ClientMsgId, err)
		response.Error(ctx, c, err)
		return
	}
	log.CtxDebug(ctx, "http send message succeeded: user_id=%s path=%s conversation_id=%s seq=%d client_msg_id=%s", userId, c.Path(), msg.ConversationId, msg.Seq, msg.ClientMsgId)

	response.Success(ctx, c, msg.ToMessageInfo())
}

// PullMessages handles pull messages request
func (h *MessageHandler) PullMessages(ctx context.Context, c *app.RequestContext) {
	userId := middleware.GetUserId(c)
	if userId == "" {
		log.CtxWarn(ctx, "http pull messages unauthorized: path=%s", c.Path())
		response.ErrorWithCode(ctx, c, errcode.ErrUnauthorized)
		return
	}

	conversationId := c.Query("conversation_id")
	if conversationId == "" {
		log.CtxWarn(ctx, "http pull messages missing conversation id: user_id=%s path=%s", userId, c.Path())
		response.ErrorWithCode(ctx, c, errcode.ErrInvalidParam)
		return
	}

	beginSeq, _ := strconv.ParseInt(c.Query("begin_seq"), 10, 64)
	endSeq, _ := strconv.ParseInt(c.Query("end_seq"), 10, 64)
	limit, _ := strconv.Atoi(c.Query("limit"))

	req := &service.PullMessagesRequest{
		ConversationId: conversationId,
		BeginSeq:       beginSeq,
		EndSeq:         endSeq,
		Limit:          limit,
	}

	messages, maxSeq, err := h.msgService.PullMessages(ctx, userId, req)
	if err != nil {
		log.CtxWarn(ctx, "http pull messages service failed: user_id=%s path=%s conversation_id=%s begin_seq=%d end_seq=%d limit=%d err=%v", userId, c.Path(), conversationId, beginSeq, endSeq, limit, err)
		response.Error(ctx, c, err)
		return
	}
	log.CtxDebug(ctx, "http pull messages succeeded: user_id=%s path=%s conversation_id=%s message_count=%d max_seq=%d", userId, c.Path(), conversationId, len(messages), maxSeq)

	msgInfos := make([]*any, 0, len(messages))
	for _, msg := range messages {
		info := msg.ToMessageInfo()
		msgInfos = append(msgInfos, func() *any { var i any = info; return &i }())
	}

	response.Success(ctx, c, map[string]any{
		"messages": msgInfos,
		"max_seq":  maxSeq,
	})
}

// GetMaxSeqRequest represents get max seq request
type GetMaxSeqRequest struct {
	ConversationId string `json:"conversation_id"`
}

// GetMaxSeq handles get max seq request
func (h *MessageHandler) GetMaxSeq(ctx context.Context, c *app.RequestContext) {
	userId := middleware.GetUserId(c)
	if userId == "" {
		log.CtxWarn(ctx, "http get max seq unauthorized: path=%s", c.Path())
		response.ErrorWithCode(ctx, c, errcode.ErrUnauthorized)
		return
	}

	conversationId := c.Query("conversation_id")
	if conversationId == "" {
		log.CtxWarn(ctx, "http get max seq missing conversation id: user_id=%s path=%s", userId, c.Path())
		response.ErrorWithCode(ctx, c, errcode.ErrInvalidParam)
		return
	}

	maxSeq, err := h.msgService.GetMaxSeq(ctx, userId, conversationId)
	if err != nil {
		log.CtxWarn(ctx, "http get max seq service failed: user_id=%s path=%s conversation_id=%s err=%v", userId, c.Path(), conversationId, err)
		response.Error(ctx, c, err)
		return
	}
	log.CtxDebug(ctx, "http get max seq succeeded: user_id=%s path=%s conversation_id=%s max_seq=%d", userId, c.Path(), conversationId, maxSeq)

	response.Success(ctx, c, map[string]interface{}{
		"max_seq": maxSeq,
	})
}
