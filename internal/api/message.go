package api

import (
	"context"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/mbeoliero/nexo/internal/api/webx"
	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/service/conversation"
	"github.com/mbeoliero/nexo/internal/service/dto"
	"github.com/mbeoliero/nexo/internal/service/message"
)

type messageHandler struct {
	svc            *message.Service
	pullPageMax    int
	convPageMax    int
	maxSeqsPageMax int
	conversation   *conversation.Service
}

func (h messageHandler) send(ctx context.Context, c *app.RequestContext) {
	req, ok := webx.Bind[dto.SendRequest](ctx, c)
	if !ok {
		return
	}
	id := webx.IdentityFrom(c)
	ack, err := h.svc.Send(ctx, message.SendInput{
		SenderId: id.UserId, ClientMsgId: req.ClientMsgId, SessionType: req.SessionType, RecvId: req.RecvId, GroupId: req.GroupId,
		ContentType: req.ContentType, Content: req.Content,
		SenderRead: req.SenderReadFor(id.Source), Unlimited: id.Source == auth.SourceInternal,
	})
	webx.Respond(ctx, c, ack, err)
}

func queryInt(c *app.RequestContext, key string) int64 {
	n, _ := strconv.ParseInt(c.Query(key), 10, 64)
	return n
}

func (h messageHandler) pull(ctx context.Context, c *app.RequestContext) {
	res, err := h.svc.Pull(ctx, message.PullInput{
		UserId: webx.IdentityFrom(c).UserId, ConversationId: c.Query("conversation_id"),
		BeginSeq: queryInt(c, "begin_seq"), EndSeq: queryInt(c, "end_seq"), Limit: int(queryInt(c, "limit")),
	}, h.pullPageMax)
	webx.Respond(ctx, c, res, err)
}

func (h messageHandler) maxSeqs(ctx context.Context, c *app.RequestContext) {
	res, err := h.svc.MaxSeqs(ctx, webx.IdentityFrom(c).UserId, c.Query("cursor"), int(queryInt(c, "limit")), h.maxSeqsPageMax)
	webx.Respond(ctx, c, res, err)
}

func (h messageHandler) list(ctx context.Context, c *app.RequestContext) {
	res, err := h.conversation.List(ctx, webx.IdentityFrom(c).UserId, c.Query("cursor"), int(queryInt(c, "limit")), h.convPageMax, c.Query("with_last_message") == "true")
	webx.Respond(ctx, c, res, err)
}

type readReq struct {
	ConversationId string `json:"conversation_id"`
	ReadSeq        int64  `json:"read_seq"`
}

func (h messageHandler) read(ctx context.Context, c *app.RequestContext) {
	req, ok := webx.BindValid(ctx, c, "conversation_id and read_seq are required", func(r readReq) bool { return r.ConversationId != "" })
	if !ok {
		return
	}
	seq, err := h.conversation.MarkRead(ctx, webx.IdentityFrom(c).UserId, "", req.ConversationId, req.ReadSeq)
	webx.Respond(ctx, c, map[string]int64{"read_seq": seq}, err)
}

type optReq struct {
	ConversationId string `json:"conversation_id"`
	conversation.Opt
}

func (h messageHandler) opt(ctx context.Context, c *app.RequestContext) {
	req, ok := webx.BindValid(ctx, c, "conversation_id is required", func(r optReq) bool { return r.ConversationId != "" })
	if !ok {
		return
	}
	webx.Respond(ctx, c, nil, h.conversation.SetOpt(ctx, webx.IdentityFrom(c).UserId, req.ConversationId, req.Opt))
}
