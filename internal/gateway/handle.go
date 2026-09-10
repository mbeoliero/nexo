package gateway

import (
	"context"
	"encoding/json/v2"

	"github.com/mbeoliero/kit/log"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/service/dto"
	"github.com/mbeoliero/nexo/internal/service/message"
)

func (g *Gateway) dispatch(c *Client, req Request) []byte {
	ctx := c.ctx()
	data, err := g.handle(ctx, c, req)
	if err != nil {
		if errcode.IsSystem(err) {
			log.CtxError(ctx, "ws req_id=%d: %v", req.ReqId, err)
		} else {
			log.CtxInfo(ctx, "ws req_id=%d: %v", req.ReqId, err)
		}
		return req.fail(err)
	}
	return req.reply(data)
}

func (g *Gateway) handle(ctx context.Context, c *Client, req Request) (any, error) {
	switch req.ReqId {
	case ReqSetOnlineSubscriptions:
		var in onlineSubscriptionRequest
		if err := bind(req, &in); err != nil {
			return nil, err
		}
		return g.setOnlineSubscriptions(c, in)
	case ReqGetMaxSeqs:
		var in struct {
			Cursor string `json:"cursor"`
			Limit  int    `json:"limit"`
		}
		if err := bind(req, &in); err != nil {
			return nil, err
		}
		return g.deps.Message.MaxSeqs(ctx, c.UserId, in.Cursor, in.Limit, g.cfg.Limits.MaxSeqsPageMax)
	case ReqPullMsgBySeqRange:
		var in struct {
			ConversationId string `json:"conversation_id"`
			BeginSeq       int64  `json:"begin_seq"`
			EndSeq         int64  `json:"end_seq"`
			Limit          int    `json:"limit"`
		}
		if err := bind(req, &in); err != nil {
			return nil, err
		}
		return g.deps.Message.Pull(ctx, message.PullInput{UserId: c.UserId, ConversationId: in.ConversationId, BeginSeq: in.BeginSeq, EndSeq: in.EndSeq, Limit: in.Limit}, g.cfg.Limits.PullPageMax)
	case ReqSendMsg:
		var in dto.SendRequest
		if err := bind(req, &in); err != nil {
			return nil, err
		}
		return g.deps.Message.Send(ctx, message.SendInput{
			SenderId: c.UserId, SenderConnId: c.Id, ClientMsgId: in.ClientMsgId, SessionType: in.SessionType, RecvId: in.RecvId, GroupId: in.GroupId,
			ContentType: in.ContentType, Content: in.Content, SenderRead: in.SenderReadFor(c.Source == auth.SourceInternal),
		})
	case ReqMarkRead:
		var in struct {
			ConversationId string `json:"conversation_id"`
			ReadSeq        int64  `json:"read_seq"`
		}
		if err := bind(req, &in); err != nil {
			return nil, err
		}
		if in.ConversationId == "" {
			return nil, errcode.ErrInvalidParam.WithMessage("conversation_id is required")
		}
		seq, err := g.deps.Conv.MarkRead(ctx, c.UserId, c.Id, in.ConversationId, in.ReadSeq)
		if err != nil {
			return nil, err
		}
		return map[string]int64{"read_seq": seq}, nil
	default:
		return nil, errcode.ErrInvalidProtocol.WithMessage("unknown req_id")
	}
}

func bind(req Request, v any) error {
	if len(req.Data) == 0 {
		return nil
	}
	if err := json.Unmarshal(req.Data, v); err != nil {
		return errcode.ErrInvalidParam.Wrap(err)
	}
	return nil
}
