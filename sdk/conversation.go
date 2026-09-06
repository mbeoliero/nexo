package sdk

import (
	"context"
	"net/url"
	"strconv"
)

func listQuery(req ListConversationsRequest) url.Values {
	q := url.Values{}
	if req.Cursor != "" {
		q.Set("cursor", req.Cursor)
	}
	if req.Limit > 0 {
		q.Set("limit", strconv.Itoa(req.Limit))
	}
	if req.WithLastMessage {
		q.Set("with_last_message", "true")
	}
	return q
}

func (c *Client) Conversations(ctx context.Context, req ListConversationsRequest) (ConversationList, error) {
	var r ConversationList
	return r, c.get(ctx, "/conversation/list", listQuery(req), &r)
}

// MarkRead returns the effective read_seq (never moves backwards).
func (c *Client) MarkRead(ctx context.Context, conversationId string, readSeq int64) (int64, error) {
	var out struct {
		ReadSeq int64 `json:"read_seq"`
	}
	err := c.post(ctx, "/conversation/read", map[string]any{"conversation_id": conversationId, "read_seq": readSeq}, &out)
	return out.ReadSeq, err
}

func (c *Client) SetConversationOpt(ctx context.Context, conversationId string, opt ConversationOpt) error {
	type body struct {
		ConversationId string `json:"conversation_id"`
		ConversationOpt
	}
	return c.put(ctx, "/conversation/opt", body{ConversationId: conversationId, ConversationOpt: opt}, nil)
}

func (c *Client) InternalConversations(ctx context.Context, req ListConversationsRequest, opts ...RequestOption) (ConversationList, error) {
	var r ConversationList
	return r, c.internalGet(ctx, "/internal/conversation/list", listQuery(req), &r, opts)
}
