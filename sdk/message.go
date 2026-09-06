package sdk

import (
	"context"
	"encoding/json/v2"
	"net/url"
	"strconv"
)

func (c *Client) SendMessage(ctx context.Context, req SendMessageRequest) (Ack, error) {
	var a Ack
	return a, c.post(ctx, "/message/send", req, &a)
}

// SendText is SendMessage with content_type=1 and content {"text": ...}.
func (c *Client) SendText(ctx context.Context, clientMsgId, recvId, text string) (Ack, error) {
	return c.SendMessage(ctx, SendMessageRequest{ClientMsgId: clientMsgId, SessionType: SessionTypeSingle, RecvId: recvId,
		ContentType: ContentTypeText, Content: textContent(text)})
}

func (c *Client) SendGroupText(ctx context.Context, clientMsgId, groupId, text string) (Ack, error) {
	return c.SendMessage(ctx, SendMessageRequest{ClientMsgId: clientMsgId, SessionType: SessionTypeGroup, GroupId: groupId,
		ContentType: ContentTypeText, Content: textContent(text)})
}

func textContent(text string) string {
	b, _ := json.Marshal(map[string]string{"text": text})
	return string(b)
}

func (c *Client) Pull(ctx context.Context, req PullRequest) (PullResult, error) {
	q := url.Values{"conversation_id": {req.ConversationId},
		"begin_seq": {strconv.FormatInt(req.BeginSeq, 10)}, "end_seq": {strconv.FormatInt(req.EndSeq, 10)}}
	if req.Limit > 0 {
		q.Set("limit", strconv.Itoa(req.Limit))
	}
	var r PullResult
	return r, c.get(ctx, "/message/pull", q, &r)
}

func (c *Client) MaxSeqs(ctx context.Context, cursor string, limit int) (MaxSeqsResult, error) {
	q := url.Values{}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var r MaxSeqsResult
	return r, c.get(ctx, "/message/max_seqs", q, &r)
}

// InternalSendMessage sends as AsUser(...); the platform default is sender_read=false.
func (c *Client) InternalSendMessage(ctx context.Context, req SendMessageRequest, opts ...RequestOption) (Ack, error) {
	var a Ack
	return a, c.internalPost(ctx, "/internal/message/send", req, &a, opts)
}
