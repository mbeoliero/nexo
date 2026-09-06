package offlinepush

import (
	"context"
	"strconv"

	"github.com/mbeoliero/nexo/msgbody"
)

// Notification carries facts only; the push side resolves names from its own user system and
// decides whether to parse Content (design §6.6, A7).
type Notification struct {
	ConversationId string `json:"conversation_id"`
	Seq            int64  `json:"seq"`
	SessionType    int32  `json:"session_type"`
	SenderId       string `json:"sender_id"`
	GroupId        string `json:"group_id,omitempty"`
	ContentType    int32  `json:"content_type"`
	Content        string `json:"content"`
	SendTime       int64  `json:"send_time"`
}

// EventId is the idempotency key receivers dedupe on.
func (n Notification) EventId() string { return n.ConversationId + ":" + strconv.FormatInt(n.Seq, 10) }

// Preview is the default text; "" for custom or unparsable content.
func (n Notification) Preview() string { return msgbody.Preview(n.ContentType, n.Content) }

type Pusher interface {
	Push(ctx context.Context, userIds []string, n Notification) error
}
