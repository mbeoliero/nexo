// Package dto holds response types shared by more than one service package (AGENTS.md rule 5:
// service packages do not import each other).
package dto

import (
	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/store"
)

type Message struct {
	ServerMsgId    string `json:"server_msg_id"`
	ClientMsgId    string `json:"client_msg_id"`
	ConversationId string `json:"conversation_id"`
	Seq            int64  `json:"seq"`
	SessionType    int32  `json:"session_type"`
	SenderId       string `json:"sender_id"`
	RecvId         string `json:"recv_id,omitempty"`
	GroupId        string `json:"group_id,omitempty"`
	ContentType    int32  `json:"content_type"`
	Content        string `json:"content"`
	SendTime       int64  `json:"send_time"`
}

func MessageFromStore(m store.Message) Message {
	return Message{
		ServerMsgId: m.ServerMsgId, ClientMsgId: m.ClientMsgId, ConversationId: m.ConversationId, Seq: m.Seq, SessionType: m.SessionType,
		SenderId: m.SenderId, RecvId: m.RecvId, GroupId: m.GroupId, ContentType: m.ContentType, Content: m.Content, SendTime: m.SendTime.UnixMilli(),
	}
}

// SendRequest is the wire shape of a send over HTTP and WS; both handlers bind into it.
type SendRequest struct {
	ClientMsgId string `json:"client_msg_id"`
	SessionType int32  `json:"session_type"`
	RecvId      string `json:"recv_id"`
	GroupId     string `json:"group_id"`
	ContentType int32  `json:"content_type"`
	Content     string `json:"content"`
	SenderRead  *bool  `json:"sender_read"`
}

// SenderReadFor resolves the sender_read default for a send authenticated as source (an
// auth.Source* constant): a platform send over the internal channel leaves the sender's own
// devices unread, a client send marks its own conversation read. It lives next to the wire type
// so the HTTP and WS handlers cannot drift into two versions of one product rule.
func (r SendRequest) SenderReadFor(source string) bool {
	return lo.FromPtrOr(r.SenderRead, source != auth.SourceInternal)
}
