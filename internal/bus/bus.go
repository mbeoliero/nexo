package bus

import (
	"context"
	"encoding/json/jsontext"
)

// Event types (design §6.1). Delivery is at-most-once; Resync covers lost windows.
const (
	TypePush         = "push"
	TypeKick         = "kick"
	TypeGroupChanged = "group_changed"
	TypeConvRead     = "conv_read"
)

// MaxPayloadBytes is the cutoff above which a push is sent in reference form and the
// receiving node reads the message back from the DB (PG NOTIFY caps payloads at 8000).
const MaxPayloadBytes = 7500

type Event struct {
	Type    string         `json:"type"`
	NodeId  string         `json:"node_id"`
	Payload jsontext.Value `json:"payload"`
}

type Bus interface {
	Publish(ctx context.Context, ev Event) error
	// Subscribe blocks until ctx is done and reconnects internally. onConnected runs
	// after every (re)connect, including the first; the gateway resyncs on all but the first.
	Subscribe(ctx context.Context, onEvent func(Event), onConnected func()) error
}

type Kick struct {
	UserId      string `json:"user_id"`
	PlatformId  int    `json:"platform_id"`
	KeepTokenId string `json:"keep_token_id"`
}

type GroupChanged struct {
	GroupId string `json:"group_id"`
}

type ConvRead struct {
	UserId         string `json:"user_id"`
	ReaderConnId   string `json:"reader_conn_id,omitempty"`
	ConversationId string `json:"conversation_id"`
	ReadSeq        int64  `json:"read_seq"`
}
