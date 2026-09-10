package bus

import (
	"context"
	"encoding/json/jsontext"
)

// Event types (design §6.1). Delivery is at-most-once; message Resync and periodic
// presence reads cover lost windows.
const (
	TypePush            = "push"
	TypeKick            = "kick"
	TypeGroupChanged    = "group_changed"
	TypeConvRead        = "conv_read"
	TypePresenceChanged = "presence_changed"
)

// MaxPayloadBytes bounds presence events and is the cutoff above which a push uses
// reference form (PG NOTIFY caps payloads at 8000).
const MaxPayloadBytes = 7500

type Event struct {
	Type    string         `json:"type"`
	NodeId  string         `json:"node_id"`
	Payload jsontext.Value `json:"payload"`
}

type Bus interface {
	// Publish broadcasts ev to the other nodes. A nil return means only that no failure was
	// detected, never that every node received ev. It returns errcode.ErrBusFailed when the
	// driver positively knows a subscriber missed the event — a full local subscriber queue,
	// a Redis PUBLISH reporting zero receivers — and a wrapped driver error (encoding,
	// connection, command) when the publish itself failed. Both are reported and neither is
	// retried; a committed message's ACK does not change because publishing failed (design §6.1).
	Publish(ctx context.Context, ev Event) error
	// DegradedPublishes reports this instance's cumulative count of publishes the driver knew
	// were degraded, i.e. the occasions Publish returned errcode.ErrBusFailed: local counts one
	// per full subscriber queue, so a single Publish may count several; redis counts one per
	// PUBLISH that reported zero receivers. ok is false for a driver with no receiver signal at
	// all (PG NOTIFY), which must return the zero value so callers report "unavailable" instead
	// of reading a healthy-looking 0 (design §6.1). Counters are per instance and reset on
	// restart; they are a failure signal, not a message-loss rate.
	DegradedPublishes() (n int64, ok bool)
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

// PresenceChanged requests a fresh OnlineStore read; it does not describe online state.
type PresenceChanged struct {
	UserIds []string `json:"user_ids"`
}
