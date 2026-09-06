package message

import (
	"context"
	"encoding/json/v2"
	"errors"

	"github.com/mbeoliero/kit/log"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/bus"
	"github.com/mbeoliero/nexo/internal/store"
)

// PushPayload is the wire form of a push event. Ref=true omits Message and the
// receiver reads it back by (conversation_id, seq) (design §6.1 size rule).
type PushPayload struct {
	ConversationId string   `json:"conversation_id"`
	Seq            int64    `json:"seq"`
	SessionType    int32    `json:"session_type"`
	SenderId       string   `json:"sender_id"`
	SenderConnId   string   `json:"sender_conn_id,omitempty"`
	RecvId         string   `json:"recv_id,omitempty"`
	GroupId        string   `json:"group_id,omitempty"`
	Ref            bool     `json:"ref,omitzero"`
	Msg            *Message `json:"msg,omitempty"`
}

type busPublisher struct {
	bus    bus.Bus
	nodeId string
}

// NewBusPublisher turns committed sends into bus push events; the sending node
// receives its own event and delivers locally like any other node.
func NewBusPublisher(b bus.Bus, nodeId string) Publisher {
	return busPublisher{bus: b, nodeId: nodeId}
}

func (p busPublisher) Publish(ctx context.Context, ev PushEvent) {
	payload := PushPayload{ConversationId: ev.ConversationId, Seq: ev.Message.Seq, SessionType: ev.SessionType, SenderId: ev.SenderId,
		SenderConnId: ev.SenderConnId, RecvId: ev.RecvId, GroupId: ev.GroupId, Msg: &ev.Message}
	raw, err := json.Marshal(payload)
	if err == nil && len(raw) > bus.MaxPayloadBytes {
		payload.Ref, payload.Msg = true, nil
		raw, err = json.Marshal(payload)
	}
	if err == nil {
		err = p.bus.Publish(ctx, bus.Event{Type: bus.TypePush, NodeId: p.nodeId, Payload: raw})
	}
	if err != nil {
		log.CtxError(ctx, "bus publish push conv=%s seq=%d: %v", ev.ConversationId, ev.Message.Seq, errcode.ErrBusFailed.Wrap(err))
	}
}

// ResolvePush rebuilds a PushEvent from a decoded payload, reading the message from the store when
// the payload is a reference. It is the half of push decoding that can touch the DB, so the gateway
// runs it on a delivery worker rather than on its single bus consumer goroutine.
func (s *Service) ResolvePush(ctx context.Context, p PushPayload) (PushEvent, error) {
	out := PushEvent{ConversationId: p.ConversationId, SessionType: p.SessionType, SenderId: p.SenderId, SenderConnId: p.SenderConnId, RecvId: p.RecvId, GroupId: p.GroupId}
	if p.Msg != nil {
		out.Message = *p.Msg
		return out, nil
	}
	msgs, err := s.store.GetMessages(ctx, []store.MessageKey{{ConversationId: p.ConversationId, Seq: p.Seq}})
	if err != nil {
		return PushEvent{}, errcode.ErrStoreFailed.Wrap(err)
	}
	if len(msgs) == 0 {
		return PushEvent{}, errcode.ErrMessageNotFound.Wrap(errors.New("push ref not found"))
	}
	out.Message = FromStore(msgs[0])
	return out, nil
}
