package conversation

import (
	"context"
	"encoding/json/v2"

	"github.com/mbeoliero/kit/log"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/bus"
)

type busNotifier struct {
	bus    bus.Bus
	nodeId string
}

func NewBusNotifier(b bus.Bus, nodeId string) Notifier { return busNotifier{bus: b, nodeId: nodeId} }

func (n busNotifier) ConversationRead(ctx context.Context, ev ReadEvent) {
	raw, _ := json.Marshal(bus.ConvRead{UserId: ev.UserId, ReaderConnId: ev.ReaderConnId, ConversationId: ev.ConversationId, ReadSeq: ev.ReadSeq})
	if err := n.bus.Publish(ctx, bus.Event{Type: bus.TypeConvRead, NodeId: n.nodeId, Payload: raw}); err != nil {
		log.CtxError(ctx, "bus publish conv_read: %v", errcode.ErrBusFailed.Wrap(err))
	}
}
