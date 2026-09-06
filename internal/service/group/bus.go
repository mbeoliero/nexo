package group

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

func (n busNotifier) GroupChanged(ctx context.Context, groupId string) {
	raw, _ := json.Marshal(bus.GroupChanged{GroupId: groupId})
	if err := n.bus.Publish(ctx, bus.Event{Type: bus.TypeGroupChanged, NodeId: n.nodeId, Payload: raw}); err != nil {
		log.CtxError(ctx, "bus publish group_changed: %v", errcode.ErrBusFailed.Wrap(err))
	}
}
