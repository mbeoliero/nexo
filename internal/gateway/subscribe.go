package gateway

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mbeoliero/kit/log"
	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/internal/bus"
	"github.com/mbeoliero/nexo/internal/service/message"
)

// Run owns the node-level loops: purge stale online rows, renew on a timer, consume
// the Bus. It returns when ctx is done or the Bus subscription fails.
func (g *Gateway) Run(ctx context.Context) error {
	if !g.beginWork() {
		return nil
	}
	defer g.work.Done()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(g.runCtx, cancel)
	if g.runCtx.Err() != nil {
		cancel()
	}
	var renew sync.WaitGroup
	defer func() { stop(); cancel(); renew.Wait() }()
	if g.deps.Online != nil {
		if err := g.deps.Online.PurgeNode(ctx, g.cfg.NodeId); err != nil {
			log.CtxWarn(ctx, "onlinestore purge node=%s: %v", g.cfg.NodeId, err)
		}
		renew.Go(func() { g.renewLoop(ctx) })
	}
	return g.Subscribe(ctx)
}

// renewFailsBeforeError is how many consecutive renew failures turn the log line into an error.
// One is a blip; renewFailsBeforeError in a row means every connection on this node has passed
// online_store.ttl and presence is silently wrong.
const renewFailsBeforeError = 3

func (g *Gateway) renewLoop(ctx context.Context) {
	ticker := time.NewTicker(cmp.Or(g.cfg.OnlineStore.RenewInterval, 20*time.Second))
	defer ticker.Stop()
	fails := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := g.renew(ctx)
			if err == nil {
				fails = 0
				continue
			}
			fails++
			logAt := lo.Ternary(fails >= renewFailsBeforeError, log.CtxError, log.CtxWarn)
			logAt(ctx, "onlinestore renew %d conns (%d consecutive failures): %v", n, fails, err)
		}
	}
}

// Subscribe runs the node's Bus consumer until ctx is done. Every event, including
// the node's own, takes the same delivery path (design §6.1).
func (g *Gateway) Subscribe(ctx context.Context) error {
	if g.deps.Bus == nil {
		g.readyOnce.Do(func() { close(g.ready) })
		<-ctx.Done()
		return nil
	}
	g.startDeliver()
	var connects atomic.Int32
	return g.deps.Bus.Subscribe(ctx, func(ev bus.Event) { g.onEvent(ctx, ev) }, func() {
		g.readyOnce.Do(func() { close(g.ready) })
		if connects.Add(1) > 1 {
			log.CtxWarn(ctx, "bus reconnected; resyncing %d local connections", g.users.Count())
			g.ResyncAll("bus_reconnected")
		}
	})
}

func (g *Gateway) onEvent(ctx context.Context, ev bus.Event) {
	ctx = log.AppendLogKv(ctx, "bus_event", ev.Type)
	switch ev.Type {
	case bus.TypePush:
		if p, ok := decodeBus[message.PushPayload](ctx, g, ev); ok {
			g.enqueuePush(ctx, p)
		}
	case bus.TypeKick:
		if k, ok := decodeBus[bus.Kick](ctx, g, ev); ok {
			g.Kick(k.UserId, k.PlatformId, k.KeepTokenId)
		}
	case bus.TypeConvRead:
		if r, ok := decodeBus[bus.ConvRead](ctx, g, ev); ok {
			g.ConversationRead(ctx, r.UserId, r.ReaderConnId, r.ConversationId, r.ReadSeq)
		}
	case bus.TypeGroupChanged:
		if gc, ok := decodeBus[bus.GroupChanged](ctx, g, ev); ok {
			g.deps.Message.InvalidateGroup(gc.GroupId)
		}
	default:
		log.CtxWarn(ctx, "bus: unknown event type %q from %s", ev.Type, ev.NodeId)
	}
}

// decodeBus unmarshals an event payload. A failure is a real defect — a publisher on another node
// disagrees with this one about the wire format — so it is logged and counted rather than silently
// skipped, which is how all three of these branches used to behave.
func decodeBus[T any](ctx context.Context, g *Gateway, ev bus.Event) (T, bool) {
	var v T
	if err := json.Unmarshal(ev.Payload, &v); err != nil {
		g.decodeFails.Add(1)
		log.CtxError(ctx, "bus %s decode from=%s: %v", ev.Type, ev.NodeId, err)
		return v, false
	}
	return v, true
}

// ResyncAll pushes 2004 to every local connection; clients re-pull by seq.
func (g *Gateway) ResyncAll(reason string) {
	frame := pushFrame(Resync, map[string]string{"reason": reason})
	for _, c := range g.users.All() {
		_ = c.Send(frame)
	}
}

// publishKick broadcasts the same-platform kick for a freshly opened connection; without a Bus it
// applies locally. It runs from the WS upgrade callback, so it takes a connection-scoped context.
func (g *Gateway) publishKick(c *Client) {
	if c.activeCtx.Err() != nil || g.closing.Load() {
		return
	}
	userId, platformId, keepTokenId := c.UserId, c.PlatformId, c.TokenId
	if g.deps.Bus == nil {
		g.kick(c.activeCtx, userId, platformId, keepTokenId)
		return
	}
	ctx, cancel := connOp(c, c.activeCtx)
	defer cancel()
	if ctx.Err() != nil {
		return
	}
	raw, _ := json.Marshal(bus.Kick{UserId: userId, PlatformId: platformId, KeepTokenId: keepTokenId})
	if err := g.deps.Bus.Publish(ctx, bus.Event{Type: bus.TypeKick, NodeId: g.cfg.NodeId, Payload: raw}); err != nil {
		if ctx.Err() != nil || c.activeCtx.Err() != nil {
			return
		}
		log.CtxError(ctx, "bus publish kick user=%s: %v", userId, err)
		g.kick(ctx, userId, platformId, keepTokenId)
	}
}
