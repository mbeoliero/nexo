package gateway

import (
	"context"
	"hash/fnv"
	"time"

	"github.com/mbeoliero/kit/log"

	"github.com/mbeoliero/nexo/internal/bus"
	"github.com/mbeoliero/nexo/internal/service/message"
)

// deliverTimeout bounds one push: the ref lookup, the recipient query and the visibility query.
const deliverTimeout = 10 * time.Second

// startDeliver launches the push workers. They live until the gateway context is cancelled at the
// end of Shutdown; the channels are never closed, so a late event is dropped rather than panicking.
func (g *Gateway) startDeliver() {
	g.deliverOnce.Do(func() {
		for _, ch := range g.deliver {
			if !g.beginWork() {
				return
			}
			go func() { defer g.work.Done(); g.deliverWorker(ch) }()
		}
	})
}

func (g *Gateway) deliverWorker(ch <-chan message.PushPayload) {
	for {
		select {
		case <-g.ctx.Done():
			return
		case p := <-ch:
			g.deliverOne(p)
		}
	}
}

// enqueuePush hands the event to the shard that owns its conversation. Sharding by conversation
// keeps per-conversation ordering while stopping one slow recipient lookup from stalling unrelated
// conversations — and, before this existed, kicks and read receipts as well, since all four types
// shared the single bus consumer goroutine. A full shard drops the event, which is the at-most-once
// contract (design §6.1): the client recovers on its next pull or on Resync.
func (g *Gateway) enqueuePush(ctx context.Context, p message.PushPayload) {
	ch := g.deliver[shardOf(p.ConversationId, len(g.deliver))]
	select {
	case ch <- p:
	default:
		g.pushDropped.Add(1)
		log.CtxWarn(ctx, "push queue full conv=%s seq=%d: event dropped", p.ConversationId, p.Seq)
	}
}

func shardOf(key string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key)) // hash.Hash.Write never returns an error
	return int(h.Sum32() % uint32(n))
}

// deliverOne runs off the bus consumer goroutine, so it builds its own context: the consumer's is
// tied to Subscribe and carries that event's log fields, neither of which belongs to this worker.
func (g *Gateway) deliverOne(p message.PushPayload) {
	ctx, cancel := context.WithTimeout(log.AppendLogKv(g.ctx, "bus_event", bus.TypePush), deliverTimeout)
	defer cancel()
	ev, err := g.deps.Message.ResolvePush(ctx, p)
	if err != nil {
		log.CtxError(ctx, "push resolve conv=%s seq=%d: %v", p.ConversationId, p.Seq, err)
		return
	}
	g.Deliver(ctx, ev)
}

// Deliver resolves local recipients, re-checks visibility in the DB, and fans out 2001.
func (g *Gateway) Deliver(ctx context.Context, ev message.PushEvent) {
	candidates, err := g.deps.Message.Recipients(ctx, ev)
	if err != nil {
		log.CtxError(ctx, "push recipients conv=%s seq=%d: %v", ev.ConversationId, ev.Message.Seq, err)
		return
	}
	local := g.users.Online(candidates)
	if len(local) == 0 {
		return
	}
	targets, err := g.deps.Message.VisibleTo(ctx, ev.ConversationId, local, ev.Message.Seq)
	if err != nil {
		log.CtxError(ctx, "push visibility conv=%s seq=%d: %v", ev.ConversationId, ev.Message.Seq, err)
		return
	}
	frame := pushFrame(PushMsg, ev.Message)
	for _, userId := range targets {
		g.fanout(userId, ev.SenderConnId, frame)
	}
}

// ConversationRead pushes 2003 to the user's other devices (multi-device read sync).
func (g *Gateway) ConversationRead(ctx context.Context, userId, readerConnId, conversationId string, readSeq int64) {
	g.fanout(userId, readerConnId, pushFrame(ConvRead, map[string]any{"conversation_id": conversationId, "read_seq": readSeq}))
}

// fanout writes frame to every local connection of userId except the one that caused it.
func (g *Gateway) fanout(userId, exceptConnId string, frame []byte) {
	for _, c := range g.users.Get(userId) {
		if c.Id != exceptConnId {
			_ = c.Push(frame)
		}
	}
}
