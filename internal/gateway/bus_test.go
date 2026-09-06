package gateway

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/bus"
	buslocal "github.com/mbeoliero/nexo/internal/bus/local"
	"github.com/mbeoliero/nexo/internal/service/conversation"
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

// twoNodes builds two gateways that share one store and one local bus, i.e. two
// nodes behind a load balancer, each with its own subscriber running.
func twoNodes(t *testing.T) (*Gateway, *Gateway) {
	t.Helper()
	m := storetest.NewMem()
	for _, id := range []string{"u___1", "u___2"} {
		_ = m.UpsertUser(t.Context(), &store.User{Id: id, UpdatedAt: time.Now()})
	}
	b := buslocal.New()
	node := func(id string) *Gateway {
		cfg := testConfig()
		cfg.NodeId = id
		g := New(cfg, Deps{Auth: auth.NewExternal([]string{"ext"}, "user"), Bus: b,
			Message: message.New(message.Adapt(m), message.NewBusPublisher(b, id), 8192), Conv: conversation.New(m, conversation.NewBusNotifier(b, id))})
		done := make(chan error, 1)
		go func() { done <- g.Run(t.Context()) }()
		t.Cleanup(func() {
			shutdownBusGateway(t, g)
			waitBusExit(t, done)
		})
		select {
		case <-g.Ready():
		case <-time.After(2 * time.Second):
			t.Fatal("bus subscription not ready")
		}
		return g
	}
	return node("n1"), node("n2")
}

func shutdownBusGateway(t *testing.T, g *Gateway) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := g.Shutdown(ctx); err != nil {
		t.Errorf("gateway workers did not stop: %v", err)
	}
}

func waitBusExit(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("bus subscription exit: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("bus subscription did not stop")
	}
}

func TestTwoNodesCleanup(t *testing.T) {
	var nodes []*Gateway
	t.Run("start", func(t *testing.T) {
		a, b := twoNodes(t)
		nodes = []*Gateway{a, b}
	})
	for _, g := range nodes {
		if g.ctx.Err() == nil {
			t.Errorf("node %s delivery context still active", g.cfg.NodeId)
		}
	}
}

func TestPushCrossesNodes(t *testing.T) {
	n1, n2 := twoNodes(t)
	sender := serveOn(t, n1, "u___1", 1)
	senderOther := serveOn(t, n2, "u___1", 2)
	peer := serveOn(t, n2, "u___2", 1)

	sender.in <- []byte(`{"req_id":1003,"data":{"client_msg_id":"c1","session_type":1,"recv_id":"u___2","content_type":1,"content":"{}"}}`)
	if r := sender.next(t); r.Code != 0 {
		t.Fatalf("ack: %+v", r)
	}
	for name, f := range map[string]*fakeConn{"peer@n2": peer, "senderOther@n2": senderOther} {
		if r := f.next(t); r.ReqId != PushMsg || !strings.Contains(fmtData(r), "seq:1") {
			t.Fatalf("%s: %+v", name, r)
		}
	}
	sender.quiet(t)
}

func TestLargePushGoesByReference(t *testing.T) {
	n1, n2 := twoNodes(t)
	sender := serveOn(t, n1, "u___1", 1)
	peer := serveOn(t, n2, "u___2", 1)
	content := `{"text":"` + strings.Repeat("x", 8000) + `"}`

	var seen bus.Event
	probe := buslocal.New()
	ready := make(chan struct{})
	seenCh := make(chan bus.Event, 1)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- probe.Subscribe(ctx, func(ev bus.Event) { seenCh <- ev }, func() { close(ready) }) }()
	t.Cleanup(func() { cancel(); waitBusExit(t, done) })
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("probe subscription not ready")
	}
	message.NewBusPublisher(probe, "probe").Publish(t.Context(), message.PushEvent{ConversationId: "c", SessionType: 1, SenderId: "u___1", RecvId: "u___2", Message: message.Message{Seq: 1, Content: content}})
	select {
	case seen = <-seenCh:
	case <-time.After(2 * time.Second):
		t.Fatal("probe did not receive published event")
	}
	if len(seen.Payload) > bus.MaxPayloadBytes || !strings.Contains(string(seen.Payload), `"ref":true`) || strings.Contains(string(seen.Payload), "xxxx") {
		t.Fatalf("payload not in ref form: %d bytes", len(seen.Payload))
	}

	sender.in <- []byte(`{"req_id":1003,"data":{"client_msg_id":"c1","session_type":1,"recv_id":"u___2","content_type":1,"content":` + quote(content) + `}}`)
	if r := sender.next(t); r.Code != 0 {
		t.Fatalf("ack: %+v", r)
	}
	if r := peer.next(t); r.ReqId != PushMsg || !strings.Contains(fmtData(r), strings.Repeat("x", 8000)) {
		t.Fatalf("peer must get the full message read back from the store: %+v", r.ReqId)
	}
}

func quote(s string) string { return `"` + strings.ReplaceAll(s, `"`, `\"`) + `"` }

func TestKickCrossesNodes(t *testing.T) {
	n1, n2 := twoNodes(t)
	_, old := serveAs(t, n1, auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "t1"}, "c1")
	sameCl, same := serveAs(t, n2, auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "t2"}, "c2")

	n2.publishKick(sameCl)
	expectKick(t, old, KickNewLogin)
	same.quiet(t)
}

func TestConvReadCrossesNodes(t *testing.T) {
	n1, n2 := twoNodes(t)
	a := serveOn(t, n1, "u___1", 1)
	b := serveOn(t, n2, "u___1", 2)
	peer := serveOn(t, n1, "u___2", 1)

	peer.in <- []byte(`{"req_id":1003,"data":{"client_msg_id":"c1","session_type":1,"recv_id":"u___1","content_type":1,"content":"{}"}}`)
	conv := peer.next(t).Data.(map[string]any)["conversation_id"].(string)
	a.next(t)
	b.next(t)
	a.in <- []byte(`{"req_id":1004,"data":{"conversation_id":"` + conv + `","read_seq":1}}`)
	if r := b.next(t); r.ReqId != ConvRead || !strings.Contains(fmtData(r), "read_seq:1") {
		t.Fatalf("device on other node: %+v", r)
	}
}

type flappingBus struct {
	connects int
	ready    chan struct{}
}

func (f *flappingBus) Publish(context.Context, bus.Event) error { return nil }
func (f *flappingBus) Subscribe(ctx context.Context, _ func(bus.Event), onConnected func()) error {
	for range f.connects {
		onConnected()
	}
	close(f.ready)
	<-ctx.Done()
	return nil
}

func TestResyncOnlyAfterReconnect(t *testing.T) {
	for name, connects := range map[string]int{"initial": 1, "reconnected": 2} {
		t.Run(name, func(t *testing.T) {
			b := &flappingBus{connects: connects, ready: make(chan struct{})}
			g := New(testConfig(), Deps{Bus: b})
			f := serveOn(t, g, "u___1", 1)
			done := make(chan error, 1)
			go func() { done <- g.Run(t.Context()) }()
			t.Cleanup(func() { shutdownBusGateway(t, g); waitBusExit(t, done) })
			select {
			case <-b.ready:
			case <-time.After(2 * time.Second):
				t.Fatal("bus connect callbacks did not finish")
			}
			shutdownBusGateway(t, g) // Flush queued frames before asserting their absence.
			if connects == 2 {
				if r := f.next(t); r.ReqId != Resync || !strings.Contains(fmtData(r), "bus_reconnected") {
					t.Fatalf("want 2004 after reconnect: %+v", r)
				}
			}
			select {
			case raw := <-f.out:
				t.Fatalf("unexpected frame: %s", raw)
			default:
			}
		})
	}
}

// A payload this node cannot read means a publisher on another node disagrees about the wire
// format. All three of these branches used to swallow the error, so a rolling upgrade could stop
// delivering kicks and read receipts with nothing in the logs.
func TestUndecodableBusPayloadIsCounted(t *testing.T) {
	g := newGateway(t, testConfig())
	for _, ev := range []bus.Event{
		{Type: bus.TypeKick, NodeId: "n2", Payload: []byte(`{"platform_id":`)},
		{Type: bus.TypeConvRead, NodeId: "n2", Payload: []byte(`not json`)},
		{Type: bus.TypeGroupChanged, NodeId: "n2", Payload: []byte(`{"group_id":7}`)},
		{Type: bus.TypePush, NodeId: "n2", Payload: []byte(`{`)},
	} {
		g.onEvent(t.Context(), ev)
	}
	if got := g.Stats().DecodeFails; got != 4 {
		t.Fatalf("DecodeFails = %d, want 4", got)
	}
}

// A full shard drops the event rather than blocking the bus consumer: at-most-once is the design's
// delivery contract (§6.1), and the client re-pulls. The drop has to be visible in Stats, though,
// or an overloaded node looks identical to an idle one.
func TestFullDeliverShardDropsAndCounts(t *testing.T) {
	cfg := testConfig()
	cfg.Ws.DeliverWorkers, cfg.Ws.DeliverQueue = 1, 1
	g := newGateway(t, cfg) // no startDeliver: nothing drains the shard
	for range 3 {
		g.onEvent(t.Context(), bus.Event{Type: bus.TypePush, NodeId: "n2", Payload: []byte(`{"conversation_id":"sg_g1","seq":1}`)})
	}
	if s := g.Stats(); s.PushDropped != 2 || s.DecodeFails != 0 {
		t.Fatalf("stats = %+v, want PushDropped 2", s)
	}
}

// Sharding is only useful if one conversation always lands on one worker: two events for the same
// conversation must never be delivered out of order by racing workers.
func TestShardOfIsStablePerConversation(t *testing.T) {
	for _, id := range []string{"sg_g1", "si_u___1:u___2", ""} {
		first := shardOf(id, 8)
		if first < 0 || first >= 8 {
			t.Fatalf("shardOf(%q) = %d, out of range", id, first)
		}
		for range 5 {
			if got := shardOf(id, 8); got != first {
				t.Fatalf("shardOf(%q) = %d then %d", id, first, got)
			}
		}
	}
}
