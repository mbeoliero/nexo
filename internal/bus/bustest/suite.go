package bustest

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/bus"
)

// Run checks the contract shared by all Bus implementations: onConnected before the
// first event, every subscriber gets every event, payloads round-trip intact.
func Run(t *testing.T, newBus func(t *testing.T) bus.Bus) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	pub := newBus(t)
	subscribe := func(name string) <-chan bus.Event {
		out := make(chan bus.Event, 8)
		ready := make(chan struct{})
		go newBus(t).Subscribe(ctx, func(ev bus.Event) { out <- ev }, func() { close(ready) })
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: onConnected not called", name)
		}
		return out
	}
	a, b := subscribe("a"), subscribe("b")
	want := bus.Event{Type: bus.TypePush, NodeId: "n1", Payload: []byte(`{"conversation_id":"c","seq":7,"msg":{"content":"{\"text\":\"hi\"}"}}`)}
	if err := pub.Publish(ctx, want); err != nil {
		t.Fatal(err)
	}
	for name, ch := range map[string]<-chan bus.Event{"a": a, "b": b} {
		select {
		case got := <-ch:
			if got.Type != want.Type || got.NodeId != want.NodeId || string(got.Payload) != string(want.Payload) {
				t.Fatalf("%s: got %+v", name, got)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: no event", name)
		}
	}
	// A payload at the size cutoff must fit every backend (PG NOTIFY caps at 8000 bytes).
	big := bus.Event{Type: bus.TypePush, Payload: []byte(`{"pad":"` + strings.Repeat("x", bus.MaxPayloadBytes-10) + `"}`)}
	if err := pub.Publish(ctx, big); err != nil {
		t.Fatalf("payload at the cutoff rejected: %v", err)
	}
	select {
	case got := <-a:
		if len(got.Payload) != len(big.Payload) {
			t.Fatalf("big payload truncated: %d", len(got.Payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("big event lost")
	}
}

// Join makes a Bus whose Subscribe stops with the test: the goroutine must return before
// the bus is closed, otherwise a leaked subscription would keep reconnecting past the test.
func Join(t *testing.T, b bus.Bus) bus.Bus { return &joined{Bus: b, t: t} }

type joined struct {
	bus.Bus
	t *testing.T
}

func (b *joined) Subscribe(ctx context.Context, onEvent func(bus.Event), onConnected func()) error {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	b.t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			b.t.Error("subscription did not stop before bus close")
		}
	})
	defer close(done)
	return b.Bus.Subscribe(ctx, onEvent, onConnected)
}

// RunReconnect checks the recovery contract: after breakConn drops the subscription's
// server-side connection, the bus must call onConnected again (the gateway turns that
// second call into a Resync) and deliver events again. nodeId scopes the events, so
// unrelated traffic on a shared server is ignored; the caller builds the bus so that
// breakConn can find its connection.
func RunReconnect(t *testing.T, b bus.Bus, nodeId string, breakConn func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	// Not defer: the subscription has to outlive this call, so a caller can still inspect
	// the reconnected connection afterwards. Join stops it when the test ends.
	t.Cleanup(cancel)
	connected := make(chan struct{}, 4)
	events := make(chan bus.Event, 4)
	go Join(t, b).Subscribe(ctx, func(ev bus.Event) {
		if ev.NodeId != nodeId {
			return
		}
		select {
		case events <- ev:
		case <-ctx.Done():
		}
	}, func() {
		select {
		case connected <- struct{}{}:
		default:
		}
	})
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("subscription not confirmed")
	}
	breakConn()
	select {
	case <-connected:
	case <-time.After(10 * time.Second):
		t.Fatal("no reconnect")
	}
	if err := b.Publish(ctx, bus.Event{Type: bus.TypeKick, NodeId: nodeId}); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-events:
		if ev.Type != bus.TypeKick {
			t.Fatalf("got %+v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no event after reconnect")
	}
}
