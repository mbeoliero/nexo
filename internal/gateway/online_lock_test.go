package gateway

import (
	"context"
	"encoding/json/v2"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/bus"
	"github.com/mbeoliero/nexo/internal/onlinestore"
)

// hookedOnline runs its hook inside the presence write, which is where the node holds the presence
// lock: a test can panic or cancel the connection at exactly that point.
type hookedOnline struct {
	mu sync.Mutex
	// reviveAll makes Renew report every ref as a registration it had to re-create, which is what a
	// driver reports after the store lost this node's rows.
	reviveAll bool
	onAdd     func()
	onRenew   func()
	adds      int
	renews    int
}

func (o *hookedOnline) Add(context.Context, string, onlinestore.ConnRef) error {
	o.mu.Lock()
	o.adds++
	hook := o.onAdd
	o.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}

func (o *hookedOnline) Renew(_ context.Context, _ string, refs []onlinestore.ConnRef) ([]onlinestore.ConnRef, error) {
	o.mu.Lock()
	o.renews++
	hook, revive := o.onRenew, o.reviveAll
	o.mu.Unlock()
	if hook != nil {
		hook()
	}
	if revive {
		return refs, nil
	}
	return nil, nil
}

func (o *hookedOnline) Remove(context.Context, string, onlinestore.ConnRef) error { return nil }

func (o *hookedOnline) Online(context.Context, []string) (map[string][]int, error) {
	return nil, nil
}

func (o *hookedOnline) PurgeNode(context.Context, string) error { return nil }

func (o *hookedOnline) addCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.adds
}

// recordingBus keeps each publication's operation context error, because a publish that reaches the
// driver with a cancelled context fails there rather than in the gateway.
type recordingBus struct {
	mu      sync.Mutex
	events  []bus.Event
	ctxErrs []error
}

func (b *recordingBus) Publish(ctx context.Context, ev bus.Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, ev)
	b.ctxErrs = append(b.ctxErrs, ctx.Err())
	return ctx.Err()
}

func (*recordingBus) DegradedPublishes() (int64, bool) { return 0, false }

func (*recordingBus) Subscribe(context.Context, func(bus.Event), func()) error { return nil }

func (b *recordingBus) taken() ([]bus.Event, []error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	events, ctxErrs := b.events, b.ctxErrs
	b.events, b.ctxErrs = nil, nil
	return events, ctxErrs
}

func (b *recordingBus) publishedUsers(t *testing.T) []string {
	t.Helper()
	events, _ := b.taken()
	got := []string{}
	for _, ev := range events {
		if ev.Type != bus.TypePresenceChanged {
			t.Fatalf("unexpected event type %q", ev.Type)
		}
		var p bus.PresenceChanged
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			t.Fatal(err)
		}
		got = append(got, p.UserIds...)
	}
	slices.Sort(got)
	return got
}

func registerPresenceClient(t *testing.T, g *Gateway, userId, connId string) *Client {
	t.Helper()
	c := g.newClient(auth.Identity{UserId: userId, PlatformId: 1}, connId, "", newFakeConn())
	if err := g.users.Register(c); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close("test") })
	return c
}

// The presence semaphore holds one token and everything under it does store I/O or walks client
// state other goroutines mutate. A panic that unwound past an unlock would leave the token gone for
// good: the node would keep serving sockets while never registering or removing presence again.
func TestPresenceLockSurvivesPanicInsideTheRegion(t *testing.T) {
	for name, arm := range map[string]func(*hookedOnline){
		"add":   func(o *hookedOnline) { o.onAdd = func() { panic("onlinestore add panicked") } },
		"renew": func(o *hookedOnline) { o.onRenew = func() { panic("onlinestore renew panicked") } },
	} {
		t.Run(name, func(t *testing.T) {
			online := &hookedOnline{}
			arm(online)
			g := New(testConfig(), Deps{Online: online})
			t.Cleanup(func() { shutdownBusGateway(t, g) })
			c := registerPresenceClient(t, g, "u___1", "conn-1")
			func() {
				defer func() {
					if recover() == nil {
						t.Error("the injected panic never reached the caller")
					}
				}()
				if name == "add" {
					g.onlineAdd(c)
					return
				}
				_, _ = g.renew(t.Context())
			}()
			// A wedged semaphore blocks lockPresence until its context ends, so bound the wait.
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			if !g.lockPresence(ctx) {
				t.Fatal("presence lock stayed held after the panic; this node can no longer register or remove presence")
			}
			g.unlockPresence()
		})
	}
}

// The registration Add wrote is durable, so its announcement must outlive the socket: a duplicate
// login kick or a drain landing right after the write would otherwise cancel the publish, and peers
// would keep the user offline until their next periodic read (design §7.4).
func TestOnlineAddPublishesAfterAKickCancelsTheConnection(t *testing.T) {
	b, online := &recordingBus{}, &hookedOnline{}
	g := New(testConfig(), Deps{Online: online, Bus: b})
	t.Cleanup(func() { shutdownBusGateway(t, g) })
	c := registerPresenceClient(t, g, "u___1", "conn-1")
	online.onAdd = c.cancelActive // the kick lands between the write and the publish
	g.onlineAdd(c)
	if c.activeCtx.Err() == nil {
		t.Fatal("the injected kick did not cancel the connection")
	}
	events, ctxErrs := b.taken()
	if len(events) != 1 || events[0].Type != bus.TypePresenceChanged {
		t.Fatalf("published %d events, want one presence_changed", len(events))
	}
	if ctxErrs[0] != nil || g.onlinePublishFails.Load() != 0 {
		t.Fatalf("publish ran on a dead context (%v) and counted %d failures", ctxErrs[0], g.onlinePublishFails.Load())
	}
	var p bus.PresenceChanged
	if err := json.Unmarshal(events[0].Payload, &p); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(p.UserIds, []string{"u___1"}) {
		t.Fatalf("announced %v, want the user whose registration landed", p.UserIds)
	}
}

// A renew that changed nothing must publish nothing. Announcing every live ref on every heartbeat
// tick instead marks every matching subscription dirty on this node and on every peer, which drags
// their refreshes from the snapshot period down to the minimum read interval for no state change at
// all; only the registrations the tick actually created are presence changes (design §7.4).
func TestRenewPublishesOnlyTheRegistrationsItRestored(t *testing.T) {
	b, online := &recordingBus{}, &hookedOnline{}
	g := New(testConfig(), Deps{Online: online, Bus: b})
	t.Cleanup(func() { shutdownBusGateway(t, g) })
	for i, id := range []string{"u___1", "u___2"} {
		g.onlineAdd(registerPresenceClient(t, g, id, "conn-"+strconv.Itoa(i)))
	}
	if got := b.publishedUsers(t); !slices.Equal(got, []string{"u___1", "u___2"}) {
		t.Fatalf("registrations announced %v", got)
	}
	if n, err := g.renew(t.Context()); n != 2 || err != nil {
		t.Fatalf("renew: conns=%d err=%v", n, err)
	}
	if online.addCount() != 2 {
		t.Fatalf("renew re-added %d registrations, want the two the connections already had", online.addCount()-2)
	}
	if got := b.publishedUsers(t); len(got) != 0 {
		t.Fatalf("a steady-state renew announced %v, want no event at all", got)
	}

	// A connection whose Add failed is re-added by the next renew: that write is a real change.
	c := registerPresenceClient(t, g, "u___3", "conn-2")
	if n, err := g.renew(t.Context()); n != 3 || err != nil {
		t.Fatalf("renew: conns=%d err=%v", n, err)
	}
	if !c.onlineAdded || online.addCount() != 3 {
		t.Fatalf("renew wrote %d registrations, want the one connection that had none", online.addCount()-2)
	}
	if got := b.publishedUsers(t); !slices.Equal(got, []string{"u___3"}) {
		t.Fatalf("renew announced %v, want only the connection it registered", got)
	}

	// And a driver that restored lost registrations reports them, so those users are announced too.
	online.mu.Lock()
	online.reviveAll = true
	online.mu.Unlock()
	if n, err := g.renew(t.Context()); n != 3 || err != nil {
		t.Fatalf("renew: conns=%d err=%v", n, err)
	}
	if got := b.publishedUsers(t); !slices.Equal(got, []string{"u___1", "u___2", "u___3"}) {
		t.Fatalf("renew announced %v, want every registration the driver had to restore", got)
	}
}
