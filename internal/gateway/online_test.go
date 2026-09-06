package gateway

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/bus"
	buslocal "github.com/mbeoliero/nexo/internal/bus/local"
	"github.com/mbeoliero/nexo/internal/onlinestore"
	"github.com/mbeoliero/nexo/internal/service/group"
)

type fakeOnline struct {
	mu      sync.Mutex
	added   []onlinestore.ConnRef
	removed []onlinestore.ConnRef
	renewed [][]onlinestore.ConnRef
	purged  []string
	// ctxErrs is ctx.Err() as each presence write saw it; see TestPresenceWritesGetALiveContext.
	ctxErrs []error
}

func (f *fakeOnline) Add(ctx context.Context, _ string, c onlinestore.ConnRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.added = append(f.added, c)
	f.ctxErrs = append(f.ctxErrs, ctx.Err())
	return nil
}

func (f *fakeOnline) Remove(ctx context.Context, _ string, c onlinestore.ConnRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, c)
	f.ctxErrs = append(f.ctxErrs, ctx.Err())
	return nil
}

func (f *fakeOnline) Renew(_ context.Context, _ string, conns []onlinestore.ConnRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewed = append(f.renewed, conns)
	return nil
}

func (f *fakeOnline) Online(context.Context, []string) (map[string][]int, error) { return nil, nil }

func (f *fakeOnline) PurgeNode(_ context.Context, node string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.purged = append(f.purged, node)
	return nil
}

func TestOnlineStoreLifecycle(t *testing.T) {
	cfg := testConfig()
	cfg.NodeId = "n1"
	cfg.OnlineStore.RenewInterval = 30 * time.Millisecond
	g := newGateway(t, cfg)
	fo := &fakeOnline{}
	g.deps.Online = fo
	g.deps.Bus = buslocal.New()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go g.Run(ctx)

	url := startServer(t, g)
	conn, _, err := dialWs(url + "?platform_id=1&token=" + token(1))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	fo.mu.Lock()
	added, renewed, purged := len(fo.added), len(fo.renewed), fo.purged
	fo.mu.Unlock()
	if added != 1 || renewed == 0 || len(purged) != 1 || purged[0] != "n1" {
		t.Fatalf("added=%d renewed=%d purged=%v", added, renewed, purged)
	}
	conn.Close()
	for range 50 {
		fo.mu.Lock()
		n := len(fo.removed)
		fo.mu.Unlock()
		if n == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("close did not remove the connection from the online store")
}

func TestGroupChangedInvalidatesMemberCache(t *testing.T) {
	g, groups := newPushGateway(t)
	g.deps.Message.SetMemberCacheTtl(time.Hour)
	info, err := groups.Create(t.Context(), "u___1", group.CreateInput{Name: "g", MemberIds: []string{"u___2"}})
	if err != nil {
		t.Fatal(err)
	}
	sender := serveOn(t, g, "u___1", 1)
	member := serveOn(t, g, "u___2", 1)
	joiner := serveOn(t, g, "u___3", 1)
	send := func(id string) {
		sender.in <- []byte(`{"req_id":1003,"data":{"client_msg_id":"` + id + `","session_type":2,"group_id":"` + info.Id + `","content_type":1,"content":"{}"}}`)
		if r := sender.next(t); r.Code != 0 {
			t.Fatalf("ack: %+v", r)
		}
	}
	send("g1") // roster cached: u1, u2
	member.next(t)
	joiner.quiet(t)

	if err := groups.Join(t.Context(), info.Id, "u___3"); err != nil {
		t.Fatal(err)
	}
	send("g2") // cache still says u1, u2
	member.next(t)
	joiner.quiet(t)

	g.onEvent(t.Context(), bus.Event{Type: bus.TypeGroupChanged, Payload: []byte(`{"group_id":"` + info.Id + `"}`)})
	send("g3")
	member.next(t)
	if r := joiner.next(t); r.ReqId != PushMsg {
		t.Fatalf("joiner after invalidation: %+v", r)
	}
}

// The WS upgrade callback runs after Handle has returned, so the request context is already
// cancelled: presence writes made from there must carry their own context or every one of them
// fails with context canceled, leaving the node invisible for offline push and online status.
func TestPresenceWritesGetALiveContext(t *testing.T) {
	cfg := testConfig()
	cfg.NodeId = "n1"
	cfg.OnlineStore.RenewInterval = time.Hour // only the connection-scoped writes are under test
	g := newGateway(t, cfg)
	fo := &fakeOnline{}
	g.deps.Online = fo
	g.deps.Bus = buslocal.New()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go g.Run(ctx)

	conn, _, err := dialWs(startServer(t, g) + "?platform_id=1&token=" + token(1))
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	for range 50 {
		fo.mu.Lock()
		done, errs := len(fo.added) == 1 && len(fo.removed) == 1, slices.Clone(fo.ctxErrs)
		fo.mu.Unlock()
		if !done {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if slices.ContainsFunc(errs, func(e error) bool { return e != nil }) {
			t.Fatalf("presence write ran on a dead context: %v", errs)
		}
		return
	}
	t.Fatal("open and close did not reach the online store")
}
