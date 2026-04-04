package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mbeoliero/kit/log"
)

type stubInstanceStateReader struct {
	states map[string]*InstanceState
	err    error
}

func (s *stubInstanceStateReader) ReadState(_ context.Context, instanceID string) (*InstanceState, error) {
	if s == nil {
		return nil, nil
	}
	if s.err != nil {
		return nil, s.err
	}
	if s.states == nil {
		return nil, nil
	}
	state, ok := s.states[instanceID]
	if !ok {
		return nil, nil
	}
	return state, nil
}

type countingInstanceStateReader struct {
	states map[string]*InstanceState
	calls  map[string]int
}

func (s *countingInstanceStateReader) ReadState(_ context.Context, instanceID string) (*InstanceState, error) {
	if s.calls == nil {
		s.calls = make(map[string]int)
	}
	s.calls[instanceID]++
	return s.states[instanceID], nil
}

func captureRouteStoreLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	log.SetOutput(buf)
	t.Cleanup(func() {
		log.SetOutput(io.Discard)
	})
	return buf
}

func TestRegisterConnWritesConnKeyUserIndexAndInstanceIndex(t *testing.T) {
	_, rdb := newTestRedis(t)
	store := NewRouteStore(rdb, time.Minute, nil)
	ctx := context.Background()
	conn := RouteConn{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}

	if err := store.RegisterConn(ctx, conn); err != nil {
		t.Fatalf("register conn: %v", err)
	}

	if got := rdb.Get(ctx, routeConnKey(conn.InstanceId, conn.ConnId)).Val(); got == "" {
		t.Fatal("route key not written")
	}
	if members := rdb.SMembers(ctx, routeUserKey(conn.UserId)).Val(); len(members) != 1 || members[0] != routeConnKey(conn.InstanceId, conn.ConnId) {
		t.Fatalf("user index mismatch: %+v", members)
	}
	if members := rdb.SMembers(ctx, routeInstanceKey(conn.InstanceId)).Val(); len(members) != 1 || members[0] != routeConnKey(conn.InstanceId, conn.ConnId) {
		t.Fatalf("instance index mismatch: %+v", members)
	}
}

func TestGetUsersConnRefsUsesInjectedInstanceStateReader(t *testing.T) {
	_, rdb := newTestRedis(t)
	store := NewRouteStore(rdb, time.Minute, &stubInstanceStateReader{
		states: map[string]*InstanceState{
			"i1": {InstanceId: "i1", Ready: true, Routeable: true},
			"i2": {InstanceId: "i2", Ready: false, Routeable: false, Draining: true},
		},
	})
	ctx := context.Background()

	routeableConn := RouteConn{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}
	drainingConn := RouteConn{UserId: "u1", ConnId: "c2", InstanceId: "i2", PlatformId: 1}

	if err := store.RegisterConn(ctx, routeableConn); err != nil {
		t.Fatalf("register routeable conn: %v", err)
	}
	if err := store.RegisterConn(ctx, drainingConn); err != nil {
		t.Fatalf("register draining conn: %v", err)
	}

	got, err := store.GetUsersConnRefs(ctx, []string{"u1"})
	if err != nil {
		t.Fatalf("get users conn refs: %v", err)
	}

	if len(got["u1"]) != 1 || got["u1"][0].ConnId != "c1" {
		t.Fatalf("push refs mismatch: %+v", got["u1"])
	}
}

func TestShouldIncludeStateForPresenceUsesAliveInsteadOfRouteable(t *testing.T) {
	aliveTrue := true
	aliveFalse := false

	if shouldIncludeState(&InstanceState{Alive: &aliveFalse, Routeable: true}, false) {
		t.Fatal("expected alive=false state to be excluded from presence")
	}
	if !shouldIncludeState(&InstanceState{Alive: &aliveTrue, Routeable: false}, false) {
		t.Fatal("expected alive=true state to remain visible in presence even when routeable=false")
	}
	if shouldIncludeState(&InstanceState{Alive: &aliveTrue, Routeable: false}, true) {
		t.Fatal("expected routeable=false state to be excluded from push routing")
	}
}

func TestGetUsersPresenceConnRefsUsesInjectedAliveStateReader(t *testing.T) {
	_, rdb := newTestRedis(t)
	store := NewRouteStore(rdb, time.Minute, &stubInstanceStateReader{
		states: map[string]*InstanceState{
			"i1": {InstanceId: "i1", Ready: true, Routeable: true},
			"i2": {InstanceId: "i2", Ready: false, Routeable: false, Draining: true},
		},
	})
	ctx := context.Background()

	routeableConn := RouteConn{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}
	drainingConn := RouteConn{UserId: "u1", ConnId: "c2", InstanceId: "i2", PlatformId: 1}

	_ = store.RegisterConn(ctx, routeableConn)
	_ = store.RegisterConn(ctx, drainingConn)

	got, err := store.GetUsersPresenceConnRefs(ctx, []string{"u1"})
	if err != nil {
		t.Fatalf("get users presence refs: %v", err)
	}

	if len(got["u1"]) != 2 {
		t.Fatalf("presence refs mismatch: %+v", got["u1"])
	}
}

func TestGetUsersConnRefsSkipsConnWhenStateReaderReturnsNil(t *testing.T) {
	_, rdb := newTestRedis(t)
	store := NewRouteStore(rdb, time.Minute, &stubInstanceStateReader{
		states: map[string]*InstanceState{
			"i1": {InstanceId: "i1", Ready: true, Routeable: true},
			"i2": nil,
		},
	})
	ctx := context.Background()

	routeableConn := RouteConn{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}
	missingConn := RouteConn{UserId: "u1", ConnId: "c2", InstanceId: "i2", PlatformId: 1}

	_ = store.RegisterConn(ctx, routeableConn)
	_ = store.RegisterConn(ctx, missingConn)

	got, err := store.GetUsersConnRefs(ctx, []string{"u1"})
	if err != nil {
		t.Fatalf("get users conn refs: %v", err)
	}

	if len(got["u1"]) != 1 || got["u1"][0].ConnId != "c1" {
		t.Fatalf("push refs mismatch: %+v", got["u1"])
	}
}

func TestGetUsersConnRefsCachesInstanceStateReadPerCall(t *testing.T) {
	_, rdb := newTestRedis(t)
	reader := &countingInstanceStateReader{
		states: map[string]*InstanceState{
			"i1": {InstanceId: "i1", Ready: true, Routeable: true},
		},
	}
	store := NewRouteStore(rdb, time.Minute, reader)
	ctx := context.Background()

	for _, connID := range []string{"c1", "c2"} {
		if err := store.RegisterConn(ctx, RouteConn{UserId: "u1", ConnId: connID, InstanceId: "i1", PlatformId: 5}); err != nil {
			t.Fatalf("register conn %s: %v", connID, err)
		}
	}

	got, err := store.GetUsersConnRefs(ctx, []string{"u1"})
	if err != nil {
		t.Fatalf("get users conn refs: %v", err)
	}

	if len(got["u1"]) != 2 {
		t.Fatalf("push refs mismatch: %+v", got["u1"])
	}
	if reader.calls["i1"] != 1 {
		t.Fatalf("instance state read count mismatch: got %d want 1", reader.calls["i1"])
	}
}

func TestGetUsersConnRefsReturnsErrorWhenStateReaderNotConfiguredAndRoutePresent(t *testing.T) {
	_, rdb := newTestRedis(t)
	store := NewRouteStore(rdb, time.Minute, nil)
	ctx := context.Background()

	if err := store.RegisterConn(ctx, RouteConn{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}); err != nil {
		t.Fatalf("register conn: %v", err)
	}

	_, err := store.GetUsersConnRefs(ctx, []string{"u1"})
	if err == nil {
		t.Fatal("expected error when instance state reader is not configured")
	}
	if !errors.Is(err, ErrInstanceStateReaderNotConfigured) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUnregisterConnRemovesConnKeyAndIndexes(t *testing.T) {
	_, rdb := newTestRedis(t)
	store := NewRouteStore(rdb, time.Minute, nil)
	ctx := context.Background()
	conn := RouteConn{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}

	_ = store.RegisterConn(ctx, conn)
	if err := store.UnregisterConn(ctx, conn); err != nil {
		t.Fatalf("unregister conn: %v", err)
	}

	if rdb.Exists(ctx, routeConnKey(conn.InstanceId, conn.ConnId)).Val() != 0 {
		t.Fatal("route key still exists")
	}
	if members := rdb.SMembers(ctx, routeUserKey(conn.UserId)).Val(); len(members) != 0 {
		t.Fatalf("user index still contains route: %+v", members)
	}
	if members := rdb.SMembers(ctx, routeInstanceKey(conn.InstanceId)).Val(); len(members) != 0 {
		t.Fatalf("instance index still contains route: %+v", members)
	}
}

func TestReconcileInstanceRoutesRepairsMissingAndRemovesStale(t *testing.T) {
	_, rdb := newTestRedis(t)
	store := NewRouteStore(rdb, time.Minute, nil)
	ctx := context.Background()

	stale := RouteConn{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}
	want := RouteConn{UserId: "u2", ConnId: "c2", InstanceId: "i1", PlatformId: 1}

	_ = store.RegisterConn(ctx, stale)
	if err := store.ReconcileInstanceRoutes(ctx, "i1", []RouteConn{want}); err != nil {
		t.Fatalf("reconcile instance routes: %v", err)
	}

	if rdb.Exists(ctx, routeConnKey(stale.InstanceId, stale.ConnId)).Val() != 0 {
		t.Fatal("stale route still exists")
	}
	if rdb.Exists(ctx, routeConnKey(want.InstanceId, want.ConnId)).Val() != 1 {
		t.Fatal("missing wanted route")
	}
}

func TestProbeRouteReadUsesReadPathForOwnedRoute(t *testing.T) {
	logBuf := captureRouteStoreLogs(t)
	_, rdb := newTestRedis(t)
	store := NewRouteStore(rdb, time.Minute, &stubInstanceStateReader{
		err: errors.New("boom"),
	})
	ctx := context.Background()

	if err := store.RegisterConn(ctx, RouteConn{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}); err != nil {
		t.Fatalf("register conn: %v", err)
	}

	err := store.ProbeRouteRead(ctx, "i1")
	if !errors.Is(err, store.states.(*stubInstanceStateReader).err) {
		t.Fatalf("expected probe to return route-read error, got %v", err)
	}
	if !strings.Contains(logBuf.String(), "route read probe using user route") {
		t.Fatalf("expected user-route probe log, got %q", logBuf.String())
	}
}

func TestProbeRouteReadFallsBackToStateReaderWhenInstanceHasNoRoutes(t *testing.T) {
	_, rdb := newTestRedis(t)
	store := NewRouteStore(rdb, time.Minute, &stubInstanceStateReader{
		err: errors.New("boom"),
	})

	err := store.ProbeRouteRead(context.Background(), "i1")
	if !errors.Is(err, store.states.(*stubInstanceStateReader).err) {
		t.Fatalf("expected fallback probe to return state-read error, got %v", err)
	}
}
