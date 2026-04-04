package cluster

import (
	"context"
	"testing"
	"time"
)

type stubRouteSnapshotter struct {
	routes []RouteConn
}

func (s *stubRouteSnapshotter) SnapshotRouteConns(_ string) []RouteConn {
	return append([]RouteConn(nil), s.routes...)
}

type stubRouteReconciler struct {
	err        error
	sleep      time.Duration
	called     chan struct{}
	instanceID string
	want       []RouteConn
}

func (s *stubRouteReconciler) ReconcileInstanceRoutes(_ context.Context, instanceID string, want []RouteConn) error {
	if s.sleep > 0 {
		time.Sleep(s.sleep)
	}
	s.instanceID = instanceID
	s.want = append([]RouteConn(nil), want...)
	if s.called != nil {
		select {
		case s.called <- struct{}{}:
		default:
		}
	}
	return s.err
}

func TestRunRouteReconcileLoopAcceptsRouteReconcilerInterface(t *testing.T) {
	reconciler := &stubRouteReconciler{called: make(chan struct{}, 1)}
	snapshotter := &stubRouteSnapshotter{
		routes: []RouteConn{{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunRouteReconcileLoop(ctx, 50*time.Millisecond, "i1", snapshotter, reconciler, nil)
	}()

	select {
	case <-reconciler.called:
	case <-time.After(time.Second):
		t.Fatal("reconcile loop did not start")
	}
	if reconciler.instanceID != "i1" {
		t.Fatalf("instance id mismatch: got %q", reconciler.instanceID)
	}
	if len(reconciler.want) != 1 || reconciler.want[0].ConnId != "c1" || reconciler.want[0].UserId != "u1" {
		t.Fatalf("snapshot routes mismatch: %+v", reconciler.want)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("run reconcile loop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reconcile loop did not stop")
	}
}

func TestReconcileRepairsMissingRouteEntry(t *testing.T) {
	_, rdb := newTestRedis(t)
	store := NewRouteStore(rdb, time.Minute, nil)
	snapshotter := &stubRouteSnapshotter{
		routes: []RouteConn{{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunRouteReconcileLoop(ctx, 50*time.Millisecond, "i1", snapshotter, store, nil)
	}()

	waitForCondition(t, time.Second, func() bool {
		return rdb.Exists(context.Background(), routeConnKey("i1", "c1")).Val() == 1
	})

	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("run reconcile loop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reconcile loop did not stop")
	}
}

func TestReconcileRemovesStaleInstanceOwnedConn(t *testing.T) {
	_, rdb := newTestRedis(t)
	store := NewRouteStore(rdb, time.Minute, nil)
	ctx := context.Background()
	stale := RouteConn{UserId: "u1", ConnId: "stale", InstanceId: "i1", PlatformId: 5}
	if err := store.RegisterConn(ctx, stale); err != nil {
		t.Fatalf("register stale route: %v", err)
	}

	snapshotter := &stubRouteSnapshotter{}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunRouteReconcileLoop(runCtx, 50*time.Millisecond, "i1", snapshotter, store, nil)
	}()

	waitForCondition(t, time.Second, func() bool {
		return rdb.Exists(context.Background(), routeConnKey("i1", "stale")).Val() == 0
	})

	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("run reconcile loop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reconcile loop did not stop")
	}
}

func TestRunRouteReconcileLoopUpdatesReconcileDurationStat(t *testing.T) {
	reconciler := &stubRouteReconciler{
		sleep:  5 * time.Millisecond,
		called: make(chan struct{}, 1),
	}
	snapshotter := &stubRouteSnapshotter{}
	stats := NewRuntimeStats()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunRouteReconcileLoop(ctx, 50*time.Millisecond, "i1", snapshotter, reconciler, stats)
	}()

	select {
	case <-reconciler.called:
	case <-time.After(time.Second):
		t.Fatal("reconcile loop did not start")
	}

	if got := stats.Snapshot().ReconcileDurationMs; got <= 0 {
		t.Fatalf("reconcile duration mismatch: got %d want > 0", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("run reconcile loop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reconcile loop did not stop")
	}
}
