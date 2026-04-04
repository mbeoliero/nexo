package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWriteStatePublishesReadyRouteableAndDraining(t *testing.T) {
	_, rdb := newTestRedis(t)
	manager := NewInstanceManager(rdb, time.Minute)
	ctx := context.Background()

	if err := manager.WriteState(ctx, InstanceState{
		InstanceId: "i1",
		Ready:      true,
		Routeable:  false,
		Draining:   true,
	}); err != nil {
		t.Fatalf("write state: %v", err)
	}

	got, err := manager.ReadState(ctx, "i1")
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if got == nil || !got.Ready || got.Routeable || !got.Draining {
		t.Fatalf("instance state mismatch: %+v", got)
	}
}

func TestReadyAndRouteableCanDiverge(t *testing.T) {
	_, rdb := newTestRedis(t)
	manager := NewInstanceManager(rdb, time.Minute)
	ctx := context.Background()

	if err := manager.WriteState(ctx, InstanceState{
		InstanceId: "i1",
		Ready:      false,
		Routeable:  true,
	}); err != nil {
		t.Fatalf("write state: %v", err)
	}

	got, err := manager.ReadState(ctx, "i1")
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if got == nil || got.Ready || !got.Routeable {
		t.Fatalf("instance state mismatch: %+v", got)
	}
}

func TestHeartbeatRefreshesAliveTTL(t *testing.T) {
	server, rdb := newTestRedis(t)
	manager := NewInstanceManager(rdb, 120*time.Millisecond)
	manager.now = func() time.Time { return time.UnixMilli(123) }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, "i1", func() InstanceState {
			return InstanceState{
				Ready:     true,
				Routeable: true,
			}
		})
	}()

	waitForCondition(t, time.Second, func() bool {
		return server.Exists(instanceAliveKey("i1"))
	})
	server.FastForward(100 * time.Millisecond)
	waitForCondition(t, time.Second, func() bool {
		return server.Exists(instanceAliveKey("i1"))
	})

	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("run instance manager: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("instance manager did not stop")
	}
}

func TestInstanceManagerCanBeUsedAsInstanceStateReaderAndPublisher(t *testing.T) {
	_, rdb := newTestRedis(t)

	var reader InstanceStateReader = NewInstanceManager(rdb, time.Minute)
	var publisher InstanceStatePublisher = NewInstanceManager(rdb, time.Minute)

	ctx := context.Background()
	if err := publisher.WriteState(ctx, InstanceState{
		InstanceId: "i1",
		Ready:      true,
		Routeable:  true,
	}); err != nil {
		t.Fatalf("write state via publisher: %v", err)
	}

	state, err := reader.ReadState(ctx, "i1")
	if err != nil {
		t.Fatalf("read state via reader: %v", err)
	}
	if state == nil || !state.Ready || !state.Routeable {
		t.Fatalf("instance state mismatch: %+v", state)
	}
}

func TestInstanceManagerSyncNowStillPublishesUpdatedSnapshot(t *testing.T) {
	_, rdb := newTestRedis(t)
	manager := NewInstanceManager(rdb, time.Minute)
	manager.SetHeartbeatInterval(time.Hour)

	var ready atomic.Bool
	var routeable atomic.Bool
	var draining atomic.Bool
	ready.Store(true)
	routeable.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var publisher InstanceStatePublisher = manager
	var reader InstanceStateReader = manager

	done := make(chan error, 1)
	go func() {
		done <- publisher.Run(ctx, "i1", func() InstanceState {
			return InstanceState{
				Ready:     ready.Load(),
				Routeable: routeable.Load(),
				Draining:  draining.Load(),
			}
		})
	}()

	waitForCondition(t, time.Second, func() bool {
		state, err := reader.ReadState(context.Background(), "i1")
		return err == nil && state != nil && state.Ready && state.Routeable
	})

	ready.Store(false)
	routeable.Store(false)
	draining.Store(true)

	if err := publisher.SyncNow(context.Background()); err != nil {
		t.Fatalf("sync now: %v", err)
	}

	state, err := reader.ReadState(context.Background(), "i1")
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if state == nil || state.Ready || state.Routeable || !state.Draining {
		t.Fatalf("synced state mismatch: %+v", state)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("run instance manager: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("instance manager did not stop")
	}
}

func TestInstanceManagerSyncNowReturnsErrorWhenRuntimeSnapshotUnavailable(t *testing.T) {
	_, rdb := newTestRedis(t)
	manager := NewInstanceManager(rdb, time.Minute)

	err := manager.SyncNow(context.Background())
	if err == nil {
		t.Fatal("expected sync now to fail when runtime snapshot is unavailable")
	}
	if !errors.Is(err, ErrInstanceStateSyncNotConfigured) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInstanceManagerSyncNowUsesConfiguredSnapshotBeforeRun(t *testing.T) {
	manager := NewInstanceManager(nil, time.Minute)
	manager.now = func() time.Time { return time.UnixMilli(123) }

	var wrote InstanceState
	manager.writeStateFn = func(_ context.Context, state InstanceState) error {
		wrote = state
		return nil
	}
	manager.ConfigureSnapshot("i1", func() InstanceState {
		return InstanceState{
			Ready:     false,
			Routeable: false,
			Draining:  true,
		}
	})

	if err := manager.SyncNow(context.Background()); err != nil {
		t.Fatalf("sync now before run: %v", err)
	}

	if wrote.InstanceId != "i1" || wrote.Ready || wrote.Routeable || !wrote.Draining || wrote.UpdatedAt != 123 {
		t.Fatalf("configured snapshot mismatch: %+v", wrote)
	}
}

func TestInstanceManagerSyncNowPreservesExplicitAliveFalseBeforeRun(t *testing.T) {
	manager := NewInstanceManager(nil, time.Minute)
	manager.now = func() time.Time { return time.UnixMilli(456) }

	var wrote InstanceState
	manager.writeStateFn = func(_ context.Context, state InstanceState) error {
		wrote = state
		return nil
	}
	alive := false
	manager.ConfigureSnapshot("i1", func() InstanceState {
		return InstanceState{
			Alive:     &alive,
			Ready:     false,
			Routeable: false,
		}
	})

	if err := manager.SyncNow(context.Background()); err != nil {
		t.Fatalf("sync now before run: %v", err)
	}

	if wrote.Alive == nil || *wrote.Alive {
		t.Fatalf("expected explicit alive=false to be written: %+v", wrote)
	}
	if wrote.UpdatedAt != 456 {
		t.Fatalf("updated_at mismatch: got %d want 456", wrote.UpdatedAt)
	}
}

func TestInstanceManagerSyncNowFallsBackToDirectWriteAfterRunStops(t *testing.T) {
	_, rdb := newTestRedis(t)
	manager := NewInstanceManager(rdb, time.Minute)
	manager.SetHeartbeatInterval(time.Hour)

	var ready atomic.Bool
	var routeable atomic.Bool
	var draining atomic.Bool
	ready.Store(true)
	routeable.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, "i1", func() InstanceState {
			return InstanceState{
				Ready:     ready.Load(),
				Routeable: routeable.Load(),
				Draining:  draining.Load(),
			}
		})
	}()

	waitForCondition(t, time.Second, func() bool {
		state, err := manager.ReadState(context.Background(), "i1")
		return err == nil && state != nil && state.Ready && state.Routeable
	})

	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("run instance manager: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("instance manager did not stop")
	}

	ready.Store(false)
	routeable.Store(false)
	draining.Store(true)

	if err := manager.SyncNow(context.Background()); err != nil {
		t.Fatalf("sync now after stop: %v", err)
	}

	state, err := manager.ReadState(context.Background(), "i1")
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if state == nil || state.Ready || state.Routeable || !state.Draining {
		t.Fatalf("synced state mismatch after stop: %+v", state)
	}
}
