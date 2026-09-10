package local

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/bus"
	"github.com/mbeoliero/nexo/internal/bus/bustest"
)

func TestBus(t *testing.T) {
	shared := New()
	bustest.Run(t, func(*testing.T) bus.Bus { return shared })
}

func TestSubscribeStopsWithContext(t *testing.T) {
	b := New()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.Subscribe(ctx, func(bus.Event) {}, func() {}) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("subscribe did not return")
	}
	if len(b.subs) != 0 {
		t.Fatal("subscriber leaked")
	}
}

func TestPublishReportsAllDropsAndStillDelivers(t *testing.T) {
	t.Parallel()
	b := New()
	want := bus.Event{Type: bus.TypeKick, NodeId: "n1"}
	// Two full subscriptions distinguish per-subscriber drops from failed Publish calls.
	for range 2 {
		ch := make(chan bus.Event, 1)
		ch <- want
		b.subs[ch] = struct{}{}
	}
	healthy := make(chan bus.Event, 1)
	b.subs[healthy] = struct{}{}
	for i := range 3 {
		done := make(chan error, 1)
		go func() { done <- b.Publish(t.Context(), want) }()
		select {
		case err := <-done:
			if !errors.Is(err, errcode.ErrBusFailed) {
				t.Fatalf("full subscriber: got %v, want ErrBusFailed", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Publish waited for a full subscriber")
		}
		select {
		case got := <-healthy:
			if got.Type != want.Type || got.NodeId != want.NodeId {
				t.Fatalf("healthy subscriber got %+v", got)
			}
		default:
			t.Fatal("full subscriber prevented delivery to the healthy subscriber")
		}
		if got := b.DroppedCount(); got != int64(2*(i+1)) {
			t.Fatalf("dropped=%d, want %d", got, 2*(i+1))
		}
		if n, ok := b.DegradedPublishes(); n != int64(2*(i+1)) || !ok {
			t.Fatalf("DegradedPublishes=%d/%v, want %d/true", n, ok, 2*(i+1))
		}
	}
	if got := New().DroppedCount(); got != 0 {
		t.Fatalf("another Bus inherited %d drops", got)
	}
	if n, ok := New().DegradedPublishes(); n != 0 || !ok {
		t.Fatalf("another Bus reported DegradedPublishes=%d/%v, want 0/true", n, ok)
	}
}

func TestPublishDroppedCountConcurrent(t *testing.T) {
	t.Parallel()
	b := New()
	ch := make(chan bus.Event, 1)
	ch <- bus.Event{}
	b.subs[ch] = struct{}{}
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			if err := b.Publish(t.Context(), bus.Event{}); !errors.Is(err, errcode.ErrBusFailed) {
				t.Errorf("got %v, want ErrBusFailed", err)
			}
			_ = b.DroppedCount()
		})
	}
	wg.Wait()
	if got := b.DroppedCount(); got != 100 {
		t.Fatalf("dropped=%d, want 100", got)
	}
}

func TestPublishPreservesCanceledContextBehavior(t *testing.T) {
	t.Parallel()
	b := New()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := b.Publish(ctx, bus.Event{}); err != nil {
		t.Fatalf("empty local Bus: %v", err)
	}
	ch := make(chan bus.Event, 1)
	b.subs[ch] = struct{}{}
	if err := b.Publish(ctx, bus.Event{}); err != nil {
		t.Fatalf("nonblocking local publication: %v", err)
	}
	select {
	case <-ch:
	default:
		t.Fatal("local publication stopped attempting delivery with a canceled context")
	}
	if got := b.DroppedCount(); got != 0 {
		t.Fatalf("successful publication counted %d drops", got)
	}
}
