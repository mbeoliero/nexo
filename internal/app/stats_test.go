package app

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/api"
	"github.com/mbeoliero/nexo/internal/bus"
	buslocal "github.com/mbeoliero/nexo/internal/bus/local"
	busredis "github.com/mbeoliero/nexo/internal/bus/redis"
	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/internal/gateway"
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

func TestStatsAggregatesInstanceCounters(t *testing.T) {
	mem := storetest.NewMem()
	for _, id := range []string{"u___1", "u___2"} {
		if err := mem.UpsertUser(t.Context(), &store.User{Id: id}); err != nil {
			t.Fatal(err)
		}
	}
	svc := message.New(message.Adapt(mem), message.NoopPublisher{}, message.Config{MaxContentBytes: 1024})
	in := message.SendInput{SenderId: "u___1", RecvId: "u___2", ClientMsgId: "stats", SessionType: 1, ContentType: 1, Content: `{}`}
	for range 2 {
		if _, err := svc.Send(t.Context(), in); err != nil {
			t.Fatal(err)
		}
	}
	b := buslocal.New()
	ctx, cancel := context.WithCancel(t.Context())
	ready, entered, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	enter := sync.OnceFunc(func() { close(entered) })
	go func() {
		defer close(done)
		_ = b.Subscribe(ctx, func(bus.Event) { enter(); <-ctx.Done() }, func() { close(ready) })
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("blocked local subscriber did not stop")
		}
	})
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("local bus did not subscribe")
	}
	ev := bus.Event{Type: bus.TypeKick}
	if err := b.Publish(ctx, ev); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("local subscriber did not start its callback")
	}
	var publishErr error
	for range 4096 {
		if publishErr = b.Publish(ctx, ev); publishErr != nil {
			break
		}
	}
	if !errors.Is(publishErr, errcode.ErrBusFailed) {
		t.Fatalf("full local bus returned %v", publishErr)
	}
	a := &App{bus: b, deps: api.Deps{Message: svc}, gw: gateway.New(&config.Config{}, gateway.Deps{})}
	t.Cleanup(func() { _ = a.Shutdown(t.Context()) })
	for range 2 {
		stats := a.Stats()
		if stats.MessageRepublishAttempts != 1 || stats.BusDegradedPublishes != 1 || !stats.BusDegradedPublishesAvailable {
			t.Fatalf("instance counters not aggregated or reset on read: %+v", stats)
		}
	}
	other := &App{bus: buslocal.New(), gw: gateway.New(&config.Config{}, gateway.Deps{})}
	t.Cleanup(func() { _ = other.Shutdown(t.Context()) })
	// A fresh local bus has a signal and nothing to report, so availability is true and every
	// other counter is zero; anything else means a counter leaked between App instances.
	if stats := other.Stats(); stats != (gateway.Stats{BusDegradedPublishesAvailable: true}) {
		t.Fatalf("counters leaked between app instances: %+v", stats)
	}
}

// A driver with no receiver signal (PG NOTIFY, deploy/config.pg-only.yaml) must surface as
// unavailable, not as a zero that reads like a healthy bus (design §6.1).
func TestStatsReportsUnavailableForDriverWithoutReceiverSignal(t *testing.T) {
	a := &App{bus: noSignalBus{}, gw: gateway.New(&config.Config{}, gateway.Deps{})}
	t.Cleanup(func() { _ = a.Shutdown(t.Context()) })
	stats := a.Stats()
	if stats.BusDegradedPublishesAvailable || stats.BusDegradedPublishes != 0 {
		t.Fatalf("driver without a receiver signal reported as observable: %+v", stats)
	}
}

type noSignalBus struct{ bus.Bus }

func (noSignalBus) DegradedPublishes() (int64, bool) { return 0, false }

func TestStatsAggregatesRedisDegradedPublishes(t *testing.T) {
	addr := os.Getenv("NEXO_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("NEXO_TEST_REDIS_ADDR not set")
	}
	if os.Getenv("NEXO_TEST_DISPOSABLE") != "1" {
		t.Skip("zero-subscriber assertion requires NEXO_TEST_DISPOSABLE=1")
	}
	b, err := busredis.New(t.Context(), addr, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Publish(t.Context(), bus.Event{Type: bus.TypePresenceChanged}); !errors.Is(err, errcode.ErrBusFailed) {
		t.Fatalf("publish without subscribers: %v", err)
	}
	a := &App{bus: b, gw: gateway.New(&config.Config{}, gateway.Deps{})}
	t.Cleanup(func() { _ = a.Shutdown(t.Context()) })
	stats := a.Stats()
	if stats.BusDegradedPublishes != 1 || !stats.BusDegradedPublishesAvailable {
		t.Fatalf("Redis counter not aggregated: %+v", stats)
	}
}
