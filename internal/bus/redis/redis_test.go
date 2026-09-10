package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"uuid"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/bus"
	"github.com/mbeoliero/nexo/internal/bus/bustest"
)

func TestBus(t *testing.T) {
	addr := os.Getenv("NEXO_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("NEXO_TEST_REDIS_ADDR not set")
	}
	bustest.Run(t, func(t *testing.T) bus.Bus {
		b, err := New(t.Context(), addr, "", 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return bustest.Join(t, b)
	})
}

func TestNewContextTimeoutEnabled(t *testing.T) {
	addr := os.Getenv("NEXO_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("NEXO_TEST_REDIS_ADDR not set")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	b, err := New(ctx, addr, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if !b.cli.Options().ContextTimeoutEnabled {
		t.Fatal("bus constructor must enable context socket deadlines")
	}
}

func TestPublishCountsNoReceivers(t *testing.T) {
	addr := os.Getenv("NEXO_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("NEXO_TEST_REDIS_ADDR not set")
	}
	if os.Getenv("NEXO_TEST_DISPOSABLE") != "1" {
		t.Skip("zero-subscriber assertion requires NEXO_TEST_DISPOSABLE=1")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	b, err := New(ctx, addr, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	ev := bus.Event{Type: bus.TypeKick, NodeId: uuid.NewV7().String()}
	for range 2 {
		if err := b.Publish(ctx, ev); !errors.Is(err, errcode.ErrBusFailed) {
			t.Fatalf("zero subscribers: got %v, want ErrBusFailed", err)
		}
	}
	if got := b.NoReceiversCount(); got != 2 {
		t.Fatalf("no receivers=%d, want 2", got)
	}
	if n, ok := b.DegradedPublishes(); n != 2 || !ok {
		t.Fatalf("DegradedPublishes=%d/%v, want 2/true", n, ok)
	}
	other, err := New(ctx, addr, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	if got := other.NoReceiversCount(); got != 0 {
		t.Fatalf("another Bus inherited %d zero-receiver publications", got)
	}
	ps := other.cli.Subscribe(ctx, channel)
	defer ps.Close()
	if _, err := ps.Receive(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := b.Publish(ctx, ev); err != nil {
		t.Fatalf("confirmed subscriber: %v", err)
	}
	if _, err := ps.ReceiveMessage(ctx); err != nil {
		t.Fatalf("receive published event: %v", err)
	}
	if got := b.NoReceiversCount(); got != 2 {
		t.Fatalf("successful publication increased no-receivers count to %d", got)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(ctx, ev); !errors.Is(err, goredis.ErrClosed) {
		t.Fatalf("closed client: got %v, want ErrClosed", err)
	}
	if got := b.NoReceiversCount(); got != 2 {
		t.Fatalf("connection failure increased no-receivers count to %d", got)
	}
}

func TestSubscribeIdleCancel(t *testing.T) {
	addr := os.Getenv("NEXO_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("NEXO_TEST_REDIS_ADDR not set")
	}
	b, err := New(t.Context(), addr, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if _, ok := ctx.Deadline(); ok {
		t.Fatal("subscription context must have no deadline")
	}
	connected := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- b.Subscribe(ctx, func(bus.Event) {}, func() {
			select {
			case connected <- struct{}{}:
			default:
			}
		})
	}()
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("subscription not confirmed")
	}
	// Leave Receive blocked on an idle connection before canceling.
	select {
	case err := <-done:
		t.Fatalf("subscription returned before cancellation: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("subscription cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("idle subscription did not return after cancellation")
	}
	if err := b.cli.Ping(t.Context()).Err(); err != nil {
		t.Fatalf("subscription cancellation closed the client: %v", err)
	}
}

func TestReconnectAfterClientKilled(t *testing.T) {
	addr := os.Getenv("NEXO_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("NEXO_TEST_REDIS_ADDR not set")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	name := "nexo-test-" + uuid.NewV7().String()
	b := &Bus{cli: goredis.NewClient(&goredis.Options{Addr: addr, ClientName: name})}
	t.Cleanup(func() { _ = b.Close() })
	clientId := func() int64 {
		t.Helper()
		list, err := b.cli.Do(ctx, "CLIENT", "LIST", "TYPE", "pubsub").Text()
		if err != nil {
			t.Fatal(err)
		}
		id, err := namedPubsubId(list, name)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	var killedId int64
	bustest.RunReconnect(t, b, name, func() {
		killedId = clientId()
		killed, err := b.cli.Do(ctx, "CLIENT", "KILL", "ID", killedId).Int64()
		if err != nil {
			t.Fatal(err)
		}
		if killed != 1 {
			t.Fatalf("killed %d clients, want exactly one", killed)
		}
	})
	if got := clientId(); got == killedId {
		t.Fatal("reconnect kept the terminated client ID")
	}
}

func namedPubsubId(list, name string) (int64, error) {
	var found int64
	for line := range strings.SplitSeq(list, "\n") {
		var clientName, clientId string
		for field := range strings.FieldsSeq(line) {
			key, value, ok := strings.Cut(field, "=")
			if !ok {
				continue
			}
			switch key {
			case "name":
				clientName = value
			case "id":
				clientId = value
			}
		}
		if clientName != name {
			continue
		}
		id, err := strconv.ParseInt(clientId, 10, 64)
		if err != nil || id <= 0 {
			return 0, fmt.Errorf("invalid pubsub client ID for %q: %q", name, clientId)
		}
		if found != 0 {
			return 0, fmt.Errorf("multiple pubsub clients named %q", name)
		}
		found = id
	}
	if found == 0 {
		return 0, fmt.Errorf("no pubsub client named %q", name)
	}
	return found, nil
}
