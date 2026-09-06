package redis

import (
	"context"
	"encoding/json/v2"
	"fmt"

	"github.com/mbeoliero/kit/log"
	"github.com/redis/go-redis/v9"

	"github.com/mbeoliero/nexo/internal/bus"
)

const channel = "nexo:events"

// Bus uses one PUBLISH / SUBSCRIBE channel. go-redis reconnects and resubscribes on
// its own; the subscribe confirmation it re-emits afterwards is our reconnect signal.
type Bus struct {
	cli *redis.Client
}

func New(ctx context.Context, addr, password string, db int) (*Bus, error) {
	cli := redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: db, ContextTimeoutEnabled: true})
	if err := cli.Ping(ctx).Err(); err != nil {
		cli.Close()
		return nil, fmt.Errorf("bus/redis: %w", err)
	}
	return &Bus{cli: cli}, nil
}

func (b *Bus) Close() error { return b.cli.Close() }

func (b *Bus) Publish(ctx context.Context, ev bus.Event) error {
	raw, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("bus/redis: %w", err)
	}
	if err := b.cli.Publish(ctx, channel, raw).Err(); err != nil {
		return fmt.Errorf("bus/redis: publish: %w", err)
	}
	return nil
}

func (b *Bus) Subscribe(ctx context.Context, onEvent func(bus.Event), onConnected func()) error {
	ps := b.cli.Subscribe(ctx, channel)
	stop := context.AfterFunc(ctx, func() { _ = ps.Close() })
	defer stop()
	defer ps.Close()
	backoff := bus.MinBackoff
	for {
		msg, err := ps.Receive(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			log.CtxWarn(ctx, "bus/redis: receive failed (%v); retrying in %s", err, backoff)
			if backoff, err = bus.Sleep(ctx, backoff); err != nil {
				return nil
			}
			continue
		}
		backoff = bus.MinBackoff
		switch m := msg.(type) {
		case *redis.Subscription:
			if m.Kind == "subscribe" {
				onConnected()
			}
		case *redis.Message:
			var ev bus.Event
			if err := json.Unmarshal([]byte(m.Payload), &ev); err != nil {
				log.CtxError(ctx, "bus/redis: bad payload: %v", err)
				continue
			}
			onEvent(ev)
		}
	}
}
