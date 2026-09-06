package postgres

import (
	"context"
	"encoding/json/v2"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mbeoliero/kit/log"

	"github.com/mbeoliero/nexo/internal/bus"
)

const (
	channel  = "nexo_events"
	maxConns = 4
)

// Bus uses pg_notify / LISTEN. Notifications are not persisted: anything sent while
// the listener is reconnecting is lost, and the gateway resyncs clients afterwards.
// Requires a direct PG connection or PgBouncer in session mode.
type Bus struct {
	dsn  string
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Bus, error) {
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("bus/postgres: %w", err)
	}
	pc.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("bus/postgres: %w", err)
	}
	return &Bus{dsn: dsn, pool: pool}, nil
}

func (b *Bus) Close() { b.pool.Close() }

func (b *Bus) Publish(ctx context.Context, ev bus.Event) error {
	raw, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("bus/postgres: %w", err)
	}
	if _, err := b.pool.Exec(ctx, "SELECT pg_notify($1, $2)", channel, string(raw)); err != nil {
		return fmt.Errorf("bus/postgres: notify: %w", err)
	}
	return nil
}

func (b *Bus) Subscribe(ctx context.Context, onEvent func(bus.Event), onConnected func()) error {
	backoff := bus.MinBackoff
	for {
		// A session that actually connected earns a fresh ladder; otherwise one long outage
		// leaves every later blip waiting MaxBackoff (bus/redis resets on each received message).
		connected := false
		err := b.listen(ctx, onEvent, func() { connected = true; onConnected() })
		if ctx.Err() != nil {
			return nil
		}
		if connected {
			backoff = bus.MinBackoff
		}
		log.CtxWarn(ctx, "bus/postgres: listener lost (%v); reconnecting in %s", err, backoff)
		if backoff, err = bus.Sleep(ctx, backoff); err != nil {
			return nil
		}
	}
}

// listen holds one dedicated connection (no pool, no transaction) until it fails.
func (b *Bus) listen(ctx context.Context, onEvent func(bus.Event), onConnected func()) error {
	conn, err := pgx.Connect(ctx, b.dsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.WithoutCancel(ctx))
	if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
		return err
	}
	onConnected()
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		var ev bus.Event
		if err := json.Unmarshal([]byte(n.Payload), &ev); err != nil {
			log.CtxError(ctx, "bus/postgres: bad payload: %v", err)
			continue
		}
		onEvent(ev)
	}
}
