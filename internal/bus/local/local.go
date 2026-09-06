package local

import (
	"context"
	"sync"

	"github.com/mbeoliero/nexo/internal/bus"
)

const queueSize = 1024

// Bus is the in-process implementation. Several subscribers may share one Bus,
// which lets tests run two gateways as two nodes.
type Bus struct {
	mu   sync.RWMutex
	subs map[chan bus.Event]struct{}
}

func New() *Bus { return &Bus{subs: map[chan bus.Event]struct{}{}} }

func (b *Bus) Publish(_ context.Context, ev bus.Event) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.subs {
		select {
		case ch <- ev:
		default: // at-most-once: a stalled subscriber loses the event
		}
	}
	return nil
}

func (b *Bus) Subscribe(ctx context.Context, onEvent func(bus.Event), onConnected func()) error {
	ch := make(chan bus.Event, queueSize)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.subs, ch)
		b.mu.Unlock()
	}()
	onConnected()
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-ch:
			onEvent(ev)
		}
	}
}
