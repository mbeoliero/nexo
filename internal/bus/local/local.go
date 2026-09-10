package local

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/bus"
)

const queueSize = 1024

// Bus is the in-process implementation. Several subscribers may share one Bus,
// which lets tests run two gateways as two nodes.
type Bus struct {
	mu      sync.RWMutex
	subs    map[chan bus.Event]struct{}
	dropped atomic.Int64
}

func New() *Bus { return &Bus{subs: map[chan bus.Event]struct{}{}} }

func (b *Bus) DroppedCount() int64 { return b.dropped.Load() }

func (b *Bus) DegradedPublishes() (int64, bool) { return b.dropped.Load(), true }

func (b *Bus) Publish(_ context.Context, ev bus.Event) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var dropped int64
	for ch := range b.subs {
		select {
		case ch <- ev:
		default: // at-most-once: a stalled subscriber loses the event
			dropped++
		}
	}
	if dropped > 0 {
		b.dropped.Add(dropped)
		return errcode.ErrBusFailed
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
