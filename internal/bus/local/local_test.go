package local

import (
	"context"
	"testing"
	"time"

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
