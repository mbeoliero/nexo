package app

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mbeoliero/nexo/internal/api"
	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/internal/gateway"
	"github.com/mbeoliero/nexo/internal/offlinepush"
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
	"github.com/mbeoliero/nexo/msgbody"
)

type blockedPusher struct {
	entered chan struct{}
	release chan struct{}
	done    chan struct{}
}

func (p *blockedPusher) Push(context.Context, []string, offlinepush.Notification) error {
	close(p.entered)
	<-p.release
	close(p.done)
	return nil
}

func shutdownApp(t *testing.T) (*App, *blockedPusher) {
	t.Helper()
	mem := storetest.NewMem()
	if err := mem.UpsertUser(t.Context(), &store.User{Id: "u___2"}); err != nil {
		t.Fatal(err)
	}
	p := &blockedPusher{entered: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	svc := message.New(message.Adapt(mem), message.NoopPublisher{}, 1024)
	svc.SetOfflinePush(nil, p)
	return &App{deps: api.Deps{Message: svc}, gw: gateway.New(&config.Config{}, gateway.Deps{})}, p
}

func sendOffline(t *testing.T, a *App, p *blockedPusher) {
	t.Helper()
	_, err := a.deps.Message.Send(t.Context(), message.SendInput{
		SenderId: "u___1", RecvId: "u___2", ClientMsgId: "c1",
		SessionType: store.ConversationSingle, ContentType: msgbody.Text, Content: `{}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	<-p.entered
}

func TestShutdownOfflineDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, p := shutdownApp(t)
		defer close(p.release)
		sendOffline(t, a, p)
		var closed bool
		a.closers = []func(){func() { closed = true }}
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := a.Shutdown(ctx)
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 100*time.Millisecond {
			t.Errorf("shutdown elapsed=%v err=%v, want 100ms and deadline exceeded", time.Since(start), err)
		}
		if !closed {
			t.Error("dependencies not released after bounded wait")
		}
	})
}

func TestShutdownOfflineCompletesBeforeClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, p := shutdownApp(t)
		sendOffline(t, a, p)
		var closed bool
		a.closers = []func(){func() {
			closed = true
			select {
			case <-p.done:
			default:
				t.Error("dependencies closed before offline push completed")
			}
		}}
		go func() { time.Sleep(50 * time.Millisecond); close(p.release) }()
		defer func() { <-p.done }()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := a.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
		if !closed {
			t.Fatal("dependencies not released")
		}
	})
}

func TestDrainCloseOfflineUsesRemainingBudget(t *testing.T) {
	for _, httpTime := range []time.Duration{50 * time.Millisecond, 150 * time.Millisecond} {
		t.Run(httpTime.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				a, p := shutdownApp(t)
				defer close(p.release)
				var closed bool
				a.closers = []func(){func() { closed = true }}
				ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
				defer cancel()
				if err := a.Drain(ctx); err != nil {
					t.Fatal(err)
				}
				if closed {
					t.Fatal("Drain closed dependencies while HTTP still running")
				}
				// An in-flight HTTP handler can enqueue a push after Drain returns.
				sendOffline(t, a, p)
				time.Sleep(httpTime)
				start := time.Now()
				a.Close()
				want := max(100*time.Millisecond-httpTime, 0)
				if elapsed := time.Since(start); elapsed != want || !closed {
					t.Errorf(
						"Close elapsed=%v want=%v closed=%v",
						elapsed,
						want,
						closed,
					)
				}
			})
		})
	}
}

func TestShutdownPartialApp(t *testing.T) {
	var closed bool
	a := &App{closers: []func(){func() { closed = true }}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := a.Shutdown(ctx); !errors.Is(err, context.Canceled) || !closed {
		t.Fatalf("partial shutdown: err=%v closed=%v", err, closed)
	}
	a.Close()
}

func TestCloseOfflineFallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, p := shutdownApp(t)
		defer close(p.release)
		sendOffline(t, a, p)
		start := time.Now()
		a.Close()
		if elapsed := time.Since(start); elapsed != 5*time.Second {
			t.Errorf("Close without Drain took %v, want fallback 5s", elapsed)
		}
	})
}
