package gateway

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/bus"
	buslocal "github.com/mbeoliero/nexo/internal/bus/local"
)

type gatedWrite struct {
	*fakeConn
	release chan struct{}
}

func (f *gatedWrite) WriteMessage(raw []byte) error {
	select {
	case <-f.release:
		return f.fakeConn.WriteMessage(raw)
	case <-f.done:
		return errors.New("closed")
	}
}

func TestKickStopsAdmissionWithoutInterruptingFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := newGateway(t, testConfig())
		f := &gatedWrite{fakeConn: newFakeConn(), release: make(chan struct{})}
		c := serve(t, g, "u___1", f)
		_ = c.Send([]byte(`{"req_id":2004,"data":{}}`))
		synctest.Wait()
		c.kick(KickNewLogin)
		f.in <- []byte(`{"req_id":1003,"data":{"client_msg_id":"after-kick","session_type":1,` +
			`"recv_id":"u___2","content_type":1,"content":"{}"}}`)
		synctest.Wait()
		seqs, err := g.deps.Message.MaxSeqs(
			t.Context(),
			c.UserId,
			"",
			10,
			100,
		)
		if err != nil || len(seqs.Items) != 0 {
			t.Errorf("request after kick reached service: %+v %v", seqs, err)
		}
		if f.isClosed() {
			t.Error("read loop interrupted the pending kick flush")
		}
		close(f.release)
		g.work.Wait()
		if r := f.next(t); r.ReqId != Resync {
			t.Errorf("queued frame was lost: %+v", r)
		}
		if r := f.next(t); r.ReqId != KickOnline {
			t.Errorf("kick frame was lost: %+v", r)
		}
		if !f.isClosed() {
			t.Error("drain did not close socket")
		}
	})
}

func TestKickBoundsEntireDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := newGateway(t, testConfig())
		f := newFakeConn()
		f.block = true
		c := serve(t, g, "u___1", f)
		c.kick(KickNewLogin)
		time.Sleep(writeWait - time.Nanosecond)
		if f.isClosed() {
			t.Error("closed before drain budget expired")
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if !f.isClosed() || g.users.Count() != 0 {
			t.Error("kick did not hard-close socket at overall drain deadline")
		}
		c.Close("test")
		g.work.Wait()
	})
}

func TestLateHandshakeCannotKickFreshConnection(t *testing.T) {
	for _, driver := range []string{"none", "local"} {
		for _, state := range []string{"closed", "draining"} {
			t.Run(driver+"/"+state, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					g := newGateway(t, testConfig())
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					if driver == "local" {
						g.deps.Bus = buslocal.New()
						go g.Subscribe(ctx)
						<-g.Ready()
					}
					old := g.newClient(
						auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "old"},
						"old",
						"",
						newFakeConn(),
					)
					fresh := g.newClient(
						auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "fresh"},
						"fresh",
						"",
						newFakeConn(),
					)
					for _, c := range []*Client{old, fresh} {
						if err := g.users.Register(c); err != nil {
							t.Fatal(err)
						}
						defer c.Close("test")
					}
					if state == "closed" {
						old.Close("test")
					} else {
						old.kick(KickNewLogin)
					}
					g.publishKick(old)
					synctest.Wait()
					if fresh.draining.Load() {
						t.Error("late old handshake kicked fresh connection")
					}
					g.cancel()
				})
			})
		}
	}
}

type cancelKickBus struct {
	bus.Bus
	entered chan struct{}
}

func (b *cancelKickBus) Publish(ctx context.Context, _ bus.Event) error {
	close(b.entered)
	<-ctx.Done()
	return ctx.Err()
}

func TestKickCancelsPendingPublishWithoutLocalFallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := &cancelKickBus{entered: make(chan struct{})}
		g := New(testConfig(), Deps{Bus: b})
		old := g.newClient(
			auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "old"},
			"old",
			"",
			newFakeConn(),
		)
		fresh := g.newClient(
			auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "fresh"},
			"fresh",
			"",
			newFakeConn(),
		)
		for _, c := range []*Client{old, fresh} {
			if err := g.users.Register(c); err != nil {
				t.Fatal(err)
			}
			defer c.Close("test")
		}
		done := make(chan struct{})
		go func() { g.publishKick(old); close(done) }()
		<-b.entered
		start := time.Now()
		old.kick(KickNewLogin)
		<-done
		if time.Since(start) != 0 || fresh.draining.Load() {
			t.Errorf("publish outlived kick or ran stale fallback: elapsed=%v fresh-draining=%v",
				time.Since(start), fresh.draining.Load())
		}
	})
}

func TestSlowConsumerCleanupDoesNotBlockBus(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConfig()
		cfg.Ws.SendQueue = 1
		f := &blockingOnline{blockRemove: true, waitCtx: true, entered: make(chan struct{}, 1)}
		g := New(cfg, Deps{Online: f})
		socket := newFakeConn()
		c := g.newClient(
			auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "old"},
			"c1",
			"",
			socket,
		)
		if err := g.users.Register(c); err != nil {
			t.Fatal(err)
		}
		_ = c.Send([]byte(`{}`))
		start := time.Now()
		g.onEvent(t.Context(), bus.Event{Type: bus.TypeKick,
			Payload: []byte(`{"user_id":"u___1","platform_id":1,"keep_token_id":"new"}`)})
		if time.Since(start) != 0 {
			t.Errorf("bus callback waited %v for remote cleanup", time.Since(start))
		}
		if !socket.isClosed() || g.users.Count() != 0 {
			t.Error("socket and local slot were not released immediately")
		}
		<-f.entered
		if err := g.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) != 0 {
			t.Error("shutdown did not cancel pending cleanup")
		}
	})
}

func TestPresenceCleanupTasksAreBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := New(testConfig(), Deps{Online: &blockingOnline{blockRemove: true, waitCtx: true}})
		for range cap(g.cleanup) + 2 {
			c := g.newClient(
				auth.Identity{UserId: "u___1"},
				"c1",
				"",
				newFakeConn(),
			)
			c.Close("test")
		}
		synctest.Wait()
		if len(g.cleanup) != cap(g.cleanup) {
			t.Fatalf("cleanup tasks: %d, want bounded capacity %d", len(g.cleanup), cap(g.cleanup))
		}
		g.cancelOps()
		g.work.Wait()
		if len(g.cleanup) != 0 {
			t.Fatal("cleanup permits leaked after cancellation")
		}
	})
}

func TestUnlimitedPushAccounting(t *testing.T) {
	for _, action := range []string{"write", "close", "full queue"} {
		t.Run(action, func(t *testing.T) {
			cfg := testConfig()
			cfg.Limits.WsSendBytesTotal = 0
			cfg.Ws.SendQueue = 1
			g := New(cfg, Deps{})
			c := g.newClient(
				auth.Identity{UserId: "u___1"},
				"c1",
				"",
				newFakeConn(),
			)
			defer c.Close("test")
			if err := c.Push([]byte(`{}`)); err != nil {
				t.Fatal(err)
			}
			if got := g.sendBytes.Load(); got != 2 {
				t.Errorf("unlimited Push skipped accounting: got %d, want 2", got)
			}
			switch action {
			case "write":
				c.write(<-c.send)
			case "close":
				c.Close("test")
			case "full queue":
				if err := c.Push([]byte(`{}`)); err == nil {
					t.Error("full queue did not reject push")
				}
			}
			if got := g.sendBytes.Load(); got != 0 {
				t.Errorf("released bytes = %d, want 0", got)
			}
		})
	}
}
