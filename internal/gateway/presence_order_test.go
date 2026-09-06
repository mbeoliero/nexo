package gateway

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/onlinestore"
)

type orderedOnline struct {
	fakeOnline
	entered chan struct{}
	release chan struct{}
	live    map[string]bool
}

func (f *orderedOnline) Add(_ context.Context, _ string, c onlinestore.ConnRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.live[c.ConnId] = true
	return nil
}
func (f *orderedOnline) Remove(_ context.Context, _ string, c onlinestore.ConnRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.live, c.ConnId)
	return nil
}
func (f *orderedOnline) Renew(ctx context.Context, _ string, refs []onlinestore.ConnRef) error {
	select {
	case f.entered <- struct{}{}:
	default:
	}
	select {
	case <-f.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range refs {
		f.live[c.ConnId] = true
	}
	return nil
}

func TestPresenceRenewCannotResurrectClosedClient(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConfig()
		cfg.OnlineStore.RenewInterval = time.Second
		f := &orderedOnline{entered: make(chan struct{}, 1), release: make(chan struct{}), live: map[string]bool{}}
		g := New(cfg, Deps{Online: f})
		c := g.newClient(auth.Identity{UserId: "u___1"}, "c1", "", newFakeConn())
		if err := g.users.Register(c); err != nil {
			t.Fatal(err)
		}
		g.onlineAdd(c)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go g.renewLoop(ctx)
		<-f.entered
		closed := make(chan struct{})
		go func() { c.Close("test"); close(closed) }()
		synctest.Wait()
		close(f.release)
		<-closed
		cancel()
		synctest.Wait()
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.live[c.Id] {
			t.Fatal("stale renew resurrected removed client")
		}
	})
}

func TestPresenceQueuedAddSkipsClosingClient(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := &fakeOnline{}
		g := New(testConfig(), Deps{Online: f})
		c := g.newClient(auth.Identity{UserId: "u___1"}, "c1", "", newFakeConn())
		if !g.lockPresence(t.Context()) {
			t.Fatal("presence lock unavailable")
		}
		added, closed := make(chan struct{}), make(chan struct{})
		go func() { g.onlineAdd(c); close(added) }()
		synctest.Wait()
		go func() { c.Close("test"); close(closed) }()
		synctest.Wait()
		g.unlockPresence()
		<-added
		<-closed
		if len(f.added) != 0 {
			t.Fatal("queued Add ignored close while waiting for presence lock")
		}
	})
}

func TestPresenceLateAddSkipsClosedClient(t *testing.T) {
	g := New(testConfig(), Deps{Online: &fakeOnline{}})
	c := g.newClient(auth.Identity{UserId: "u___1"}, "c1", "", newFakeConn())
	c.Close("test")
	g.onlineAdd(c)
	if f := g.deps.Online.(*fakeOnline); len(f.added) != 0 {
		t.Fatal("late Add registered a closed client")
	}
}
