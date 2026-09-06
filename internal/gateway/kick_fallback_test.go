package gateway

import (
	"context"
	"sync"
	"testing"

	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/bus"
)

type invalidateOnLog struct{ invalidate func() }

func (e invalidateOnLog) Error() string {
	e.invalidate()
	return "publish failed"
}

type failedKickBus struct {
	bus.Bus
	err error
}

func (b failedKickBus) Publish(context.Context, bus.Event) error { return b.err }

func TestLocalKickRechecksSourceAfterPublishFailure(t *testing.T) {
	for _, state := range []string{"closed", "draining"} {
		t.Run(state, func(t *testing.T) {
			g := newGateway(t, testConfig())
			defer g.cancel()
			old := g.newClient(auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "old"}, "old", "", newFakeConn())
			fresh := g.newClient(auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "fresh"}, "fresh", "", newFakeConn())
			for _, c := range []*Client{old, fresh} {
				if err := g.users.Register(c); err != nil {
					t.Fatal(err)
				}
				defer c.Close("test")
			}
			invalidated := false
			// Error formatting runs after publishKick's preflight guards, before its fallback.
			g.deps.Bus = failedKickBus{err: invalidateOnLog{invalidate: sync.OnceFunc(func() {
				invalidated = true
				if state == "closed" {
					old.close()
				} else {
					old.closeAfterFlush(nil)
				}
			})}}
			g.publishKick(old)
			if !invalidated || old.activeCtx.Err() == nil {
				t.Fatal("source invalidation was not injected before fallback")
			}
			if fresh.draining.Load() || fresh.activeCtx.Err() != nil {
				t.Fatal("cancelled source kicked its replacement")
			}
		})
	}
}
