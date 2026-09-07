package gateway

import (
	"context"
	"errors"
	"time"

	"github.com/mbeoliero/kit/log"
)

func (g *Gateway) Shutdown(ctx context.Context) error {
	g.kickMu.Lock()
	g.closing.Store(true)
	g.kickMu.Unlock()
	g.work.shutdown(ctx)
	g.cancelRun()
	clients := g.users.Close()
	hardDone := make(chan struct{})
	stopHard := context.AfterFunc(ctx, func() {
		g.work.cancelOps()
		g.cancel()
		for _, c := range clients {
			c.hardClose()
			c.close()
		}
		close(hardDone)
	})
	defer func() {
		if !stopHard() {
			<-hardDone
		}
	}()
	for _, c := range clients {
		c.closeAfterFlush(nil)
	}
	for g.users.Count() > 0 && ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
	if ctx.Err() != nil {
		g.work.cancelOps()
		for _, c := range clients {
			c.hardClose()
			c.close()
		}
	}
	g.cancel()
	g.work.cancelOps()
	g.work.seal()
	done := make(chan struct{})
	go func() {
		g.work.wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
	log.CtxInfo(ctx, "ws shutdown: closed %d connections", len(clients))
	return errors.Join(ctx.Err(), g.purgeNode(ctx))
}

// purgeNode shares the caller's deadline (design §10): once it has passed, this node's leftover
// online_conns rows expire by online_store.ttl and the next start runs PurgeNode again.
func (g *Gateway) purgeNode(ctx context.Context) error {
	if g.deps.Online == nil || ctx.Err() != nil {
		return nil
	}
	purged := make(chan error, 1)
	go func() { purged <- g.deps.Online.PurgeNode(ctx, g.cfg.NodeId) }()
	select {
	case err := <-purged:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
