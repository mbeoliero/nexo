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
	g.workMu.Lock()
	g.shutdownCtx = ctx
	g.workMu.Unlock()
	g.cancelRun()
	clients := g.users.Close()
	hardDone := make(chan struct{})
	stopHard := context.AfterFunc(ctx, func() {
		g.cancelOps()
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
		g.cancelOps()
		for _, c := range clients {
			c.hardClose()
			c.close()
		}
	}
	g.cancel()
	g.cancelOps()
	g.workMu.Lock()
	g.sealed = true
	g.workMu.Unlock()
	done := make(chan struct{})
	go func() {
		g.work.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
	log.CtxInfo(ctx, "ws shutdown: closed %d connections", len(clients))
	return errors.Join(ctx.Err(), g.purgeNode(ctx))
}

// purgeGrace bounds the presence purge once the caller's deadline has already blown. Skipping it
// there is the worst case, not the safe one: this node's online_conns rows then live to
// online_store.ttl and every other node reads these users as online and suppresses their pushes.
// A bounded overrun buys that back; an uncooperative store still cannot hold Shutdown open.
const purgeGrace = time.Second

func (g *Gateway) purgeNode(ctx context.Context) error {
	if g.deps.Online == nil {
		return nil
	}
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), purgeGrace)
		defer cancel()
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
