package gateway

import (
	"context"
	"sync"
	"time"

	"github.com/samber/lo"
)

// workGroup tracks every goroutine and dependency call the gateway owns so Shutdown can wait
// for them. Its state changes in one order: shutdown (deadline known) → cancel ops → seal
// (no new work). The mutex covers the flag and the shutdown context, never the WaitGroup itself.
type workGroup struct {
	mu          sync.Mutex
	wg          sync.WaitGroup
	sealed      bool
	shutdownCtx context.Context // set by Shutdown; its deadline bounds every later op
	opsCtx      context.Context // cancelled when in-flight dependency calls must stop
	cancelOps   context.CancelFunc
}

func newWorkGroup() *workGroup {
	w := &workGroup{}
	w.opsCtx, w.cancelOps = context.WithCancel(context.Background())
	return w
}

// begin registers one unit of work; false once the group is sealed.
func (w *workGroup) begin() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.sealed {
		return false
	}
	w.wg.Add(1)
	return true
}

func (w *workGroup) done() { w.wg.Done() }

// op bounds one dependency call by connOpTimeout and, once Shutdown started, by its deadline.
// The returned context is already cancelled when no new work may start.
func (w *workGroup) op(parent context.Context) (context.Context, context.CancelFunc) {
	w.mu.Lock()
	defer w.mu.Unlock()
	deadline := time.Now().Add(connOpTimeout)
	if w.shutdownCtx != nil {
		if d, ok := w.shutdownCtx.Deadline(); ok {
			deadline = minTime(deadline, d)
		}
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	if w.sealed || w.opsCtx.Err() != nil || (w.shutdownCtx != nil && w.shutdownCtx.Err() != nil) {
		cancel()
		return ctx, cancel
	}
	w.wg.Add(1)
	stop := context.AfterFunc(w.opsCtx, cancel)
	return ctx, func() { stop(); cancel(); w.wg.Done() }
}

// shutdown records the deadline every later op must respect.
func (w *workGroup) shutdown(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.shutdownCtx = ctx
}

// shutdownContext is nil until Shutdown has been called.
func (w *workGroup) shutdownContext() context.Context {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.shutdownCtx
}

// seal refuses new work; wait then returns once the registered work has finished.
func (w *workGroup) seal() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sealed = true
}

func (w *workGroup) wait() { w.wg.Wait() }

func minTime(a, b time.Time) time.Time { return lo.Ternary(a.Before(b), a, b) }
