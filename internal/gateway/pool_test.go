package gateway

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// queuedPoolJobs is more than one because a select whose queue and whose g.ctx.Done are both ready
// picks between them at random: a single queued job would only expose a dropped one half the time.
const queuedPoolJobs = 16

// A queued item can hold gateway bookkeeping that only running it gives back: a presence read
// carries the work.op registration its worker releases (presence.go dispatchOnline). Dropping the
// item when g.ctx is cancelled would leave that registration outstanding, work.wait would never
// return, and Shutdown would sit out its entire deadline and then report it.
func TestStartPoolDrainsQueuedWorkAtShutdown(t *testing.T) {
	g := newGateway(t, testConfig())
	t.Cleanup(g.cancel)
	t.Cleanup(g.cancelRun)
	jobs := make(chan context.CancelFunc, queuedPoolJobs)
	var ran atomic.Int64
	// One worker, so every job after the first is still queued when shutdown cancels g.ctx.
	startPool(g, 1, func(finish context.CancelFunc) {
		if ran.Add(1) == 1 {
			<-g.ctx.Done()
		}
		finish()
	}, jobs)
	for range cap(jobs) {
		_, finish := g.work.op(context.Background())
		jobs <- finish
	}
	// Generous enough that reaching it means Shutdown waited for work that never ran, not that a
	// loaded machine was slow.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	err := g.Shutdown(ctx)
	elapsed := time.Since(start)
	if err != nil || elapsed > 2*time.Second {
		t.Fatalf("shutdown took %v and returned %v after running %d of %d jobs, want a prompt nil",
			elapsed, err, ran.Load(), cap(jobs))
	}
	if ran.Load() != int64(cap(jobs)) {
		t.Fatalf("%d of %d queued jobs ran; the rest were dropped with their work registration held", ran.Load(), cap(jobs))
	}
}
