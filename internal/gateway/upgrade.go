package gateway

import (
	"sync/atomic"
	"time"
)

// A reservation belongs to HTTP until the upgrade callback claims it. Request completion,
// shutdown, expiry and a synchronous upgrade error may all attempt to return it, but only one wins.
type upgradeReservation struct {
	users   *UserMap
	slot    Slot
	settled atomic.Bool
	done    chan struct{}
}

func (r *upgradeReservation) claim() bool {
	if !r.settled.CompareAndSwap(false, true) {
		return false
	}
	close(r.done)
	return true
}

func (r *upgradeReservation) release() {
	if r.claim() {
		r.users.Release(r.slot)
	}
}

// settledEarly reports a reservation already returned before the upgrade started, which is the drain
// window: UserMap.Reserve only fails after users.Close(), while Shutdown cancels runCtx one step
// before that and seals the work group only after its drain poll, so a request preempted anywhere in
// between still holds a slot watchUpgrade hands straight back. Both of those triggers settle before
// watchUpgrade returns, so Handle sees them; it answers 503 instead of upgrading, because past HTTP
// 101 the callback can only drop the socket without a close frame and the client reads 1006 rather
// than the documented draining error (design §7.3). The writeWait expiry settles asynchronously and
// keeps its documented behaviour there: the late callback closes the connection and the client
// handshakes again.
func (r *upgradeReservation) settledEarly() bool { return r.settled.Load() }

// Hertz normally resets RequestContext after response failure or after the hijack handler returns.
// Keep only Finished's channel because the context is pooled. A host can suppress Reset, so the
// write budget also bounds unclaimed reservations without interrupting the host's HTTP writer.
func (g *Gateway) watchUpgrade(finished <-chan struct{}, slot Slot) *upgradeReservation {
	r := &upgradeReservation{users: g.users, slot: slot, done: make(chan struct{})}
	// Test runCtx before begin(), so an already-draining node settles the reservation here rather
	// than from the watcher goroutine, where Handle would race it and upgrade anyway. Reading it
	// first also keeps the group balanced: no begin() without a matching done().
	if g.runCtx.Err() != nil || !g.work.begin() {
		r.release()
		return r
	}
	timer := time.NewTimer(writeWait)
	go func() {
		defer g.work.done()
		defer timer.Stop()
		select {
		case <-r.done:
			return
		case <-finished:
		case <-g.runCtx.Done():
		case <-timer.C:
		}
		r.release()
	}()
	return r
}
