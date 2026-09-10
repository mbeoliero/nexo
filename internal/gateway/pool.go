package gateway

// startPool launches perQueue goroutines over each queue, every one of them registered with the
// gateway work group so Shutdown waits for it, and all of them exiting with g.ctx at the end of
// Shutdown. Delivery shards its queues by conversation id and runs one worker each, because
// ordering inside a conversation is the point (design §6.1); presence runs several workers over one
// queue, because a connection's snapshots are already ordered by its single in-flight read (§7.4).
// The queues are never closed, so a late event is dropped rather than panicking. What is already
// queued when g.ctx is cancelled still runs: a queued item can hold gateway bookkeeping that only
// its worker gives back — a presence read carries the work.op registration finishOnlineRead releases
// (§7.4) — so abandoning it would leave work.wait waiting for a job nobody will ever run.
func startPool[T any](g *Gateway, perQueue int, run func(T), queues ...chan T) {
	for _, ch := range queues {
		for range perQueue {
			if !g.work.begin() {
				return
			}
			go func() {
				defer g.work.done()
				for {
					select {
					case v := <-ch:
						run(v)
					case <-g.ctx.Done():
						// The drain cannot outlive the shutdown deadline: nothing refills the queue
						// past this point, and Shutdown cancels the op contexts before g.ctx, so
						// each remaining item finds its work already cancelled and returns at once.
						for {
							select {
							case v := <-ch:
								run(v)
							default:
								return
							}
						}
					}
				}
			}()
		}
	}
}
